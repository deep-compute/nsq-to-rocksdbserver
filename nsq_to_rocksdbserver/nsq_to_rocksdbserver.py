from rocksdbserver import Table, RocksDBServer, RocksDBAPI

import nsq
import uuid
import json
import Queue
import atexit
import rocksdb
import datetime
import tornado.ioloop
from threading import Thread, Lock

class NSQTable(Table):
    NAME = "nsq_dump"

    # TODO should we make pack and unpack json or msgpack ?

    # TODO revisit these options
    def define_options(self):
        opts = super(NSQTable, self).define_options()
        opts.max_open_files = 300000
        opts.write_buffer_size = 2 * 1024 * 1024
        opts.max_write_buffer_number = 3
        opts.target_file_size_base = 128 * 1024 * 1024
        opts.compression = rocksdb.CompressionType.zlib_compression
        opts.table_factory = rocksdb.BlockBasedTableFactory(
            filter_policy=rocksdb.BloomFilterPolicy(10),
            block_cache=rocksdb.LRUCache(32 * (1024 ** 2)),
            block_size = 8 * 1024 * 1024,
        )

        return opts

class NSQRocksAPI(RocksDBAPI):
    def __init__(self, data_dir, server):
        self._server = server
        super(NSQRocksAPI, self).__init__(data_dir)

    def define_tables(self):
        table = self._server.nsqtable
        return { table.NAME: table }

    # TODO pass filters here
    def tail(self):
        _id, queue = self._server.register_tail()
        try:
            while True:
                try:
                    msg = queue.get(timeout=5)
                except Queue.Empty:
                    continue

                yield msg

        finally:
            self._server.deregister_tail(_id)

class KILL(object): pass

class NSQRocksServer(RocksDBServer):
    NAME = "NSQRocksServer"
    DESC = "A rocksdb store for all messages through nsq"

    def run(self):
        # rocksdb database that will store the nsq messages
        self.nsqtable = NSQTable(self.args.data_dir, self)

        # writes to the nsqtable happen in bulk periodically
        self.msgqueue = Queue.Queue(maxsize=2000)

        # client tails
        # TODO filtering logic / jq support
        self.tails = {}
        self.tail_lock = Lock()

        # TODO change to lookupd if we are going to have many nsqds
        nsqd_tcp_addresses = [ self.args.nsqd_tcp_address ]

        self.nsq_reader = nsq.Reader(
            topic=self.args.nsq_topic,
            channel="nsq_to_rocksdb",
            nsqd_tcp_addresses=nsqd_tcp_addresses,
        )
        self.nsq_reader.set_message_handler(self.on_nsq_msg)
        self.nsq_reader.set_max_in_flight(100)

        self.rocksdb_writer = Thread(target=self.write_msgs_periodically)
        self.rocksdb_writer.daemon = True
        self.rocksdb_writer.start()

        atexit.register(self.close)
        super(NSQRocksServer, self).run()

    def on_nsq_msg(self, msg):
        try:
            data = json.loads(msg.body)
        except (ValueError, TypeError):
            self.log.exception("skipping msg, invalid json", msg=msg.body)
            return True

        msg.enable_async()
        self.msgqueue.put((msg, data))

        with self.tail_lock:
            for q in self.tails.itervalues():
                q.put(data)

    def register_tail(self):
        q = Queue.Queue(maxsize=100)
        _id = uuid.uuid1().hex
        with self.tail_lock:
            self.tails[_id] = q

        return _id, q

    def deregister_tail(self, _id):
        with self.tail_lock:
            del self.tails[_id]

    def write_msgs_periodically(self):
        buf = []
        interval = datetime.timedelta(seconds=3)
        last_write_at = datetime.datetime.utcnow()
        while True:
            now = datetime.datetime.utcnow()
            item = None
            empty = False
            try:
                item = self.msgqueue.get(block=True, timeout=1)
                if item != KILL:
                    msg, data = item
                    buf.append((msg, data))

            except Queue.Empty:
                empty = True

            about_time = (now - last_write_at) > interval
            has_items = len(buf) > 0
            has_too_many_items = len(buf) > 500
            if (has_items and about_time) or has_too_many_items:
                self._write_to_db(buf)
                buf = []
                last_write_at = now

            if not empty:
                self.msgqueue.task_done()

            if item == KILL:
                break

    def _write_to_db(self, msg_data_list):
        messages = []

        bulk_data = []
        for msg, data in msg_data_list:
            messages.append(msg)
            # get a key from data
            key = []
            suffix = uuid.uuid1().hex
            try:
                beat = data['beat']
                timestamp = data['@timestamp']
                timestamp = timestamp.encode('utf8', 'ignore')

                key.append(timestamp)

                hostname = beat['hostname']
                hostname = hostname.encode('utf8', 'ignore')
                key.append(hostname)

            except:
                pass

            key.append(suffix)
            keystr = '-'.join(key).encode('utf8')
            bulk_data.append((keystr, data))


        self.nsqtable.put_many(bulk_data)

        def finish_messages():
            for msg in messages:
                msg.finish()

        tornado.ioloop.IOLoop.instance().add_callback(finish_messages)

    def prepare_api(self):
        return NSQRocksAPI(self.args.data_dir, server=self)

    def define_args(self, parser):
        super(NSQRocksServer, self).define_args(parser)
        parser.add_argument("nsq_topic")
        parser.add_argument("--nsqd-tcp-address", default="localhost:4150",
            help="default: %(default)s",
        )

    def close(self):
        # don't accept any more messages
        self.nsq_reader.set_max_in_flight(0)
        self.log.info("waiting for all messages in queue to be written")
        # read all existing messages from the queue
        self.msgqueue.put(KILL)
        self.msgqueue.join()

        self.nsq_reader.close()
        self.nsqtable.close()

def main():
    NSQRocksServer().start()

if __name__ == '__main__':
    main()
