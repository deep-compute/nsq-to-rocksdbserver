# nsq-to-rocksdbserver
Dumps messages from nsq into rocksdb, exposed via rocksdbserver.

## Installation

> Recommended to be in a virtual environment
> Please follow build instructions for rocksdbserver at
> https://github.com/deep-compute/rocksdbserver

Until this is available on pypi, clone the repository and run

```
pip install .
```

## Usage

```
nsq-to-rocksdbserver run --port PORT /path/to/rocksdb/data/dir NSQ_TOPIC
```
