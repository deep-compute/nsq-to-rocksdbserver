from setuptools import setup, find_packages

version = '0.0.2'
setup(
    name="nsq-to-rocksdbserver",
    version=version,
    description="dumps nsq messages into rocksdb",
    long_description=(
        "listens on the nsq for messages and writes them to rocksdb "
        "In addition to all rocksdbserver api functions, allows users "
        "to tail live messages."
    ),
    keywords='nsq rocksdb funcserver rocksdbserver',
    author='Deep Compute, LLC',
    author_email="contact@deepcompute.com",
    url="https://github.com/deep-compute/nsq-to-rocksdbserver",
    download_url="https://github.com/deep-compute/nsq-to-rocksdbserver/tarball/%s" % version,
    license='MIT License',
    install_requires=[
        "rocksdbserver",
        "pynsq",
    ],
    package_dir={'nsq_to_rocksdbserver': 'nsq_to_rocksdbserver'},
    packages=find_packages('.'),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
    ],

    entry_points={
        "console_scripts": [
            "nsq-to-rocksdbserver = nsq_to_rocksdbserver.nsq_to_rocksdbserver:main",
        ],
    }
)
