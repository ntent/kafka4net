kafka4net
=========

#NTent implementation of Kafka-0.8 client

##Features:
* Event-driven architecture, all asynchronous
* Automatic following changes of Leader Partition in case of broker failure
* Connection sharing: one connection per kafka broker is used
* Flow control: slow consumer will suspend fetching and prevent memory exhausting
* Integration tests are part of the codebase. Use Vagrant to provision 1 zookeeper and 3 kafka virtual servers
* Use RxExtensions library to expose API and for internal implementation

##Not implemented:
* Offset Fetch/Commit API
* No compression
* No configurable serializers (everything is byte array)
* No configurable partitioner (fletcher32 checksum is hardcoded)

## Documentation
[Design](https://github.com/ntent-ad/kafka4net/wiki/Design)
[Troubleshooting](https://github.com/ntent-ad/kafka4net/wiki/Troubleshooting)
