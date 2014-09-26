kafka4net
=========

NTent implementation of Kafka-0.8 client. <br/>
<b>Work in progress, not ready yet!<b>

Features:
* Event-driven architecture, all asynchronous
* Automatic following changes of Leader Partition in case of broker failure
* Connection sharing: one connection per kafka broker is used
* Integration tests are part of the codebase. Use Vagrant to provision 1 zookeeper and 3 kafka virtual servers
* Use RxExtensions library to expose API and for internal implementation

Not impleented:
* Offset Fetch/Commit API
* No compression
* No configurable serializers (everything is byte array)
* No configurable partitioners (fletcher32 checksum is hardcoded)
