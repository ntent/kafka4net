#!/bin/bash

mkdir -p /var/zookeeper
mkdir -p /opt/kafka_2.10-0.8.1.1/logs
sed -i -e 's/-Xmx512M/-Xmx1024M/' /opt/kafka_2.10-0.8.1.1/bin/zookeeper-server-start.sh
sed -i -e 's/-Xms512M/-Xms1024M/' /opt/kafka_2.10-0.8.1.1/bin/zookeeper-server-start.sh

cp /vagrant/config/zookeeper.properties /opt/kafka_2.10-0.8.1.1/config/
chmod a-x /opt/kafka_2.10-0.8.1.1/config/zookeeper.properties

cp /vagrant/config/zookeeper.conf /etc/init/
chmod a-x /etc/init/zookeeper.conf
service zookeeper start

