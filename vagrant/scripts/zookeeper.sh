#!/bin/bash

. /vagrant/scripts/env.sh

mkdir -p /var/zookeeper
mkdir -p /opt/kafka_2.10-${kafka_version}/logs
sed -i -e 's/-Xmx512M/-Xmx1024M/' "/opt/kafka_2.10-${kafka_version}/bin/zookeeper-server-start.sh"
sed -i -e 's/-Xms512M/-Xms1024M/' "/opt/kafka_2.10-${kafka_version}/bin/zookeeper-server-start.sh"

cp /vagrant/config/zookeeper.properties "/opt/kafka_2.10-${kafka_version}/config/"
chmod a-x "/opt/kafka_2.10-${kafka_version}/config/zookeeper.properties"

cp /vagrant/config/zookeeper.conf /etc/init/
chmod a-x /etc/init/zookeeper.conf
sed -i -e "s/kafka_version/${kafka_version}/g" "/etc/init/zookeeper.conf"

service zookeeper start

