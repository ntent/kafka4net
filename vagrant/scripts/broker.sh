#!/bin/bash

id=$1
. /vagrant/scripts/env.sh

sed -i -e "s/^broker\.id=.*/broker.id=$id/" "/opt/kafka_2.10-${kafka_version}/config/server.properties"
sed -i -e "s/^#*host\.name=.*/host.name=192.168.56.${id}0/" "/opt/kafka_2.10-${kafka_version}/config/server.properties"
sed -i -e "s/^log.segment.bytes=.*/log.segment.bytes=1048576/" "/opt/kafka_2.10-${kafka_version}/config/server.properties"
sed -i -e "s/^#*num\.io\.threads=.*/num.io.threads=2/" "/opt/kafka_2.10-${kafka_version}/config/server.properties"
sed -i -e 's/^num\.partitions=.*/num\.partitions=6\
default.replication.factor=3/' "/opt/kafka_2.10-${kafka_version}/config/server.properties"
sed -i -e "s/^#*zookeeper\.connect=.*/zookeeper.connect=192.168.56.2:2181/" \
    "/opt/kafka_2.10-${kafka_version}/config/server.properties"
sed -i -e 's/-Xmx1G/-Xmx1G/' "/opt/kafka_2.10-${kafka_version}/bin/kafka-server-start.sh"
sed -i -e 's/-Xms1G/-Xms1G/' "/opt/kafka_2.10-${kafka_version}/bin/kafka-server-start.sh"

cp /vagrant/config/kafka.conf /etc/init/
chmod a-x /etc/init/kafka.conf
sed -i -e "s/kafka_version/${kafka_version}/g" "/etc/init/kafka.conf"

service kafka start
