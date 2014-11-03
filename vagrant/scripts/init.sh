#!/bin/bash

# Executed as root

ap-get update
apt-get install -y openjdk-7-jre-headless

kafka_url='http://www.eng.lsu.edu/mirrors/apache/kafka/0.8.1.1/kafka_2.10-0.8.1.1.tgz'
kafka='kafka_2.10-0.8.1.1'
kafka_bin="$kafka.tgz"

if [ ! -d "/opt/$kafka" ]; then
  mkdir -p /vagrant/files
  cd /vagrant/files
  if [ ! -f "/vagrant/files/$kafka_bin" ]; then
    wget $kafka_url
  fi
  tar -C /opt -xzf $kafka_bin
fi
