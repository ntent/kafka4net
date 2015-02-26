#!/bin/bash

# Executed as root

. /vagrant/scripts/env.sh

apt-get install -y openjdk-7-jre-headless

if [ ! -d "/opt/$kafka" ]; then
  mkdir -p /vagrant/files
  cd /vagrant/files
  if [ ! -f "/vagrant/files/$kafka_bin" ]; then
    wget $kafka_url
  fi
  tar -C /opt -xzf $kafka_bin
fi
