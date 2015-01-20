kafka_version=`cat /vagrant/scripts/kafka_version.txt`
kafka="kafka_2.10-${kafka_version}"
kafka_bin="$kafka.tgz"
#kafka_url="http://www.eng.lsu.edu/mirrors/apache/kafka/${kafka_version}/${kafka_bin}"
kafka_url="https://people.apache.org/~junrao/kafka-${kafka_version}-candidate1/${kafka_bin}"
