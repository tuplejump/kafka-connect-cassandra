#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ ! -f "$KAFKA_HOME" ]
then
    if [ ! -f kafka.tgz ]
    then
        echo "KAFKA_HOME is not configured. Downloading Kafka."
        wget "http://archive.apache.org/dist/kafka/0.9.0.1/kafka_2.11-0.9.0.1.tgz" -O kafka.tgz
    fi
    tar -xzf kafka.tgz
    KAFKA_HOME=$DIR/$(tar tfz kafka.tgz --exclude '*/*')
fi

if [ ! -f "$CASSANDRA_HOME" ]
then
    if [ ! -f cassandra.tar.gz ]
    then
        echo "CASSANDRA_HOME is not configured. Downloading Cassandra."
        wget "http://archive.apache.org/dist/cassandra/3.1.1/apache-cassandra-3.1.1-bin.tar.gz" -O cassandra.tar.gz
    fi
    mkdir -p cassandra
    tar -xzf cassandra.tar.gz -C cassandra --strip-components=1
    CASSANDRA_HOME=cassandra
fi

if [ ! -f "kafka-connect-cassandra-assembly-0.0.7.jar" ]
then
    #Todo
    echo "didnt find jar. Downloading"
fi

cp kafka-connect-cassandra-assembly-0.0.7.jar ${KAFKA_HOME}libs/

##update path of data file
sed -i "s;'data.csv';'${DIR}/data.csv';" "${DIR}/setup.cql"

##start cassandra
cd ${CASSANDRA_HOME}

bin/cassandra -p ${DIR}/'cassandra.pid' > cassandraLog

echo "waiting for cassandra to start"
sleep 10
echo "done waiting"

##setup schema
bin/cqlsh -f "${DIR}/setup.cql"

cd ${KAFKA_HOME}

## start zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties > ${DIR}/zkLog &
ZK_PID=$!

sleep 5

## start Kafka server
bin/kafka-server-start.sh config/server.properties > ${DIR}/serverLog &
KAFKA_PID=$!

sleep 10

# create topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic demo

bin/connect-standalone.sh config/connect-standalone.properties ${DIR}/config/bulk-source.properties ${DIR}/config/sink.properties > ${DIR}/connectLog &
CONNECT_PID=$!

sleep 10

cd ${DIR}
echo ${CONNECT_PID} ${KAFKA_PID} ${ZK_PID} $(cat cassandra.pid) > demo.pid