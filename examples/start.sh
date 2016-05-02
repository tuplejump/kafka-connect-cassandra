#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

waitForStart(){
    until grep -q "$2" $3
    do
      echo "waiting for "$1" to start"
      sleep 5
    done
}

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
    echo "didnt find jar. Downloading"
    wget "http://downloads.tuplejump.com/kafka-connect-cassandra-assembly-0.0.7.jar"
fi

cp kafka-connect-cassandra-assembly-0.0.7.jar ${KAFKA_HOME}libs/

##update path of data file
sed -i "s;'data.csv';'${DIR}/data.csv';" "${DIR}/setup.cql"

##start cassandra
cd ${CASSANDRA_HOME}

bin/cassandra -p ${DIR}/'demo.pid' > ${DIR}/cassandraLog

waitForStart "cassandra" "state jump to NORMAL" "${DIR}/cassandraLog"

##setup schema
bin/cqlsh -f "${DIR}/setup.cql"

cd ${KAFKA_HOME}

## start zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties > ${DIR}/zkLog &
ZK_PID=$!

waitForStart "zookeeper" "binding to port" "${DIR}/zkLog"
echo -n " "${ZK_PID} >> ${DIR}/demo.pid

## start Kafka server
bin/kafka-server-start.sh config/server.properties > ${DIR}/serverLog &
KAFKA_PID=$!

waitForStart "kafka server" "Awaiting socket connections" "${DIR}/serverLog"
echo -n " "${KAFKA_PID} >> ${DIR}/demo.pid

# create topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic demo

bin/connect-standalone.sh config/connect-standalone.properties ${DIR}/config/bulk-source.properties ${DIR}/config/sink.properties > ${DIR}/connectLog &
CONNECT_PID=$!

echo -n " "${CONNECT_PID} >> ${DIR}/demo.pid