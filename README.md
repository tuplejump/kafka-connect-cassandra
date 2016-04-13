# Kafka Connect Cassandra  [![GitHub license](https://img.shields.io/badge/license-Apache%20V2-green.svg)](https://github.com/tuplejump/kafka-connect-cassandra/blob/master/LICENSE) [![Build Status](https://travis-ci.org/tuplejump/kafka-connect-cassandra.svg?branch=master)](https://travis-ci.org/tuplejump/kafka-connect-cassandra?branch=master) 
Kafka Connect Cassandra Connector. This project includes source & sink connectors.

## Release Status
Experimental phase.

## Table of Contents

* [Usage](#usage)
* [Running Examples](#running-example)
* [CassandraSource](#cassandrasource)
    * [Supported CQL Types](#cql-types-supported)
* [CassandraSink](#cassandrasink)
* [Configuration](#configuration)
    * [Connect Properties](#connect-properties-for-both-source-and-sink)
    * [Source Connect Properties](#source-connect-properties)
    * [Sink Connect Properties](#sink-connect-properties)
* [Building From Source](#building-from-source)

## Usage
The project can be used in two ways:

1. as a library in another project,

   "kafka-connect-cassandra" is published on maven central by Tuplejump. It can be defined as a dependency in the build file.
   For example, with SBT,
   
   ```
   libraryDependencies += com.tuplejump" %% "kafka-connect-cassandra" % "0.0.7"
   ```

2. using jar with Kafka Connect

   Download the [jar](http://downloads.tuplejump.com/kafka-connect-cassandra-assembly-0.0.7.jar) and copy it to 'KAFKA_HOME/libs'

## Running Examples
The example uses CassandraSource and loads data from `demo.event_store`. This data is then saved in another table `demo.event_store_sink` using CassandraSink.
To run the example, execute `examples/start.sh` in the project directory.

This script starts Cassandra, Zookeper, Kafka Server and then initiates a Kafka Connect command using the Source and Sink Configuration specified in `examples/config`.
Once the script execution is completed, the data from `demo.event_store` will be available in `demo.event_store_sink`. You can insert a few rows in `demo.event_store` to see this.

To stop the processes started for running the example, execute `examples/stop.sh`

Note: On running the example repeatedly, an error is thrown that topic `demo` already exists but it won't block the execution

## CassandraSource
It polls Cassandra with specified query. Using this, data can be fetched from Cassandra in two modes:

 1. bulk
 2. timestamp based

The modes change automatically based on the query. For example, 

```sql
SELECT * FROM userlog ; //bulk 
                                         
SELECT * FROM userlog WHERE ts > previousTime() ; //timestamp based

SELECT * FROM userlog WHERE ts = currentTime() ; //timestamp based

SELECT * FROM userlog WHERE ts >= previousTime() AND  ts <= currentTime() ; //timestamp based
```

Here, `previousTime()` and `currentTime()` are replaced prior to fetching the data.

### CQL Types Supported

| CQL Type | Schema Type |
|----------|------------|
| ASCII | STRING | 
| VARCHAR | STRING | 
| TEXT | STRING | 
| BIGINT | INT64 | 
| COUNTER | INT64 |
| BOOLEAN | BOOLEAN |
| DECIMAL | FLOAT64 |
| DOUBLE | FLOAT64 |
| FLOAT | FLOAT32 |
| TIMESTAMP | TIMESTAMP |
| VARINT | INT64 |

All the others (BLOB, INET, UUID, TIMEUUID, LIST, SET, MAP, CUSTOM, UDT, TUPLE, SMALLINT, TINYINT, DATE,TIME) are currently NOT supported.

## CassandraSink
It stores Kafka SinkRecord in Cassandra tables. Currently, we only support STRUCT type in the SinkRecord. 
The STRUCT can have multiple fields with primitive fieldtypes. 
By default, we assume one-to-one mapping between the column names in the Cassandra sink table and the field names.

Say, the SinkRecords has the following STRUCT value
```
{
    'id': 1,
    'username': 'user1',
    'text': 'This is my first tweet'
}
```

Then the Cassandra table should have the columns - id, username, text  

We also support specifying the field name mapping to column names, using the property `cassandra.sink.field.mapping`
Say, the SinkRecords has the following STRUCT value
```
{
    'id': 1,
    'user': {
        'id': 123,
        'name': 'Foo',
        'email': 'foo@bar.com'
    },
    'text': 'This is my first tweet'
}
``` 
and the `cassandra.sink.field.mapping` has the value `{'id': 'id', 'user': {'id': 'uid', 'name': 'username'}, 'text': 'tweet_text'}`
Then the Cassandra table should have the columns - id, uid, username, tweet_text. 
Note that since we did not specify any mapping for 'user.email', it is ignored and not inserted in the Cassandra Sink table.

Note: The library does not create the Cassandra tables - users are expected to create those before starting the sink

## Configuration

Refer `examples/config` for sample configuration files


### Connect Properties (for both Source and Sink)
| name              | description                | default value  |
|--------           |----------------------------|-----------------------|
| name              |  Unique name for the connector. Attempting to register again with the same name will fail.   |                              |
| connector.class   | The Java class for the connector,  `com.tuplejump.kafka.connect.cassandra.CassandraSource` or `com.tuplejump.kafka.connect.cassandra.CassandraSink` |  |
| tasks.max         |  The maximum number of tasks that should be created for this connector. The connector may create fewer tasks if it cannot achieve this level of parallelism. | |
| | **Cassandra Properties (for both Source and Sink)** | |
| cassandra.connection.host | The host name or IP address to which the Cassandra native transport is bound. | localhost |
| cassandra.connection.port | Port for native client protocol connections. | 9042 |
| cassandra.connection.auth.username | Cassandra username | '' |
| cassandra.connection.auth.password | Cassandra password | '' |
| cassandra.connection.timeout.ms | Connection timeout duration (in ms) | 8000 |
| cassandra.connection.read.timeout | Read timeout duration (in ms) | 120000 |
| cassandra.connection.reconnect.delay.min | Minimum period of time (in ms) to wait before reconnecting to a dead node. | 1000 |
| cassandra.connection.reconnect.delay.max | Maximum period of time (in ms) to wait before reconnecting to a dead node. | 60000 |
| cassandra.connection.consistency | Consistency level. Values allowed can be seen [here](http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/ConsistencyLevel.html) | QUORUM |
| cassandra.task.parallelism | Can be one of the values: one-to-one (One topic-to-keyspace.table per task) or one-to-many (N-topic-to-keyspace.table per task) | one-to-one |
### Source Connect Properties 
| name              | description                | default value  |
|--------           |----------------------------|-----------------------|
| cassandra.source.route.\<topic_name\> | The Select Query to get the data. (Refer CassandraSource documentation for more details) | |
| cassandra.source.poll.interval | Frequency in ms to poll for new data in each table. | 60000 |
| cassandra.source.fetch.size | Number of CQL rows to fetch in a single round-trip to Cassandra. | 1000 |
### Sink Connect Properties 
| name              | description                | default value  |
|--------           |----------------------------|-----------------------|
| cassandra.sink.route.\<topic_name\> | The table to write the SinkRecords to, \<keyspace\>.\<tableName\> | |
| cassandra.sink.consistency | The consistency level for writes to Cassandra. | LOCAL_QUORUM |
| cassandra.sink.field.mapping | The JSON String mapping field names to column names. | |


## Building from Source
The project requires SBT to build from source. Execute the following command in the project directory,

```
sbt package  
sbt assembly // to generate the jar with all the dependencies
```
This will build against Scala 2.11.7 by default. You can override this with:

```
sbt -Dscala.version=2.10.6 assembly
```

Or to build against multiple Scala versions,
``` 
sbt +package
```