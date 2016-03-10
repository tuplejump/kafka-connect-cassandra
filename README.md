# Kafka Connect Cassandra
Kafka Connect Cassandra Connector. This project includes source/sink connectors.

## Building from Source
The project requires SBT to build from source. Execute the following command in the project directory,

    sbt assembly

This will build against Scala 2.11.7 by default. You can override this with:

    sbt -Dscala.version=2.10.6 assembly
    
This will create an assembly jar which can be added to `lib` directory and used with Kafka.

## Current Status

### CassandraSink
It stores Kafka SinkRecord in Cassandra tables. 
Configurable properties are:

| name   | expected value                 |
|--------|--------------------------------|
| host   | defaults to `localhost`        |
| port   | defaults to 9042               |
| topics | comma-separated list of topics(Kafka Connector property) |
| `<topicName>_table` | corresponding Cassandra table in which it should be inserted. This should be specified for every topic that should be ingested by the sink. The format of the value should be `<keyspaceName>.<tableName>`. For example, if topic `user` should be inserted into table `users` in `hr` keyspace, then the value of `user_table` should be `hr.users` |

Note: The library does not create the Cassandra tables - users are expected to create those before starting the sink

### CassandraSource
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
             
Configurable properties are:

| name   | expected value                 |
|--------|--------------------------------|
| host   | defaults to `localhost`        |
| port   | defaults to 9042               |
| query | the `select` statement that should be executed |
| pollInterval | interval(millis) in which timestamp query should be executed. Defaults to 5000 |
| topic | name for the topic(Kafka Connector property) |