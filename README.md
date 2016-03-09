# Kafka Connect Cassandra
Kafka Connect Cassandra Connector. This project includes source/sink connectors.

## Building from Source
The project requires SBT to build from source. Execute the following command in the project directory,

```
$ sbt assembly
```

This will create an assembly jar which can be added to `lib` directory and used with Kafka.


## Current Status

CassandraSink
It stores Kafka SinkRecord in Cassandra tables. 
The properties required are

| name   | expected value                 |
|--------|--------------------------------|
| host   | defaults to `localhost`        |
| port   | defaults to 9042               |
| topics | comma-separated list of topics |
| `<topicName>_table` | corresponding Cassandra table in which it should be inserted. This should be specified for every topic that should be ingested by the sink. The format of the value should be `<keyspaceName>.<tableName>`. For example, if topic `user` should be inserted into table `users` in `hr` keyspace, then the value of `user_table` should be `hr.users` |

Note: The library does not create the Cassandra tables - users are expected to create those before starting the sink


CassandraSource
It polls Cassandra with  specified query. 
The properties required are

| name   | expected value                 |
|--------|--------------------------------|
| host   | defaults to `localhost`        |
| port   | defaults to 9042               |
| query | the `select` statement that should be executed |