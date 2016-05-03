CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE demo;

CREATE TABLE IF NOT EXISTS demo.event_store(
    app_id text,
    event_type text,
    subscription_type text,
    event_ts timestamp,
    PRIMARY KEY((app_id, event_type), event_ts)
    );

CREATE TABLE IF NOT EXISTS demo.event_store_sink(
    app_id text,
    event_type text,
    subscription_type text,
    event_ts timestamp,
    PRIMARY KEY((app_id, event_type), event_ts)
    );

COPY event_store(app_id,subscription_type,event_type,event_ts) FROM 'data.csv' ;


