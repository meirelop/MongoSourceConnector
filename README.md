# Kafka Mongodb SourceConnector

The Mongodb source connector allows you to import data from Mongodb with a mongodb driver driver into Kafka topics.
Data is loaded by periodically executing a mongo query and creating an output record for each row in the result set. When copying data from a collection, the connector can load all data periodically or only new or modified rows by specifying which columns should be used to detect new or modified data.

## Using the MongoDB connector with Kafka Connect

The MongoDB connector is designed to work with [Kafka Connect](http://kafka.apache.org/documentation.html#connect) and to be deployed to a Kafka Connect runtime service. The deployed connector will monitor one or more databases and write all change events to Kafka topics, which can be independently consumed by one or more clients. Kafka Connect can be distributed to provide fault tolerance to ensure the connectors are running and continually keeping up with changes in the database.

Kafka Connect can also be run standalone as a single process, although doing so is not tolerant of failures.


## Embedding the MongoDB source connector

The MongoDB connector can also be used as a library without Kafka or Kafka Connect, enabling applications and services to directly connect to a MongoDB database and obtain the ordered change events. This approach requires the application to record the progress of the connector so that upon restart the connect can continue where it left off. Therefore, this may be a useful approach for less critical use cases.

# Configuration Options

## Database
##### mongo.uri 
            * Mongodb connection string
            * Type: string
            * Importance: high
            
##### mongo.query 
            * Mongodb query from collection
            * Type: string
            * Importance: high            
            
##### mongo.db 
            * Mongodb database name
            * Type: string
            * Importance: high    


## Connector    
##### topic.prefix 
            * Name of the topic to publish to
            * Type: string
            * Importance: high                        
            
##### batch.size 
            *
            * Type: int
            * Default: 100
            * Importance: low
             
##### poll.interval.ms 
            * Frequency in ms to poll for new data in each table..


## Mode
##### mode 
            * The mode for updating a table each time it is polled. Options include:  
                * bulk - perform a bulk load of the entire table each time it is polled
                * incrementing - use a strictly incrementing column on each table to detect only new rows. Note that this will not detect modifications or deletions of existing rows.
                * timestamp - use a timestamp (or timestamp-like) column to detect new and modified rows. This assumes the column is updated with each write, and that values are monotonically incrementing, but not necessarily unique.
                * timestamp+incrementing - use two columns, a timestamp column that detects new and modified rows and a strictly incrementing column which provides a globally unique ID for updates so each row can be assigned a unique stream offset.
            * Type: string
            * Default: ""
            * Valid Values: [, bulk, timestamp, incrementing, timestamp+incrementing]
            * Importance: high
            * Dependents: incrementing.column.name, timestamp.column.name
          
##### incrementing.column.name
            * The name of the strictly incrementing column to use to detect new rows. This column may not be nullable.
            * Type: string
            * Default: ""
            * Importance: medium

##### timestamp.column.name
            * The name of the timestamp column to use to detect new or modified rows. This column may not be nullable.
            * Type: string
            * Default: ""
            * Importance: medium



## Config file example
- name=MongoSourceConnectorDemo
- connector.class=com.orange.kafka.MongodbSourceConnector
- topic.prefix=mongotest
- mongo.uri=mongodb://user1:pwd1@mongos0.example.com:27017,mongos1.example.com:27017/admin
- mongo.db=admin
- mongo.query=db.products.find();
- poll.interval.ms=20000
- batch.size=200
- mode=incrementing
- incrementing.column.name=ID


# Quickstart

1. Put jar file from 'target' directory to directory where you store plugins. Ex: /usr/local/share/kafka/plugins
2. Include path in "$CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties"
3. Export CLASSPATH in ~/.bashrc, CLASSPATH=/usr/local/share/kafka/plugins/kafka-connect-project/*
4. Specify config file options
5. Create config file for connector, Ex: $CONFLUENT_HOME/etc/kafka/MongodbSourceConnector.properties
6. Start Connect: $CONFLUENT_HOME/bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka/MongodbSourceConnector.properties
7. Start consuming: bin/kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic test

## Testing

This module contains unit tests.
A *unit test* is a JUnit test class named `*Test.java` or `Test*.java` that never requires or uses external services, though it can use the file system and can run any components within the same JVM process. They should run very quickly, be independent of each other, and clean up after itself.


# Contribute

- Source Code: https://github.com/
- Issue Tracker: https://github.com/issues
