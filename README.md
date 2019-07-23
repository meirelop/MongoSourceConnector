# MongoSourceConnector
Mongo Source Connector to Kafka

topic.prefix - prefix to topic for consumer. Topic name is: prefix.dbname.collectionname
batch.size - default 100, maximum 500
mongo.db - mongodb DB name
mongo.uri - mongo connection uri
poll.interval.ms - period of polling in milliseconds
mode - similar to JDBC, 4 modes: bulk, incrementing, timestamp, timestamp+incrementing
incrementing.column.name - name of field with incremental ID (Number type)
timestamp.column.name - name of field with with incremental date (date type)


Usage:
1. Put jar file from target directory to directory with plugins. Ex: /usr/local/share/kafka/plugins
2. Include path in "etc/schema-registry/connect-avro-standalone.properties"
3. Export CLASSPATH in ~/.bashrc, CLASSPATH=/usr/local/share/kafka/plugins/kafka-connect-project/*
4. Specify config file options
5. Create config file for connector in $CONFLUENT_HOME/etc/kafka/MongodbSourceConnector.properties
6. start Connect $CONFLUENT_HOME/bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka/MongodbSourceConnector.properties
7. To test consuming: bin/kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic test-test --from-beginning