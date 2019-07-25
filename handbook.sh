#!/usr/bin/env bash
export CLASSPATH="$(find target/ -type f -name '*.jar'| grep '\-package' | tr '\n' ':')"
if hash docker 2>/dev/null; then
    # for docker lovers
    docker build . -t meirkhan/kafka-connect-project-1.0-SNAPSHOT
    docker run --net=host --rm -t \
           -v $(pwd)/offsets:/kafka-connect-project/offsets \
           meirkhan/kafka-connect-project-1.0-SNAPSHOT
elif hash connect-standalone 2>/dev/null; then
    # for mac users who used homebrew
    connect-standalone config/worker.properties config/MySourceConnectorExample.properties
elif [[ -z $KAFKA_HOME ]]; then
    # for people who installed kafka vanilla
    $KAFKA_HOME/bin/connect-standalone.sh $KAFKA_HOME/etc/schema-registry/connect-avro-standalone.properties config/MySourceConnectorExample.properties
elif [[ -z $CONFLUENT_HOME ]]; then
    # for people who installed kafka confluent flavour
    $CONFLUENT_HOME/bin/connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties config/MySourceConnectorExample.properties
else
    printf "Couldn't find a suitable way to run kafka connect for you.\n \
Please install Docker, or download the kafka binaries and set the variable KAFKA_HOME."
fi;


Changed dir for kafka connect pluins: For debezium and MonoSink connectors
docker run -it --rm -p 2181:2181 -p 3030:3030 -p 8081:8081 -p 8082:8082 -p 8083:8083 -p 9092:9092 -e ADV_HOST=127.0.0.1 -e RUNTESTS=0 -v ~/projects/kafkaconnectgithub/target/kafka-connect-project-1.0-SNAPSHOT-package/share/java/kafka-connect-project:/connectors landoop/fast-data-dev
docker run -it --rm -p 2182:2182 -p 3030:3030 -p 8084:8084 -p 8085:8085 -p 8086:8086 -p 9094:9094 -e ADV_HOST=127.0.0.1 -e RUNTESTS=0 -v ~/projects/kafkaconnectgithub/target/kafka-connect-project-1.0-SNAPSHOT-package/share/java/kafka-connect-project:/connectors landoop/fast-data-dev

docker issues:
- gave super rights to docker
- Create a file named /etc/systemd/system/docker.service.d/10_docker_proxy.conf with below content
[Service]
Environment=HTTP_PROXY=http://10.0.2.2:3128
Environment=HTTPS_PROXY=http://10.0.2.2:3128



Run Helloworld KafkaSourceConnector
1. Removed parts with HTTP client in GithubSourceConnector
2. mvn clean package
3. run confluent/start
4. sudo bash run.sh with only CONFLUENT_HOME
5. if error "address is already in use", kill process

6. curl -s "http://localhost:8083/connectors" ->>to see connectors list
7. curl -s "http://localhost:8083/connectors/jdbc_source_mysql_10/status"|jq '.' ->> see status of connector
8. curl -s "http://localhost:8083/connectors/jdbc_source_mysql_10/config"|jq '.' ->> config of the connector



-------Mongo-----------
--Mongo Bulk Insert
exported mongo_path at bashrc


-------------------------Mongo queries---------------------------------
https://docs.mongodb.com/manual/reference/connection-string/
https://jeromejaglale.com/doc/programming/mongodb_sharding
https://mongodb.github.io/mongo-java-driver/3.4/driver/tutorials/connect-to-mongodb/#connect-to-a-replica-set

show dbs
use test
db.products.insert( { _id: 10, item: "pen", qty: 25 } )
db.products.insert( { _id: 10, item: "pen", qty: 30 } )
db.products.insert( { _id: 10, item: "pen", qty: 40 } )

db.table.insert( [{ _id: 12, item: "pen", qty: 40, "incr":3 },
                    { _id: 13, item: "pen", qty: 40, "incr":4 }])


db.table4.insert({_id:1127, item: "qwqwgaaae", incr:5, time:ISODate("2018-11-02")});

for (var i = 1; i <= 1000000; i++) { db.test3.insert( { incr : i } ) }

db.table4.find({"time" : { $gte : new ISODate("2017-01-12T20:15:31Z") }});


db.transaction.find({
        "header.final.core.data.creationDate": {
            $gte: ISODate("2019-06-27"),
            $lt: ISODate("2019-07-03")
        }
    })
--------------- Mongo Credentials--------------------
sudo chown -R meirkhan /data/db
mongod --auth
sudo nano /etc/mongodb.conf
uncommented auth=true

-- admin user
use admin
db.createUser(
  {
    user: "useradmin",
    pwd: "thepianohasbeendrinking",
    roles: [ { role: "userAdminAnyDatabase", db: "admin" } ]
  }
)
db.auth("useradmin","thepianohasbeendrinking")

--test user
use test
db.createUser(
  {
    user: "testuser",
    pwd: "pwd1",
    roles: [ { role: "readWrite", db: "test" } ]
  }
)
db.auth("testuser","pwd1")

-- OTHER OPTIONS
mongo localhost:27017/admin -u admin -p SECRETPASSWORD
mongo -u YourUserName -p YourPassword admin
> use admin
switched to db admin
> db.auth('admin','SECRETPASSWORD');


------------------------------------- DEBEZIUM MongoDB Source instructions ----------------------------------------------
1. Download tar.gz debezium plugin
2. Create dir /usr/local/share/kafka/plugins
3. unzip tar.gz
4. Change Kafka Connect worker configs in "etc/schema-registry/connect-avro-standalone.properties" (in this case), added "plugin.path=/usr/local/share/java,usr/local/share/kafka/plugins"
5. Export CLASSPATH in ~/.bashrc, CLASSPATH=/usr/local/share/kafka/plugins/debezium-connector-mongodb/*
6. Download mongodb
7. Create file "connect-mongo-source.properties" on directory /usr/local/confluent-5.2.1/etc/kafka and specify details
8. ----------------------MongoDB configurations
8.1. create db dirs: mkdir -p /srv/mongodb/rs0-0  /srv/mongodb/rs0-1 /srv/mongodb/rs0-2
8.2. give write permissons: sudo chmod -R go+w /srv/mongodb/
8.3. start rs instances: mongod --replSet rs0 --port 27017 --bind_ip localhost --dbpath /srv/mongodb/rs0-0 --smallfiles --oplogSize 128
 					     mongod --replSet rs0 --port 27018 --bind_ip localhost --dbpath /srv/mongodb/rs0-1 --smallfiles --oplogSize 128
 					     mongod --replSet rs0 --port 27019 --bind_ip localhost --dbpath /srv/mongodb/rs0-2 --smallfiles --oplogSize 128
8.4. open replica clients: mongo --port 27017, mongo --port 27018, mongo --port 27019
8.5. In primary client, specify rs configurations
rsconf = {
  _id: "rs0",
  members: [
    {
     _id: 0,
     host: "localhost:27017"
    },
    {
     _id: 1,
     host: "localhost:27018"
    },
    {
     _id: 2,
     host: "localhost:27019"
    }
   ]
}
8.6. rs.initiate(rsconf)
8.7. create db and insert: create db - "use test", insert - "db.users.insert({username:"mkyong",password:"123456"})"
9. start Connect: bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka/connect-mongo-source.properties
10. start consuming changes: bin/kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic test.test.users --from-beginning


Links:

1. Why KC exists
http://why-not-learn-something.blogspot.com/2018/02/kafka-connect-why-exists-and-how-works.html

2. Why not to use KC
https://hackernoon.com/why-we-replaced-our-kafka-connector-with-a-kafka-consumer-972e56bebb23

3. Kafka ecosystem overview
https://medium.com/@stephane.maarek/the-kafka-api-battle-producer-vs-consumer-vs-kafka-connect-vs-kafka-streams-vs-ksql-ef584274c1e

4. Kafka Connect JDBC Source
https://www.confluent.io/blog/kafka-connect-deep-dive-jdbc-source-connector

5. Kafka offsets and partitions
https://medium.com/@yusufs/getting-started-with-kafka-in-golang-14ccab5fa26
https://stackoverflow.com/questions/53818612/kafka-will-different-partitions-have-the-same-offset-number
https://grokbase.com/t/kafka/users/143748y6tq/are-offsets-unique-immutable-identifiers-for-a-message-in-a-topic

6. GetMetadata() to return the partition list for a topic (or all topics)
https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#Consumer.GetMetadata

7. DIY Kafka Connector
https://enfuse.io/a-diy-guide-to-kafka-connectors/

8. Custom GitHubSourceConnector
https://github.com/simplesteph/kafka-connect-github-source/blob/master/src/main/java/com/simplesteph/kafka/GitHubSourceConnectorConfig.java

9.Create equivalent to mongo shell like queries
http://pingax.com/trick-convert-mongo-shell-query-equivalent-java-objects/

10. Article on features of JDBC Connector in details, maybe implement similar?
https://www.confluent.io/blog/kafka-connect-deep-dive-jdbc-source-connector

11. Create custom sink Connector
https://hackernoon.com/writing-your-own-sink-connector-for-your-kafka-stack-fa7a7bc201ea

12. Dev Guide from confluent
https://docs.confluent.io/2.0.0/connect/devguide.html

13. Kafka Connect documentation
https://docs.confluent.io/current/connect/javadocs/org/apache/kafka/connect/connector/Connector.html

14. Debezium Mongodb Source connector tutorial
https://medium.com/tech-that-works/cloud-kafka-connector-for-mongodb-source-8b525b779772

15. Catch moving data with modofied Debezium
https://medium.com/@vinee.27.10/catch-the-moving-data-e2b281ff2813

16. Debezium chat
https://gitter.im/debezium/user

17. Kafka contacts
https://kafka.apache.org/contact

18. ---PRESTO
Presto is Connector to MongoDb which allows to query from Mongo as SQL.

19. Confluent blog on JDBC source connector
https://www.confluent.io/blog/kafka-connect-deep-dive-jdbc-source-connector



Error handling:
1. Was not given incrementing column, or was given wrong one
2. Query syntax is not correct
3. in "Timestamp" mode, column has to be type of Timestamp
4. if mode was written wrong syntactically
5. no input parameter
6. .ConnectException: query may not be combined with whole-table copying settings in JDBC


# quickstart Kafka

start zookeeper
bin/zookeeper-server-start etc/kafka/zookeeper.properties

start kafka
./bin/kafka-server-start ./etc/kafka/server.properties

start Schema Registry
bin/schema-registry-start etc/schema-registry/schema-registry.properties

start producer
bin/kafka-avro-console-producer --broker-list localhost:9092 --topic test
bin/kafka-console-producer --broker-list localhost:9092 --topic test

start consumer
bin/kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic transaction_items
bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning


//TODO
1. Type casting for "increment.column" which can handle any number type. Currently it can handle double
2. Type casting for "timestamp.column"
4. do not repeat poll if offset is null and already was produced
5. implement topic.prefix
7. in JDBC, if user does not specify the query, all the tables in DB will be exported to Kafka
8. add authenticating mongodb User
9. It is possible also to control the tables pulled back by the connector, using the table.whitelist (“only include”) or table.blacklist (“include everything but”) configuration.
10. Implement schema for record keys and values
12. Mongodb uri instead of separate port and host
13. Mongo projections (Table and Query)
14. Add timezone parameter
15. Bulk mode every time repeates first batch-size records
16. Check for correctness of collection name, check connection as in JDBC
17. Check for correct pattern of query.
18. Check beforehand that given collection, column exists


--------TESTING-----
1. https://codeutopia.net/blog/2015/04/11/what-are-unit-testing-integration-testing-and-functional-testing/
2. https://stackoverflow.com/questions/4904096/whats-the-difference-between-unit-functional-acceptance-and-integration-test




-------MONGO Querying-----------
1. Mongo aggregation with Java
https://stackoverflow.com/questions/31643109/mongodb-aggregation-with-java-driver


----POSTGRES-----
1. Install postgres
https://www.digitalocean.com/community/tutorials/how-to-install-and-use-postgresql-on-ubuntu-18-04



INSERT INTO DOCUMENT_TEMPLATE(id, name, short_description, author,
                              description, content, last_updated, created)
WITH base(id, n1,n2,n3,n4,n5,n6,n7) AS
(
  SELECT id
        ,MIN(CASE WHEN rn = 1 THEN nr END)
        ,MIN(CASE WHEN rn = 2 THEN nr END)
        ,MIN(CASE WHEN rn = 3 THEN nr END)
        ,MIN(CASE WHEN rn = 4 THEN nr END)
        ,MIN(CASE WHEN rn = 5 THEN nr END)
        ,MIN(CASE WHEN rn = 6 THEN nr END)
        ,MIN(CASE WHEN rn = 7 THEN nr END)
  FROM generate_series(1,100000) id     -- number of rows
  ,LATERAL( SELECT nr, ROW_NUMBER() OVER (ORDER BY id * random())
             FROM generate_series(1,900) nr
          ) sub(nr, rn)
   GROUP BY id
), dict(lorem_ipsum, names) AS
(
   SELECT 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.'
         ,'{"James","John","Jimmy","Jessica","Jeffrey","Jonathan","Justin","Jaclyn","Jodie"}'::text[]
)
SELECT b.id, sub.*
FROM base b
,LATERAL (
     SELECT names[b.n1 % 9+1]
           ,substring(lorem_ipsum::text, b.n2, 20)
           ,names[b.n3 % 9+1]
           ,substring(lorem_ipsum::text, b.n4, 100)
           ,substring(lorem_ipsum::text, b.n5, 200)
           ,NOW() - '1 day'::INTERVAL * (b.n6 % 365)
           ,(NOW() - '1 day'::INTERVAL * (b.n7 % 365)) - '1 year' :: INTERVAL
      FROM dict
) AS sub(name,short_description, author,descriptionm,content, last_updated, created);


- SELECT ID, name, created FROM DOCUMENT_TEMPLATE order by id desc limit 100;
- SELECT count(*) FROM DOCUMENT_TEMPLATE LIMIT 100;
- \conninfo
- ALTER USER meirkhan PASSWORD 'omotoh40';




sudo curl -X PUT http://localhost:8083/connectors/jdbc_source_mysql_02/pause


name=jdbc_source_mysql_03
connector.class=io.confluent.connect.jdbc.JdbcSourceConnector
connection.url=jdbc:postgresql://localhost:5432/meirkhan
connection.user=meirkhan
connection.password=omotoh40
topic.prefix=test-
table.whitelist=users,products,transactions
poll.interval.ms=30000
mode=incrementing
incrementing.column.name=id
query=SELECT ID, name, created FROM DOCUMENT_TEMPLATE





insert into table1(ID, name, created)  select ID, name, created FROM DOCUMENT_TEMPLATE where id > 100000;



