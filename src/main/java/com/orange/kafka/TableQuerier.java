package com.orange.kafka;

import com.mongodb.client.MongoCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.LoggerFactory;

abstract class TableQuerier {
    static final Logger log = LoggerFactory.getLogger(MongodbSourceConnectorConfig.class);
    protected MongoCursor<Document> cursor;
    private String mongoUri;
    private String mongoHost;
    private int mongoPort;
    private String dbName;
    private String collectionName;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection collection;
    private String topic;

    public TableQuerier(
                        String topic,
                        String mongoUri,
                        String mongoHost,
                        int mongoPort,
                        String dbName,
                        String collectionName
                        )
    {   this.topic = topic;
        this.mongoUri = mongoUri;
        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.mongoClient = new MongoClient(mongoHost, mongoPort);
        this.database = mongoClient.getDatabase(dbName);
        this.collection = database.getCollection(collectionName);
    }

    public abstract boolean hasNext();

    public abstract void executeCursor();

    public abstract void closeCursor();

    public abstract SourceRecord extractRecord();
}
