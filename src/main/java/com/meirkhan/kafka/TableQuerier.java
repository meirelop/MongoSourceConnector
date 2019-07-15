package com.meirkhan.kafka;

import com.mongodb.client.MongoCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.util.Date;
import org.slf4j.Logger;
import org.bson.Document;
import java.time.Instant;
import org.apache.kafka.connect.source.SourceRecord;

abstract class TableQuerier {
    protected MongoCursor<Document> cursor;
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
                        String mongoHost,
                        int mongoPort,
                        String dbName,
                        String collectionName
                        )
    {   this.topic = topic;
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
