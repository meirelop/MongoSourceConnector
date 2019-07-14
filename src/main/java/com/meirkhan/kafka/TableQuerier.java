package com.meirkhan.kafka;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.util.Date;
import java.text.ParseException;
import org.slf4j.Logger;
import org.bson.Document;
import java.time.Instant;

abstract class TableQuerier {
    protected MongoCursor<Document> cursor;
    private String mongoHost;
    private int mongoPort;
    private String dbName;
    private String collectionName;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection collection;

    public TableQuerier(String mongoHost,
                        int mongoPort,
                        String dbName,
                        String collectionName){
        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.mongoClient = new MongoClient(mongoHost, mongoPort);
        this.database = mongoClient.getDatabase(dbName);
        this.collection = database.getCollection(collectionName);
    }

    public abstract MongoCursor<Document> getBatchCursor();

    public abstract MongoCursor<Document> getIncrementCursor(Double lastIncrement);

    public abstract MongoCursor<Document> getTimestampCursor (Instant lastTimestamp);

    public abstract MongoCursor<Document> getIncrementTimestampCursor();

    public boolean next() {
        return cursor.hasNext();
    }
}
