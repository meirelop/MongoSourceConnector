package com.meirkhan.kafka;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.meirkhan.kafka.MySchemas.*;

import java.text.ParseException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;

import static com.meirkhan.kafka.MySchemas.*;



public class IncrementQuerier extends TableQuerier{
    static final Logger log = LoggerFactory.getLogger(MySourceConnectorConfig.class);
    private String incrementColumn;
    private String timestampColumn;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection collection;
    MongoCursor cursor;

    public IncrementQuerier(String mongoHost,
                            int mongoPort,
                            String dbName,
                            String collectionName,
                            String incrementColumn) {

        super(mongoHost,mongoPort,dbName,collectionName);
        this.mongoClient = new MongoClient(mongoHost, mongoPort);
        this.database = mongoClient.getDatabase(dbName);
        this.collection = database.getCollection(collectionName);
        this.incrementColumn = incrementColumn;
    }

    public IncrementQuerier(String mongoHost,
                            int mongoPort,
                            String dbName,
                            String collectionName,
                            String incrementColumn,
                            String timestampColumn) {

        super(mongoHost,mongoPort,dbName,collectionName);
        this.mongoClient = new MongoClient(mongoHost, mongoPort);
        this.database = mongoClient.getDatabase(dbName);
        this.collection = database.getCollection(collectionName);
        this.incrementColumn = incrementColumn;
        this.timestampColumn = timestampColumn;
    }

    public IncrementQuerier(String mongoHost,
                            int mongoPort,
                            String dbName,
                            String collectionName) {

        super(mongoHost,mongoPort,dbName,collectionName);
        this.mongoClient = new MongoClient(mongoHost, mongoPort);
        this.database = mongoClient.getDatabase(dbName);
        this.collection = database.getCollection(collectionName);
    }

    public MongoCursor<Document> getBatchCursor() {
        cursor = collection.find().iterator();
        return cursor;
    }

    public MongoCursor<Document> getIncrementCursor (Double lastIncrement) {
        List<DBObject> criteria = new ArrayList<>();
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject(MONGO_CMD_GREATER, lastIncrement)));
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject(MONGO_CMD_TYPE, MONGO_NUMBER_TYPE)));
        BasicDBObject gtQuery = new BasicDBObject(MONGO_CMD_AND, criteria);
        cursor = collection.find(gtQuery).iterator();
        return cursor;
    }

    public MongoCursor<Document> getTimestampCursor(Instant lastTimestamp) {
        List<DBObject> criteria = new ArrayList<>();
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject(MONGO_CMD_GREATER, lastTimestamp)));
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject(MONGO_CMD_LESS, Instant.now())));
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject(MONGO_CMD_TYPE, MONGO_DATE_TYPE)));
        BasicDBObject gtQuery = new BasicDBObject(MONGO_CMD_AND, criteria);
        cursor = collection.find(gtQuery).iterator();
        return cursor;
    }

    public MongoCursor getIncrementTimestampCursor() {
        return cursor;
    }

    @Override
    public SourceRecord extractRecord() {
        return null;
    }
}
