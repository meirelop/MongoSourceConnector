package com.meirkhan.kafka;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        //MongoCursor<Document> cursor;
        List<DBObject> criteria = new ArrayList<>();
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject("$gt", lastIncrement)));
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject("$type", "number")));
        BasicDBObject gtQuery = new BasicDBObject("$and", criteria);
        cursor = collection.find(gtQuery).iterator();
        return cursor;
    }

    public MongoCursor<Document> getTimestampCursor(Instant lastTimestamp) {
//        String format = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        log.info("OOOOOSSSSHIBKAAA");
        log.info(lastTimestamp.toString());
        log.info(lastTimestamp.toString());
        log.info(incrementColumn);

        List<DBObject> criteria = new ArrayList<>();
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject("$gt", lastTimestamp)));
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject("$type", 9)));
        BasicDBObject gtQuery = new BasicDBObject("$and", criteria);
//        log.info(gtQuery.toString());
        cursor = collection.find(gtQuery).iterator();
        return cursor;
    }

    public MongoCursor getIncrementTimestampCursor() {
        return cursor;
    }

}
