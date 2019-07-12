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
import java.util.ArrayList;
import java.util.List;
import static com.meirkhan.kafka.MySchemas.*;


public class IncrementQuerier extends TableQuerier{
    private String incrementColumn;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection collection;

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

    public MongoCursor<Document> getBatchCursor() {
        return cursor;
    }

    public MongoCursor<Document> getIncrementCursor (Double lastIncrement) {
        MongoCursor<Document> cursor;
        List<DBObject> criteria = new ArrayList<>();
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject("$gt", lastIncrement)));
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject("$type", "number")));
        BasicDBObject gtQuery = new BasicDBObject("$and", criteria);
        cursor = collection.find(gtQuery).iterator();
        return cursor;
    }

    public MongoCursor<Document> getTimestampCursor() {
        return cursor;
    }

    public MongoCursor<Document> getIncrementTimestampCursor() {
        return cursor;
    }

}
