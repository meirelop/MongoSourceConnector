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
    private String mongoHost;
    private Integer mongoPort;
    private String dbName;
    private String collectionName;
    private String incrementColumn;

    public IncrementQuerier(String mongoHost,
                            Integer mongoPort,
                            String dbName,
                            String collectionName,
                            String incrementColumn) {
        //super(mongoHost, mongoPort, dbName, collectionName, incrementColumn);
        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.incrementColumn = incrementColumn;
    }

    private MongoClient mongoClient = new MongoClient(mongoHost, mongoPort);
    private MongoDatabase database = mongoClient.getDatabase(dbName);
    private MongoCollection collection = database.getCollection(collectionName);

    public MongoCursor<Document> getCursor (Double lastIncrement) {
        MongoCursor<Document> cursor;
        List<DBObject> criteria = new ArrayList<>();
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject("$gt", lastIncrement)));
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject("$type", "number")));
        BasicDBObject gtQuery = new BasicDBObject("$and", criteria);
        cursor = collection.find(gtQuery).iterator();

        return cursor;
    }

}
