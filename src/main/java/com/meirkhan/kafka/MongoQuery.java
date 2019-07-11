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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.meirkhan.kafka.MySchemas.INCREMENTING_OFFSET;


public class MongoQuery {
    private static Logger log = LoggerFactory.getLogger(MySourceConnector.class);

//    public static MongoCursor<Document> getCursor(Double lastIncrement) {
//
//        MongoClient mongoClient = new MongoClient(config.getMongoHost(), config.getMongoPort());
//        MongoDatabase database = mongoClient.getDatabase(config.getMongoDbName());
//        MongoCollection collection = database.getCollection(config.getMongoCollectionName());
//        MongoCursor<Document> cursor;
//        //String incrementColumn = config.getIncrementColumn();
//        String mode = config.getModeName();
//
//        // TODO: implement 'TABLE' and 'QUERY' cases
//        if (mode.equals(INCREMENTING_OFFSET)) {
//            List<DBObject> criteria = new ArrayList<>();
//            criteria.add(new BasicDBObject(config.getIncrementColumn(), new BasicDBObject("$gt", lastIncrement)));
//            criteria.add(new BasicDBObject(config.getIncrementColumn(), new BasicDBObject("$type", "number")));
//            BasicDBObject gtQuery = new BasicDBObject("$and", criteria);
//            cursor = collection.find(gtQuery).iterator();
//        } else {
//            cursor = collection.find().iterator();
//        }
//        return cursor;
//    }
}
