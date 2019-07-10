package com.meirkhan.kafka;

import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.FindIterable;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.Document;

import java.time.Instant;
import java.util.Iterator;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCursor;
import org.apache.commons.lang3.StringUtils;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.List;
import java.util.ArrayList;

import org.json.JSONObject;


public class MyTests {

    public static void main(String[] args) {

        MongoCursor<Document> cursor;

//        String fullQuery = "db.products.find()";
//        Boolean isRightPattern = fullQuery.matches("^db\\.(.+)find([\\(])(.*)([\\)])(\\;*)$");
//        System.out.println(isRightPattern);
//
//        String collectionName = StringUtils.substringBetween(fullQuery, "db.", ".find");
//        System.out.println(collectionName);
//        MongoClient mongo = new MongoClient("localhost",27017);
//        MongoDatabase database = mongo.getDatabase("test");
//        MongoCollection<Document> collection = database.getCollection(collectionName);
//
//
//        String queryFilters = fullQuery.substring(fullQuery.indexOf('(') + 1, fullQuery.lastIndexOf(')'));
//        //System.out.println("eu:"+queryFilters.getClass().getName());
//
//        if (queryFilters.isEmpty()) {
//            cursor = collection.find();
//        } else {
//            BasicDBObject obj = BasicDBObject.parse(queryFilters);
//            cursor = collection.find(obj);
//        }
//        //cursor = collection.find();
//        Iterator it = cursor.iterator();
//        while (it.hasNext()) {
//            Object doc = it.next();
//            System.out.println(doc);
//        }
//        cursor.iterator().close();

        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoDatabase database = mongoClient.getDatabase("test");
        MongoCollection collection = database.getCollection("table");

        List<DBObject> criteria = new ArrayList<>();
        criteria.add(new BasicDBObject("incr", new BasicDBObject("$gt", 12)));
        criteria.add(new BasicDBObject("incr", new BasicDBObject("$type", "number")));
        criteria.add(new BasicDBObject(BasicDBObject.parse("")));

        BasicDBObject gtQuery = new BasicDBObject("$and", criteria);
        BasicDBObject gtQuery1 = new BasicDBObject();
        cursor = collection.find(gtQuery).iterator();


        while (cursor.hasNext()) {
            String res = cursor.next().toJson();
            JSONObject jsonObj = new JSONObject(res);
            Double qsddd = Double.valueOf((Double) jsonObj.get("incr"));
            System.out.println(qsddd);
        }
    }
}


