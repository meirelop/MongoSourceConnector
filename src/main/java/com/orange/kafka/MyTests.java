package com.orange.kafka;

import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.time.Instant;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCursor;
import com.mongodb.DBObject;
import java.util.List;
import java.util.ArrayList;


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
        MongoCollection collection = database.getCollection("table4");
        BasicDBObject query = new BasicDBObject();

        List<DBObject> criteria = new ArrayList<>();
//        criteria.add(new BasicDBObject("time", new BasicDBObject(MONGO_CMD_GREATER, lastDate)));
        criteria.add(new BasicDBObject("time", new BasicDBObject(Constants.MONGO_CMD_LESS, Instant.now())));
        criteria.add(new BasicDBObject("time", new BasicDBObject(Constants.MONGO_CMD_TYPE, Constants.MONGO_DATE_TYPE)));
        query = new BasicDBObject(Constants.MONGO_CMD_AND, criteria);




//        while (cursor.hasNext()) {
//            String res = cursor.next().toJson();
//            JSONObject jsonObj = new JSONObject(res);
//            Double qsddd = Double.valueOf((Double) jsonObj.get("incr"));
//            System.out.println(res);
//        }
    }
}


