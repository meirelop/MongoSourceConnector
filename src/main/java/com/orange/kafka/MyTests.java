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
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;


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



        String json_str = "[{$unwind : \"$views\"}, {$match : {\"views.isActive\" : true}},\n" +
                "    {$sort : {\"views.date\" : 1}},\n" +
                "    {$limit : 200},\n" +
                "    {$project : {\"_id\" : 0, \"url\" : \"$views.url\", \"date\" : \"$views.date\"}}]\n";

        String qsd = "{\"$unwind\":\"$views\"}";

        final JSONArray obj = new JSONArray(json_str);
//        System.out.println(obj);
        for(Object i: obj) {
            String obj2 = i.toString();
            JSONObject obj3 = new JSONObject(obj2);
            JSONArray keys = obj3.names();
//            System.out.println(keys);
        }


        JSONObject jsonObject = new JSONObject(json_str.trim());
        Iterator<String> keys = jsonObject.keys();

        while(keys.hasNext()) {
            String key = keys.next();
            if (jsonObject.get(key) instanceof JSONObject) {
                // do something with jsonObject here
                System.out.println((key));
            }
        }


//        JSONObject qs = new JSONObject(qsd);
//        System.out.println(qs);

//        while (cursor.hasNext()) {
//            String res = cursor.next().toJson();
//            JSONObject jsonObj = new JSONObject(res);
//            Double qsddd = Double.valueOf((Double) jsonObj.get("incr"));
//            System.out.println(res);
//        }






    }
}


