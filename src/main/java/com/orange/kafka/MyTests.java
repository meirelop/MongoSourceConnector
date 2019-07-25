package com.orange.kafka;

import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoIterable;
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
        List<String> list = new ArrayList<>();
        String uri = "mongodb://testuser:pwd1@localhost:27017/test";
        MongoClient mongoClient = new MongoClient(new MongoClientURI(uri));
        MongoDatabase database = mongoClient.getDatabase("test");
        MongoIterable<String> colls = database.listCollectionNames();
        System.out.println(colls.into(list));
        String collectionName="test31";


        boolean temp;
        temp = list.contains(collectionName);
        System.out.println(temp);
    }
}


