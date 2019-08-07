package com.orange.kafka;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

import com.mongodb.client.model.Projections;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import com.mongodb.util.JSON;


import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import org.json.JSONObject;

public class MyTests {


    public static void main(String[] args) {
        String  mongoUri = "mongodb://testuser:pwd1@localhost:27020/test";
        String DBname = "test";
        String collectionName = "products";

        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoUri));
        MongoDatabase database = mongoClient.getDatabase(DBname);
        MongoCollection collection = database.getCollection(collectionName);

        //collection.find().projection()
//        BasicDBObject query = BasicDBObject.parse(mongoquery);
        MongoCursor<Document> cursor = collection.find().iterator();
        while (cursor.hasNext()) {
            Document record = cursor.next();
            JSONObject jsonObject = new JSONObject(record.toJson());
            System.out.println(jsonObject.getClass().getName());
        }

//        FindIterable<Document> dumps = collection.find();

//        FindIterable it = contCol.find().projection(excludeId());

//        collection.find().projection(fields(include("x", "y"), excludeId()))
//        ArrayList<Document> docs = new ArrayList();
//        it.into(docs);
//
//        for (Document doc : dumps) {
//            System.out.println(doc);
//        }

    }
}

