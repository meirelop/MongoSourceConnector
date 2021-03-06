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
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.BsonDocument;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.*;

public class MyTests {


    public static void main(String[] args) {
        String mongoUri = "mongodb://localhost:27020/test";
        String DBname = "test";
        String collectionName = "test";

        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoUri));
        MongoDatabase database = mongoClient.getDatabase(DBname);
        MongoCollection collection = database.getCollection(collectionName);

        //collection.find().projection()
//        BasicDBObject query = BasicDBObject.parse(mongoquery);
        MongoCursor<Document> cursor = collection.find().iterator();
        while (cursor.hasNext()) {
            Document record = cursor.next();
//            new DataConverter().getSchema(record);
//            JSONObject jsonObject = new JSONObject(record.toJson());
//            System.out.println(jsonObject);
//            System.out.println(record.get("ts").getClass());
//            Object o = record.get("ts");
//            org.bson.BsonTimestamp q = (org.bson.BsonTimestamp) o;
//            int res = q.getTime();


            BsonDocument bsonDoc = record.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());
            System.out.println(bsonDoc.entrySet());
            for (Map.Entry<String, BsonValue> keyPairsForSchema : bsonDoc.entrySet()) {
                System.out.println(keyPairsForSchema.getValue());
            }


//            Iterator<String> keys = jsonObject.keys();
//
//            while (keys.hasNext()) {
//                String key = keys.next();
////                System.out.println(key);
//
//                if (jsonObject.get(key) instanceof JSONObject) {
//                    // do something with jsonObject here
//                    System.out.println(jsonObject.get(key));
//                }
//            }

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
}


