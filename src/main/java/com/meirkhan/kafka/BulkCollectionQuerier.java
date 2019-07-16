package com.meirkhan.kafka;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.meirkhan.kafka.MySchemas.*;


public class BulkCollectionQuerier extends TableQuerier{
    static final Logger log = LoggerFactory.getLogger(MySourceConnectorConfig.class);
    protected MongoCursor<Document> cursor;
    private String dbName;
    private String collectionName;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection collection;
    private String topic;


    public BulkCollectionQuerier(String topic,
                                 String mongoUri,
                                 String mongoHost,
                                 int mongoPort,
                                 String dbName,
                                 String collectionName
                                 ) {
        super(topic,mongoUri, mongoHost,mongoPort,dbName,collectionName);
        this.topic = topic;
        this.collectionName = collectionName;
        this.dbName = dbName;
        if(!mongoUri.isEmpty()) {
            this.mongoClient = new MongoClient(new MongoClientURI(mongoUri));
        } else {
            this.mongoClient = new MongoClient(mongoHost,mongoPort);
        }
        this.database = mongoClient.getDatabase(dbName);
        this.collection = database.getCollection(collectionName);
    }

    private Map<String, String> sourcePartition() {
        Map<String, String> map = new HashMap<>();
        map.put(DATABASE_NAME_FIELD, dbName);
        map.put(COLLECTION_FIELD, collectionName);
        return map;
    }

    public void executeCursor() {
        //collection.find().projection()
        cursor = collection.find().iterator();
    }

    public void closeCursor(){
        cursor.close();
    }

    public boolean hasNext() {
        return cursor.hasNext();
    }

    public SourceRecord extractRecord() {
        Document record = cursor.next();
        return new SourceRecord(
                sourcePartition(),
                null,
                topic,
                null, // partition will be inferred by the framework
                null,
                null,
                null,
                record.toJson());
    }
}
