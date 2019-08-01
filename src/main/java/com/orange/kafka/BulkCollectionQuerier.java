package com.orange.kafka;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BulkCollectionQuerier always returns the entire collection.
 */
public class BulkCollectionQuerier extends TableQuerier{
    static final Logger log = LoggerFactory.getLogger(MongodbSourceConnectorConfig.class);
    protected MongoCursor<Document> cursor;
    private String DBname;
    private String collectionName;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection collection;
    private String topic;
    private String includeFields;
    private String excludeFields;


    public BulkCollectionQuerier(String topic,
                                 String mongoUri,
                                 String DBname,
                                 String collectionName,
                                 String includeFields,
                                 String excludeFields
                                 ) {
        super(topic,mongoUri,DBname,collectionName, includeFields, excludeFields);

        this.topic = topic;
        this.collectionName = collectionName;
        this.DBname = DBname;
        this.includeFields = includeFields;
        this.excludeFields = excludeFields;
        this.mongoClient = new MongoClient(new MongoClientURI(mongoUri));
        this.database = mongoClient.getDatabase(DBname);
        this.collection = database.getCollection(collectionName);
    }

    private Map<String, String> sourcePartition() {
        Map<String, String> map = new HashMap<>();
        map.put(Constants.DATABASE_NAME_FIELD, DBname);
        map.put(Constants.COLLECTION_FIELD, collectionName);
        return map;
    }

    public void executeCursor() {
        if(!excludeFields.isEmpty()) {
            List<String> fieldsList = Arrays.asList(excludeFields.split("\\s*,\\s*"));
            cursor = collection.find().projection(Projections.exclude(fieldsList)).iterator();
        }
        else if (!includeFields.isEmpty()) {
            List<String> fieldsList = Arrays.asList(includeFields.split("\\s*,\\s*"));
            cursor = collection.find().projection(Projections.include(fieldsList)).iterator();
        }else {
            cursor = collection.find().iterator();
        }
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
