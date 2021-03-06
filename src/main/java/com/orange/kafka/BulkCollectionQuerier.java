package com.orange.kafka;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import java.util.*;

import com.orange.kafka.MongodbSourceTask.ArrayEncoding;

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
    private MongodbSourceTask.ArrayEncoding arrayEncoding;

    public BulkCollectionQuerier(ArrayEncoding arrayEncoding,
                                 String topic,
                                 String mongoUri,
                                 String DBname,
                                 String collectionName,
                                 String includeFields,
                                 String excludeFields) {
        super(arrayEncoding,topic,mongoUri,DBname,collectionName, includeFields, excludeFields);

        this.arrayEncoding = arrayEncoding;
        this.topic = topic;
        this.collectionName = collectionName;
        this.DBname = DBname;
        this.includeFields = includeFields;
        this.excludeFields = excludeFields;
        this.mongoClient = new MongoClient(new MongoClientURI(mongoUri));
        this.database = mongoClient.getDatabase(DBname);
        this.collection = database.getCollection(collectionName);
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
        DataConverter converter = new DataConverter(arrayEncoding);

//        ObjectId objectID = (ObjectId) record.remove("_id");
//        Schema keySchema = converter.keySchema;
//        Struct keyStruct = converter.getKeyStruct(objectID);

        BsonDocument bsonRecord = record.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());
        Set<Map.Entry<String, BsonValue>> keyValuePairs = bsonRecord.entrySet();

        SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct().name(collectionName);
        converter.setSchema(keyValuePairs, valueSchemaBuilder);
        Schema valueSchema = valueSchemaBuilder.build();

        Struct value = new Struct(valueSchema);
        converter.setStruct(keyValuePairs, value, valueSchema);

        return new SourceRecord(
                sourcePartition(),
                null,
                topic,
                null,
                null,
                null,
                valueSchema,
                value);
    }
}
