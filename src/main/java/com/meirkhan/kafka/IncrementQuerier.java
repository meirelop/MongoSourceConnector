
package com.meirkhan.kafka;

import com.meirkhan.kafka.utils.DateUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.meirkhan.kafka.MySchemas.*;
import java.util.*;
import java.lang.Math;
import org.json.JSONObject;


public class IncrementQuerier extends TableQuerier{
    static final Logger log = LoggerFactory.getLogger(MySourceConnectorConfig.class);
    private String topic;
    private String incrementColumn;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection collection;
    MongoCursor<Document> cursor;
    private Double lastIncrement;
    private Double recordIncrement;
    private String dbName;
    private String collectionName;

    public IncrementQuerier
            (
                    String topic,
                    String mongoHost,
                    int mongoPort,
                    String dbName,
                    String collectionName,
                    String incrementColumn,
                    Double lastIncrement
            )
    {
        super(topic, mongoHost,mongoPort,dbName,collectionName);
        this.topic = topic;
        this.incrementColumn = incrementColumn;
        this.lastIncrement = lastIncrement;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.mongoClient = new MongoClient(mongoHost, mongoPort);
        this.database = mongoClient.getDatabase(dbName);
        this.collection = database.getCollection(collectionName);
    }


    private Map<String, String> sourcePartition() {
        Map<String, String> map = new HashMap<>();
        map.put(DATABASE_NAME_FIELD, dbName);
        map.put(COLLECTION_FIELD, collectionName);
        return map;
    }

    private Map<String, String> sourceOffset() {
        Map<String, String> map = new HashMap<>();
        lastIncrement = Math.max(lastIncrement, recordIncrement);
        map.put(INCREMENTING_FIELD, lastIncrement.toString());
        return map;
    }

    private BasicDBObject createQuery() {
        List<DBObject> criteria = new ArrayList<>();
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject(MONGO_CMD_GREATER, lastIncrement)));
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject(MONGO_CMD_TYPE, MONGO_NUMBER_TYPE)));
        return new BasicDBObject(MONGO_CMD_AND, criteria);
    }

    public void executeCursor() {
        //collection.find().projection()
        BasicDBObject query = createQuery();
        cursor = collection.find(query).iterator();
    }

    public void closeCursor(){
        cursor.close();
    }

    public boolean hasNext() {
        return cursor.hasNext();
    }

    @Override
    public SourceRecord extractRecord() {
        Document record = cursor.next();
        recordIncrement = record.getDouble(incrementColumn);
        //JSONObject jsonObj = new JSONObject(record);

        return new SourceRecord(
                sourcePartition(),
                sourceOffset(),
                topic,
                null,
                null,
                null,
                null,
                record.toString());
    }
}
