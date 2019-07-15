
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

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.text.SimpleDateFormat;

import static com.meirkhan.kafka.MySchemas.*;

public class IncrementQuerier extends TableQuerier{
    static final Logger log = LoggerFactory.getLogger(MySourceConnectorConfig.class);
    private String topic;
    private String incrementColumn;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection collection;
    MongoCursor<Document> cursor;
    private Double lastIncrement;
    private String timestampColumn;
    private String dbName;
    private String collectionName;
    private Instant lastDate;
    private Instant recordDate;
    private Double recordIncrement;

    public IncrementQuerier
            (
                    String topic,
                    String mongoHost,
                    int mongoPort,
                    String dbName,
                    String collectionName,
                    String incrementColumn,
                    String timestampColumn,
                    Double lastIncrement,
                    Instant lastDate
            )
    {
        super(topic, mongoHost,mongoPort,dbName,collectionName);
        this.topic = topic;
        this.incrementColumn = incrementColumn;
        this.timestampColumn = timestampColumn;
        this.lastIncrement = lastIncrement;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.mongoClient = new MongoClient(mongoHost, mongoPort);
        this.database = mongoClient.getDatabase(dbName);
        this.collection = database.getCollection(collectionName);
        this.lastDate = lastDate;
    }


    private Map<String, String> sourcePartition() {
        Map<String, String> map = new HashMap<>();
        map.put(DATABASE_NAME_FIELD, dbName);
        map.put(COLLECTION_FIELD, collectionName);
        return map;
    }

    private Map<String, String> sourceOffset() {
        Map<String, String> map = new HashMap<>();

        if(lastDate != null) {
            String maxDate = DateUtils.MaxInstant(recordDate, lastDate).toString();
            lastDate = DateUtils.MaxInstant(recordDate, lastDate);
            map.put(LAST_TIME_FIELD, maxDate);
        }
        if(lastIncrement != null) {
            map.put(INCREMENTING_FIELD, lastIncrement.toString());
        }
        return map;
    }


    public BasicDBObject createQuery() {
        List<DBObject> criteria = new ArrayList<>();
        BasicDBObject query = new BasicDBObject();

        if (lastIncrement != null && lastDate != null){
        } else if(lastIncrement == null) {
            criteria.add(new BasicDBObject(timestampColumn, new BasicDBObject(MONGO_CMD_GREATER, lastDate)));
            criteria.add(new BasicDBObject(timestampColumn, new BasicDBObject(MONGO_CMD_LESS, Instant.now())));
            criteria.add(new BasicDBObject(timestampColumn, new BasicDBObject(MONGO_CMD_TYPE, MONGO_DATE_TYPE)));
            query = new BasicDBObject(MONGO_CMD_AND, criteria);
        } else if(lastDate == null) {
            criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject(MONGO_CMD_GREATER, lastIncrement)));
            criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject(MONGO_CMD_TYPE, MONGO_NUMBER_TYPE)));
            query = new BasicDBObject(MONGO_CMD_AND, criteria);
        }
        return query;
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

        if(lastIncrement != null) {
            lastIncrement = record.getDouble(incrementColumn);
        }

        if(lastDate != null) {
            recordDate = record.getDate(timestampColumn).toInstant();
        }

        return new SourceRecord(
                sourcePartition(),
                sourceOffset(),
                topic,
                null, // partition will be inferred by the framework
                null,
                null,
                null,
                record.toString());
    }
}
