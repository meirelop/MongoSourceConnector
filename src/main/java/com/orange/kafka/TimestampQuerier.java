
package com.orange.kafka;

import com.orange.kafka.utils.DateUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant;
import java.util.*;
import static com.orange.kafka.Constants.*;


public class TimestampQuerier extends TableQuerier{
    static final Logger log = LoggerFactory.getLogger(MongodbSourceConnectorConfig.class);
    private String topic;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection collection;
    MongoCursor<Document> cursor;
    private String timestampColumn;
    private String dbName;
    private String collectionName;
    private Instant lastDate;
    private Instant recordDate;

    public TimestampQuerier
            (
                    String topic,
                    String mongoUri,
                    String mongoHost,
                    int mongoPort,
                    String dbName,
                    String collectionName,
                    String timestampColumn,
                    Instant lastDate
            )
    {
        super(topic,mongoUri, mongoHost,mongoPort,dbName,collectionName);
        this.topic = topic;
        this.timestampColumn = timestampColumn;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.lastDate = lastDate;
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

    private Map<String, String> sourceOffset() {
        Map<String, String> map = new HashMap<>();
        lastDate = DateUtils.MaxInstant(lastDate, recordDate);
        map.put(LAST_TIME_FIELD, lastDate.toString());
        return map;
    }

    private BasicDBObject createQuery() {
        List<DBObject> criteria = new ArrayList<>();
        criteria.add(new BasicDBObject(timestampColumn, new BasicDBObject(MONGO_CMD_GREATER, lastDate)));
        //criteria.add(new BasicDBObject(timestampColumn, new BasicDBObject(MONGO_CMD_LESS, Instant.now())));
        criteria.add(new BasicDBObject(timestampColumn, new BasicDBObject(MONGO_CMD_TYPE, MONGO_DATE_TYPE)));
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
        recordDate = record.getDate(timestampColumn).toInstant();

        return new SourceRecord(
                sourcePartition(),
                sourceOffset(),
                topic,
                null, // partition will be inferred by the framework
                null,
                null,
                null,
                record.toJson());
    }
}
