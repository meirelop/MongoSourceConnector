
package com.orange.kafka;

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

import java.util.*;
import java.lang.Math;

/**
 * <p>
 *   IncrementQuerier performs incremental loading of data using increment column.
 *   increment column provides monotonically incrementing values that can be used to detect new or
 *   modified rows where increment id column is modified.
 * </p>
 * @author Meirkhan Rakhmetzhanov
 */
public class IncrementQuerier extends TableQuerier{
    static final Logger log = LoggerFactory.getLogger(MongodbSourceConnectorConfig.class);
    private String topic;
    private String incrementColumn;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection collection;
    MongoCursor<Document> cursor;
    private Double lastIncrement;
    private Double recordIncrement;
    private String DBname;
    private String collectionName;

    /**
     * Constructs and initailizes an IncrementQuerier.
     * @param topic topic name to produce
     * @param mongoUri mongo connection string
     * @param DBname name od database in mongodb
     * @param collectionName collection name parsed from query string
     * @param incrementColumn name of incrementing ID field in MongoDB collection
     * @param lastIncrement last value of increment ID, querier will take records bigger than this value
     */
    public IncrementQuerier
            (
                    String topic,
                    String mongoUri,
                    String DBname,
                    String collectionName,
                    String incrementColumn,
                    Double lastIncrement
            )
    {
        super(topic, mongoUri,DBname,collectionName);
        this.topic = topic;
        this.incrementColumn = incrementColumn;
        this.lastIncrement = lastIncrement;
        this.DBname = DBname;
        this.collectionName = collectionName;
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

    private Map<String, String> sourceOffset() {
        Map<String, String> map = new HashMap<>();
        lastIncrement = Math.max(lastIncrement, recordIncrement);
        map.put(Constants.INCREMENTING_FIELD, lastIncrement.toString());
        return map;
    }

    private BasicDBObject createQuery() {
        List<DBObject> criteria = new ArrayList<>();
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject(Constants.MONGO_CMD_GREATER, lastIncrement)));
        criteria.add(new BasicDBObject(incrementColumn, new BasicDBObject(Constants.MONGO_CMD_TYPE, Constants.MONGO_NUMBER_TYPE)));
        log.debug("{} prepared mongodb query: {}", this, criteria);
        return new BasicDBObject(Constants.MONGO_CMD_AND, criteria);
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

        return new SourceRecord(
                sourcePartition(),
                sourceOffset(),
                topic,
                null,
                null,
                null,
                null,
                record.toJson());
    }
}
