
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

/**
 * <p>
 *   TimestampIncrementingQuerier performs incremental loading of data using two mechanisms: a
 *   timestamp column provides monotonically incrementing values that can be used to detect new or
 *   modified rows and a strictly incrementing (e.g. auto increment) column allows detecting new
 *   rows or combined with the timestamp provide a unique identifier for each update to the row.
 * </p>
 * <p>
 *   Both columns timestamp and incrementing columns must be specified. They are both
 *   used to ensure only new or updated rows are reported and to totally order updates so
 *   recovery can occur no matter when offsets were committed.
 * </p>
 * @author Meirkhan Rakhmetzhanov
 */
public class TimestampIncrementQuerier extends TableQuerier{
    static final Logger log = LoggerFactory.getLogger(MongodbSourceConnectorConfig.class);
    private String topic;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection collection;
    MongoCursor<Document> cursor;
    private String timestampColumn;
    private String DBname;
    private String collectionName;
    private Instant lastDate;
    private Instant recordDate;
    private Double lastIncrement;
    private Double recordIncrement;
    private String incrementColumn;

    /**
     * Constructs and initailizes an TimestampIncrementQuerier.
     * @param topic topic name to produce
     * @param mongoUri mongo connection string
     * @param DBname name od database in mongodb
     * @param collectionName collection name parsed from query string
     * @param incrementColumn name of incrementing ID field in MongoDB collection
     * @param lastIncrement last value of increment ID, querier will take records bigger than this value
     * @param timestampColumn name of date field in MongoDB collection
     * @param lastDate latest date, querier will take records bigger than this date
     */
    public TimestampIncrementQuerier
            (
                    String topic,
                    String mongoUri,
                    String DBname,
                    String collectionName,
                    String timestampColumn,
                    Instant lastDate,
                    String incrementColumn,
                    Double lastIncrement
            )
    {
        super(topic, mongoUri,DBname,collectionName);
        this.topic = topic;
        this.timestampColumn = timestampColumn;
        this.DBname = DBname;
        this.collectionName = collectionName;
        this.lastDate = lastDate;
        this.mongoClient = new MongoClient(new MongoClientURI(mongoUri));
        this.database = mongoClient.getDatabase(DBname);
        this.collection = database.getCollection(collectionName);
        this.incrementColumn = incrementColumn;
        this.lastIncrement = lastIncrement;
    }

    private Map<String, String> sourcePartition() {
        Map<String, String> map = new HashMap<>();
        map.put(Constants.DATABASE_NAME_FIELD, DBname);
        map.put(Constants.COLLECTION_FIELD, collectionName);
        return map;
    }

    private Map<String, String> sourceOffset() {
        Map<String, String> map = new HashMap<>();
        lastDate = DateUtils.MaxInstant(lastDate, recordDate);
        map.put(Constants.LAST_TIME_FIELD, lastDate.toString());
        lastIncrement = Math.max(lastIncrement, recordIncrement);
        map.put(Constants.INCREMENTING_FIELD, lastIncrement.toString());
        return map;
    }

    private BasicDBObject createQuery() {
        List<DBObject> criteria = new ArrayList<>();
        criteria.add(new BasicDBObject(timestampColumn, new BasicDBObject(Constants.MONGO_CMD_GREATER, lastDate)));
        criteria.add(new BasicDBObject(timestampColumn, new BasicDBObject(Constants.MONGO_CMD_TYPE, Constants.MONGO_DATE_TYPE)));
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
        recordDate = record.getDate(timestampColumn).toInstant();
        recordIncrement = record.getDouble(incrementColumn);

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
