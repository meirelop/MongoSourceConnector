
package com.orange.kafka;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.orange.kafka.MongodbSourceTask.ArrayEncoding;

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
    private String includeFields;
    private String excludeFields;
    private ArrayEncoding arrayEncoding;

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
            (       ArrayEncoding arrayEncoding,
                    String topic,
                    String mongoUri,
                    String DBname,
                    String collectionName,
                    String includeFields,
                    String excludeFields,
                    String incrementColumn,
                    Double lastIncrement
            )
    {
        super(arrayEncoding,topic,mongoUri,DBname,collectionName, includeFields, excludeFields);
        this.arrayEncoding = arrayEncoding;
        this.topic = topic;
        this.incrementColumn = incrementColumn;
        this.lastIncrement = lastIncrement;
        this.DBname = DBname;
        this.collectionName = collectionName;
        this.includeFields = includeFields;
        this.excludeFields = excludeFields;
        this.mongoClient = new MongoClient(new MongoClientURI(mongoUri));
        this.database = mongoClient.getDatabase(DBname);
        this.collection = database.getCollection(collectionName);
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
        BasicDBObject query = createQuery();
        if(!excludeFields.isEmpty()) {
            List<String> fieldsList = Arrays.asList(excludeFields.split("\\s*,\\s*"));
            cursor = collection.find(query).projection(Projections.exclude(fieldsList)).iterator();
        }
        else if (!includeFields.isEmpty()) {
            List<String> fieldsList = Arrays.asList(includeFields.split("\\s*,\\s*"));
            cursor = collection.find(query).projection(Projections.include(fieldsList)).iterator();
        }else {
            cursor = collection.find(query).iterator();
        }
    }

    public void closeCursor(){
        cursor.close();
    }

    public boolean hasNext() {
        return cursor.hasNext();
    }

    @Override
    public SourceRecord extractRecord() {
        DataConverter converter = new DataConverter(arrayEncoding);
        Document record = cursor.next();
        recordIncrement = record.getDouble(incrementColumn);
        BsonDocument bsonRecord = record.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());
        Set<Map.Entry<String, BsonValue>> keyValuePairs = bsonRecord.entrySet();

        SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct().name(collectionName);
        converter.setSchema(keyValuePairs, valueSchemaBuilder);
        Schema valueSchema = valueSchemaBuilder.build();

        Struct value = new Struct(valueSchema);
        converter.setStruct(keyValuePairs, value, valueSchema);

        return new SourceRecord(
                sourcePartition(),
                sourceOffset(),
                topic,
                null,
                null,
                null,
                valueSchema,
                value);
    }
}
