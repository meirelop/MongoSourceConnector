package com.orange.kafka;

import com.mongodb.MongoClientURI;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.source.SourceRecord;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract class to execute queries against specific collection. Implementations handle different types
 * of queries: periodic bulk loading, incremental loads using auto incrementing IDs, incremental
 * loads using timestamps, etc.
 */

abstract class TableQuerier {
    static final Logger log = LoggerFactory.getLogger(MongodbSourceConnectorConfig.class);
    private String mongoUri;
    private String DBname;
    private String collectionName;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private String topic;
    private String includeFields;
    private String excludeFields;

    /**
     * Constructs and initailizes a TableQuerier.
     * @param topic topic name to produce
     * @param mongoUri mongo connection string
     * @param DBname name od database in mongodb
     * @param collectionName collection name parsed from query string
     */

    public TableQuerier(
                        String topic,
                        String mongoUri,
                        String DBname,
                        String collectionName,
                        String includeFields,
                        String excludeFields
                        )
    {   this.topic = topic;
        this.mongoUri = mongoUri;
        this.DBname = DBname;
        this.collectionName = collectionName;
        this.includeFields = includeFields;
        this.excludeFields = excludeFields;
        this.mongoClient = new MongoClient(new MongoClientURI(mongoUri));
        this.database = mongoClient.getDatabase(DBname);
        isCollectionExist();

    }

    /**
     * Checks cursor for availability of record
     * @return true if there is data in cursor
     */
    public abstract boolean hasNext();

    /**
     * Executes cursor with prepared query and prepares iterator
     */
    public abstract void executeCursor();

    public abstract void closeCursor();

    /**
     * Generate and record one or more source records to describe the given event.
     * @return Newly generated record
     */
    public abstract SourceRecord extractRecord();

    /**
     * Check if there such collection in given database
     * @throws ConfigException if does not exist
     */
    public void isCollectionExist() {
        boolean exists;
        List<String> collectionsList = new ArrayList<>();
        MongoIterable<String> collectionObjects = this.database.listCollectionNames();
        collectionObjects.into(collectionsList);
        exists = collectionsList.contains(this.collectionName);
        if(!exists) {
            throw new ConfigException(String.format("There is no such collection '%s' in database: '%s'",
                    this.collectionName, this.DBname));
        }

    }
}
