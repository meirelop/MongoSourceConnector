package com.orange.kafka;

import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.bson.Document;
import org.slf4j.Logger;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

abstract class TableQuerier {
    static final Logger log = LoggerFactory.getLogger(MongodbSourceConnectorConfig.class);
    private String mongoUri;
    private String dbName;
    private String collectionName;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private String topic;

    public TableQuerier(
                        String topic,
                        String mongoUri,
                        String dbName,
                        String collectionName
                        )
    {   this.topic = topic;
        this.mongoUri = mongoUri;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.mongoClient = new MongoClient(new MongoClientURI(mongoUri));
        this.database = mongoClient.getDatabase(dbName);
        isCollectionExist();

    }

    public abstract boolean hasNext();

    public abstract void executeCursor();

    public abstract void closeCursor();

    public abstract SourceRecord extractRecord();

    public void isCollectionExist() {
        boolean exists;
        List<String> collectionsList = new ArrayList<>();
        MongoIterable<String> collectionObjects = this.database.listCollectionNames();
        collectionObjects.into(collectionsList);
        exists = collectionsList.contains(this.collectionName);
        if(!exists) {
            throw new ConfigException(String.format("There is no such collection '%s' in database: '%s'",
                    this.collectionName, this.dbName));
        }

    }
}
