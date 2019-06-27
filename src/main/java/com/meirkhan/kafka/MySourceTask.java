package com.meirkhan.kafka;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.BasicDBObject;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import java.util.*;

public class MySourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(MySourceTask.class);
  public MySourceConnectorConfig config;
  public DBCollection collection;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    //TODO: Do things here that are required to start your task. This could be open a connection to a database, etc.
    config = new MySourceConnectorConfig(map);
    MongoClient mongoClient = new MongoClient(config.getMongoHost(), config.getMongoPort());
    DB database = mongoClient.getDB(config.getMongoDbName());
    //DBCollection collection = database.getCollection(config.getMongoCollection());
    this.collection = database.getCollection(config.getMongoCollection());
  }

  private Map<String, String> sourcePartition() {
    Map<String, String> map = new HashMap<>();
    map.put("1", "2");
    map.put("3", "4");
    return map;
  }

  private Map<String, String> sourceOffset() {
    Map<String, String> map = new HashMap<>();
    map.put("5", "6");
    map.put("7", "8");
    return map;
  }


  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    final ArrayList<SourceRecord> records = new ArrayList<>();
    BasicDBObject searchQuery = new BasicDBObject();
    //searchQuery.put("item", "pen");
    DBCursor cursor = collection.find(searchQuery);
    while (cursor.hasNext()) {
      SourceRecord sourceRecord = generateSourceRecord(cursor.next());
      records.add(sourceRecord);
    }
    cursor.close();
    return records;
  }

  @Override
  public void stop() {
    //TODO: Do whatever is required to stop your task.
  }

  private SourceRecord generateSourceRecord(DBObject issue) {
    return new SourceRecord(
            sourcePartition(),
            sourceOffset(),
            config.getTopic(),
            null, // partition will be inferred by the framework
            null,
            null,
            null,
            issue.toString());
  }
}