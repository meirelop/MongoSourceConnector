package com.meirkhan.kafka;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class MySourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(MySourceTask.class);
  public MySourceConnectorConfig config;
  public MongoCollection collection;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    //TODO: Do things here that are required to start your task. This could be open a connection to a database, etc.
    config = new MySourceConnectorConfig(map);
    MongoClient mongoClient = new MongoClient(config.getMongoHost(), config.getMongoPort());
    MongoDatabase database = mongoClient.getDatabase(config.getMongoDbName());
    this.collection = database.getCollection(this.config.getMongoCollectionName());
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
    FindIterable cursor;

    if (this.config.getMongoQueryFilters().isEmpty()) {
      cursor = collection.find();
    } else {
      BasicDBObject obj = BasicDBObject.parse(this.config.getMongoQueryFilters());
      cursor = collection.find(obj);
    }
    Iterator iter = cursor.iterator();
    while (iter.hasNext()) {
      SourceRecord sourceRecord = generateSourceRecord(iter.next());
      records.add(sourceRecord);
    }
    cursor.iterator().close();
    TimeUnit.SECONDS.sleep(config.getPollInterval());
    return records;
  }

  @Override
  public void stop() {
    //TODO: Do whatever is required to stop your task.
  }

  private SourceRecord generateSourceRecord(Object issue) {
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