package com.meirkhan.kafka;


import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;


import java.util.*;
import java.util.concurrent.TimeUnit;
import java.time.Instant;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import static com.meirkhan.kafka.MySchemas.*;

public class MySourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(MySourceTask.class);
  public MySourceConnectorConfig config;
  public MongoCollection collection;
  protected Instant lastNumber;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    log.info("Starting MongoDB source task");
    try {
      config = new MySourceConnectorConfig(map);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start MongoDBSourceTask due to configuration error", e);
    }


    MongoClient mongoClient = new MongoClient(config.getMongoHost(), config.getMongoPort());
    MongoDatabase database = mongoClient.getDatabase(config.getMongoDbName());
    this.collection = database.getCollection(this.config.getMongoCollectionName());
    initializeLastVariables();
  }

  private void initializeLastVariables() {
      Map<String, Object> lastSourceOffset = null;
      lastSourceOffset = context.offsetStorageReader().offset(sourcePartition());
      if (lastSourceOffset==null) {
          lastNumber = Instant.now();
          //System.out.println("No offset while initializing");
      } else {
          Object lastNumberObj = lastSourceOffset.get(CURRENT_TIME_FIELD);
          if (lastNumberObj instanceof String) {
              lastNumber = Instant.parse((String) lastNumberObj);
          }
          //System.out.printf("Offset in initializing: %s", lastNumberObj.toString());
      }
  }


  private Map<String, String> sourcePartition() {
    Map<String, String> map = new HashMap<>();
    map.put(DATABASE_NAME_FIELD, this.config.getMongoDbName());
    map.put(COLLECTION_FIELD, this.config.getMongoCollectionName());
    return map;
  }

  private Map<String, String> sourceOffset(Instant updatedAt) {
    Map<String, String> map = new HashMap<>();
    map.put(CURRENT_TIME_FIELD, updatedAt.toString());
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
    System.out.println(context.offsetStorageReader().offset(sourcePartition()));
    return records;
  }

  @Override
  public void stop() {
    //TODO: Do whatever is required to stop your task.
  }

  private SourceRecord generateSourceRecord(Object issue) {
//    switch (mode) {
//      case TABLE:
//        String name = tableId.tableName(); // backwards compatible
//        partition = Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, name);
//        topic = topicPrefix + name;
//        break;
//      case QUERY:
//        partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY
//        topic = topicPrefix;
//        break;
//      default:
//        throw new ConnectException("Unexpected query mode: " + mode);

    return new SourceRecord(
            sourcePartition(),
            sourceOffset(Instant.now()),
            config.getTopic(),
            null, // partition will be inferred by the framework
            null,
            null,
            null,
            issue.toString());
  }
}