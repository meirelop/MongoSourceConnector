package com.meirkhan.kafka;


import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.errors.ConnectException;
import org.bson.Document;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;


import java.util.PriorityQueue;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.time.Instant;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import static com.meirkhan.kafka.MySchemas.*;

public class MySourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(MySourceTask.class);
  public MySourceConnectorConfig config;
  //public MongoCollection collection;
  protected Instant lastNumber;
  protected Double lastIncrement;
//  protected MongoCursor<Document> cursor;
  String mode;
  private PriorityQueue<TableQuerier> tableQueue = new PriorityQueue<TableQuerier>();

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

    this.mode = config.getModeName();

    //MongoClient mongoClient = new MongoClient(config.getMongoHost(), config.getMongoPort());
    //MongoDatabase database = mongoClient.getDatabase(config.getMongoDbName());
    //this.collection = database.getCollection(this.config.getMongoCollectionName());
    initializeLastVariables();

    if (mode.equals(INCREMENTING_FIELD)) {
      log.info("Creating IncrementQuerier instance");
      log.info(config.getMongoHost());
      log.info(config.getMongoCollectionName());
      log.info(config.getMongoDbName());
      log.info(config.getIncrementColumn());
      tableQueue.add(
              new IncrementQuerier(
                      config.getMongoHost(),
                      config.getMongoPort(),
                      config.getMongoDbName(),
                      config.getMongoCollectionName(),
                      config.getIncrementColumn()
              )
      );
    }
  }

  private void initializeLastVariables() {
      Map<String, Object> lastSourceOffset;
      lastSourceOffset = context.offsetStorageReader().offset(sourcePartition());
      if (lastSourceOffset==null) {
          lastNumber = Instant.now();
          lastIncrement = 0.0;
          //System.out.println("No offset while initializing");
      } else {
          Object lastNumberObj = lastSourceOffset.get(CURRENT_TIME_FIELD);
          Object lastIncrementObj = lastSourceOffset.get(INCREMENTING_FIELD);
          if (lastNumberObj instanceof String) {
            lastNumber = Instant.parse((String) lastNumberObj);
          }
          if (lastIncrementObj instanceof String) {
            lastIncrement = Double.valueOf((String) lastIncrementObj);
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

  private Map<String, String> sourceOffset(Instant curTime, Double offset) {
    Map<String, String> map = new HashMap<>();
    map.put(CURRENT_TIME_FIELD, curTime.toString());
    map.put(INCREMENTING_FIELD, offset.toString());
    return map;
  }


  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    log.info("Entered polling sate");
    MongoCursor<Document> cursor;
    final ArrayList<SourceRecord> records = new ArrayList<>();


    String incrementColumn = config.getIncrementColumn();

    int batchMaxRows = config.getBatchSize();
    final TableQuerier querier = tableQueue.peek();
    log.info("Getting cursor");
    if (mode.equals(INCREMENTING_FIELD)) {
      cursor = querier.getIncrementCursor(lastIncrement);
    } else {
      cursor = querier.getBatchCursor();
    }

    while (cursor.hasNext()) {
      log.info("Entered cursor loop");
      String res = cursor.next().toJson();
      JSONObject jsonObj = new JSONObject(res);
      Double qs = Double.valueOf((Double) jsonObj.get(incrementColumn));
      SourceRecord sourceRecord = generateSourceRecord(res, qs);
      records.add(sourceRecord);
      lastIncrement = qs;
      resetAndRequeueHead(querier);

    }
    cursor.close();
    TimeUnit.SECONDS.sleep(config.getPollInterval());
    System.out.println(context.offsetStorageReader().offset(sourcePartition()));
    return records;
  }

  @Override
  public void stop() {
    //TODO: Do whatever is required to stop your task.
  }

  private SourceRecord generateSourceRecord(Object issue, Double lastIncrID) {
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
            sourceOffset(Instant.now(), lastIncrID),
            config.getTopic(),
            null, // partition will be inferred by the framework
            null,
            null,
            null,
            issue.toString());
  }

  private void resetAndRequeueHead(TableQuerier expectedHead) {
    log.debug("Resetting querier {}", expectedHead.toString());
    TableQuerier removedQuerier = tableQueue.poll();
    assert removedQuerier == expectedHead;
    //expectedHead.reset(time.milliseconds());
    tableQueue.add(expectedHead);
  }
}