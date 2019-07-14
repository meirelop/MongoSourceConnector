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
import com.meirkhan.kafka.utils.DateUtils;


import java.text.ParseException;
import java.util.PriorityQueue;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.time.Instant;
import java.time.LocalDateTime;
import java.text.SimpleDateFormat;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import static com.meirkhan.kafka.MySchemas.*;

public class MySourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(MySourceTask.class);
  public MySourceConnectorConfig config;
  protected Instant lastDate;
  protected Double lastIncrement;
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

    if (mode.equals(INCREMENTING_FIELD)) {
      log.info("Creating IncrementQuerier instance");
      tableQueue.add(
              new IncrementQuerier(
                      config.getMongoHost(),
                      config.getMongoPort(),
                      config.getMongoDbName(),
                      config.getMongoCollectionName(),
                      config.getIncrementColumn()
              )
      );
      initializeLastVariables();
    } else if(mode.equals(BATCH_FIELD)) {
      log.info("Creating BatchQuerier instance");
      tableQueue.add(
              new IncrementQuerier(
                      config.getMongoHost(),
                      config.getMongoPort(),
                      config.getMongoDbName(),
                      config.getMongoCollectionName()
              )
      );
    } else if(mode.equals(TIMESTAMP_FIELD)) {
      log.info("Creating Timestamp instance");
      tableQueue.add(
              new IncrementQuerier(
                      config.getMongoHost(),
                      config.getMongoPort(),
                      config.getMongoDbName(),
                      config.getMongoCollectionName(),
                      config.getTimestampColumn()
              )
      );
      initializeLastVariables();
    }
  }

  private void initializeLastVariables() {
      Map<String, Object> lastSourceOffset;
      lastSourceOffset = context.offsetStorageReader().offset(sourcePartition());
      if (lastSourceOffset==null) {
        lastIncrement = 0.0;
      } else {
        Object lastIncrementObj = lastSourceOffset.get(INCREMENTING_FIELD);
        if (lastIncrementObj != null && lastIncrementObj instanceof String) {
          lastIncrement = Double.valueOf((String) lastIncrementObj);
        }
      }

      if(lastDate==null) {
        lastDate = (Instant.ofEpochMilli(1));
      } else {
        Object lastDateObj = lastSourceOffset.get(LAST_TIME_FIELD);
        if(lastDateObj != null && lastDateObj instanceof LocalDateTime) {
//          lastDate = (String) lastDateObj;
          lastDate = Instant.parse((String) lastDateObj);
        }
      }

//      } else {
//          Object lastNumberObj = lastSourceOffset.get(CURRENT_TIME_FIELD);
//          Object lastIncrementObj = lastSourceOffset.get(INCREMENTING_FIELD);
//          if (lastNumberObj instanceof String) {
////            curDate = Instant.parse((String) lastNumberObj);
//          }
//          if (lastIncrementObj instanceof String) {
//            lastIncrement = Double.valueOf((String) lastIncrementObj);
//          }
          //System.out.printf("Offset in initializing: %s", lastNumberObj.toString());
//      }
  }

  private Map<String, String> sourcePartition() {
    Map<String, String> map = new HashMap<>();
    map.put(DATABASE_NAME_FIELD, this.config.getMongoDbName());
    map.put(COLLECTION_FIELD, this.config.getMongoCollectionName());
    return map;
  }

  private Map<String, String> sourceOffset(Instant record_time, Double offset) {
    Map<String, String> map = new HashMap<>();
    String maxDate = DateUtils.MaxInstant(record_time, lastDate).toString();
    map.put(LAST_TIME_FIELD, maxDate);
    System.out.printf("MAAXX DATE: %s", maxDate);
//    map.put(LAST_TIME_FIELD, curTime.toString());
    map.put(INCREMENTING_FIELD, offset.toString());
    return map;
  }


  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    MongoCursor<Document> cursor;
    final ArrayList<SourceRecord> records = new ArrayList<>();
    String incrementColumn = config.getIncrementColumn();
    String timestampColumn = config.getTimestampColumn();

    int batchMaxRows = config.getBatchSize();

    final TableQuerier querier = tableQueue.peek();
    cursor = querier.getBatchCursor();
    if (mode.equals(INCREMENTING_FIELD)) {
      cursor = querier.getIncrementCursor(lastIncrement);
    } else if (mode.equals(TIMESTAMP_FIELD)){
        System.out.printf("LLAAAASSST DATE: %s",lastDate.toString());
        cursor = querier.getTimestampCursor(lastDate);
    }

    while (cursor.hasNext()) {
      log.info("Entered cursor loop");
      Document res = cursor.next();
      // TODO: there is no incrementcolumn in case of Batch
//      JSONObject jsonObj = new JSONObject(res);
//      Double qs = Double.valueOf((Double) jsonObj.get(incrementColumn));
      Instant record_time = res.getDate(timestampColumn).toInstant();
      System.out.printf("DDDATE FROM MONGOOODB: %s", record_time.toString());
      Double record_id = res.getDouble(incrementColumn);
      SourceRecord sourceRecord = generateSourceRecord(res, record_id, record_time);
      records.add(sourceRecord);
      lastIncrement = record_id;
      lastDate = DateUtils.MaxInstant(record_time, lastDate);
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

  private SourceRecord generateSourceRecord(Object record, Double lastIncrID, Instant record_time) {
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
            sourceOffset(record_time, lastIncrID),
            config.getTopic(),
            null, // partition will be inferred by the framework
            null,
            null,
            null,
            record.toString());
  }

  private void resetAndRequeueHead(TableQuerier expectedHead) {
    log.debug("Resetting querier {}", expectedHead.toString());
    TableQuerier removedQuerier = tableQueue.poll();
    assert removedQuerier == expectedHead;
    //expectedHead.reset(time.milliseconds());
    tableQueue.add(expectedHead);
  }
}