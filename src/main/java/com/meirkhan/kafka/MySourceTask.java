package com.meirkhan.kafka;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.meirkhan.kafka.utils.DateUtils;
import java.util.PriorityQueue;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.time.Instant;
import java.time.LocalDateTime;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import static com.meirkhan.kafka.MySchemas.*;

public class MySourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(MySourceTask.class);
  public MySourceConnectorConfig config;
  protected Instant lastDate;
  protected Double lastIncrement;
  private PriorityQueue<TableQuerier> tableQueue = new PriorityQueue<TableQuerier>();

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    log.debug("Starting MongoDB source task");
    try {
      config = new MySourceConnectorConfig(map);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start MongoDBSourceTask due to configuration error", e);
    }

    String mode = config.getModeName();
    String topic = config.getTopicPrefix() + config.getMongoCollectionName();


    if(mode.equals(MySourceConnectorConfig.MODE_BULK)) {
      log.info("Creating BatchQuerier instance");
      tableQueue.add(
              new BulkCollectionQuerier(
                      topic,
                      config.getMongoHost(),
                      config.getMongoPort(),
                      config.getMongoDbName(),
                      config.getMongoCollectionName()
              )
      );
    } else if(mode.equals(MySourceConnectorConfig.MODE_INCREMENTING)) {
      log.info("Creating IncrementQuerier instance");
      initializeLastVariables();
      tableQueue.add(
              new IncrementQuerier(
                      topic,
                      config.getMongoHost(),
                      config.getMongoPort(),
                      config.getMongoDbName(),
                      config.getMongoCollectionName(),
                      config.getIncrementColumn(),
                      lastIncrement
              )
      );
    } else if(mode.equals(MySourceConnectorConfig.MODE_TIMESTAMP)) {
      log.info("Creating IncrementQuerier instance");
      initializeLastVariables();
      tableQueue.add(
              new TimestampQuerier(
                      topic,
                      config.getMongoHost(),
                      config.getMongoPort(),
                      config.getMongoDbName(),
                      config.getMongoCollectionName(),
                      config.getTimestampColumn(),
                      lastDate
              )
      );
    } else if(mode.equals(MySourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING)) {
      log.info("Creating TimestampIncrementQuerier instance");
      initializeLastVariables();
      tableQueue.add(
              new TimestampIncrementQuerier(
                      topic,
                      config.getMongoHost(),
                      config.getMongoPort(),
                      config.getMongoDbName(),
                      config.getMongoCollectionName(),
                      config.getTimestampColumn(),
                      lastDate,
                      config.getIncrementColumn(),
                      lastIncrement
              )
      );
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
          lastDate = Instant.parse((String) lastDateObj);
        }
      }
  }

  private Map<String, String> sourcePartition() {
    Map<String, String> map = new HashMap<>();
    map.put(DATABASE_NAME_FIELD, config.getMongoDbName());
    map.put(COLLECTION_FIELD, config.getMongoCollectionName());
    return map;
  }


  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    TimeUnit.SECONDS.sleep(config.getPollInterval());
    final ArrayList<SourceRecord> results = new ArrayList<>();
    int batchMaxRows = config.getBatchSize();
    final TableQuerier querier = tableQueue.peek();

    if(querier != null) {
      querier.executeCursor();
      while (results.size() < batchMaxRows && querier.hasNext()) {
        SourceRecord record = querier.extractRecord();
        results.add(record);
        resetAndRequeueHead(querier);
      }
    }

    if(querier != null) {
      querier.closeCursor();
    }

    return results;
  }

  @Override
  public void stop() {
    //TODO: Do whatever is required to stop your task.
  }


  private void resetAndRequeueHead(TableQuerier expectedHead) {
    log.debug("Resetting querier {}", expectedHead.toString());
    TableQuerier removedQuerier = tableQueue.poll();
    assert removedQuerier == expectedHead;
    //expectedHead.reset(time.milliseconds());
    tableQueue.add(expectedHead);
  }
}