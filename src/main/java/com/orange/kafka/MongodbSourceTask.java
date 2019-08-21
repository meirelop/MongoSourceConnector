package com.orange.kafka;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.PriorityQueue;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.time.Instant;
import java.time.LocalDateTime;

/**
 * A Kafka Connect source task that copies the data from single MongoDB instance or MongoDB replica sets.
 *
 * @see MongodbSourceConnector
 * @see MongodbSourceConnectorConfig
 * @author Meirkhan Rakhmetzhanov
 */
public class MongodbSourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(MongodbSourceTask.class);
  public MongodbSourceConnectorConfig config;
  protected Instant lastDate;
  protected Double lastIncrement;
  private PriorityQueue<TableQuerier> tableQueue = new PriorityQueue<TableQuerier>();

  public enum ArrayEncoding {
    ARRAY("array"),
    DOCUMENT("document");

    private final String name;
    ArrayEncoding(String name) { this.name = name; }
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }


  @Override
  public void start(Map<String, String> map) {
    log.debug("Starting MongoDB source task");
    try {
      config = new MongodbSourceConnectorConfig(map);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start MongoDBSourceTask due to configuration error", e);
    }

    String mode = config.getModeName();
    String topic = config.getTopicPrefix();

    ArrayEncoding arrayEncoding;
    if (config.getArrayEncoding().equals("array")) {
      arrayEncoding = ArrayEncoding.ARRAY;
    } else {
      arrayEncoding = ArrayEncoding.DOCUMENT;
    }

    if(mode.equals(MongodbSourceConnectorConfig.MODE_BULK)) {
      log.info("Creating BatchQuerier instance");
      tableQueue.add(
              new BulkCollectionQuerier(
                      arrayEncoding,
                      topic,
                      config.getMongoUri(),
                      config.getMongoDbName(),
                      config.getMongoCollectionName(),
                      config.getIncludeFields(),
                      config.getExcludeFields())
      );
    } else if(mode.equals(MongodbSourceConnectorConfig.MODE_INCREMENTING)) {
      log.info("Creating IncrementQuerier instance");
      initializeLastVariables();
      tableQueue.add(
              new IncrementQuerier(
                      arrayEncoding,
                      topic,
                      config.getMongoUri(),
                      config.getMongoDbName(),
                      config.getMongoCollectionName(),
                      config.getIncludeFields(),
                      config.getExcludeFields(),
                      config.getIncrementColumn(),
                      lastIncrement
              )
      );
    } else if(mode.equals(MongodbSourceConnectorConfig.MODE_TIMESTAMP)) {
      log.info("Creating IncrementQuerier instance");
      initializeLastVariables();
      tableQueue.add(
              new TimestampQuerier(
                      arrayEncoding,
                      topic,
                      config.getMongoUri(),
                      config.getMongoDbName(),
                      config.getMongoCollectionName(),
                      config.getIncludeFields(),
                      config.getExcludeFields(),
                      config.getTimestampColumn(),
                      lastDate
              )
      );
    } else if(mode.equals(MongodbSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING)) {
      log.info("Creating TimestampIncrementQuerier instance");
      initializeLastVariables();
      tableQueue.add(
              new TimestampIncrementQuerier(
                      arrayEncoding,
                      topic,
                      config.getMongoUri(),
                      config.getMongoDbName(),
                      config.getMongoCollectionName(),
                      config.getIncludeFields(),
                      config.getExcludeFields(),
                      config.getTimestampColumn(),
                      lastDate,
                      config.getIncrementColumn(),
                      lastIncrement
              )
      );
    }
    log.info("Successfully started MongoDB connector task");
  }

  private void initializeLastVariables() {
    log.debug("Source offsets initializing");
      Map<String, Object> lastSourceOffset;
      lastSourceOffset = context.offsetStorageReader().offset(sourcePartition());

      if (lastSourceOffset==null) {
        lastIncrement = 0.0;
      } else {
        Object lastIncrementObj = lastSourceOffset.get(Constants.INCREMENTING_FIELD);
        if (lastIncrementObj != null && lastIncrementObj instanceof String) {
          lastIncrement = Double.valueOf((String) lastIncrementObj);
        }
      }

      if(lastDate==null) {
        lastDate = (Instant.ofEpochMilli(1));
      } else {
        Object lastDateObj = lastSourceOffset.get(Constants.LAST_TIME_FIELD);
        if(lastDateObj != null && lastDateObj instanceof LocalDateTime) {
          lastDate = Instant.parse((String) lastDateObj);
        }
      }
  }

  private Map<String, String> sourcePartition() {
    Map<String, String> map = new HashMap<>();
    map.put(Constants.DATABASE_NAME_FIELD, config.getMongoDbName());
    map.put(Constants.COLLECTION_FIELD, config.getMongoCollectionName());
    return map;
  }


  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    TimeUnit.SECONDS.sleep(config.getPollInterval());
    final ArrayList<SourceRecord> results = new ArrayList<>();
    int batchMaxRows = config.getBatchSize();
    final TableQuerier querier = tableQueue.peek();

    int i = 0;
    if(querier != null) {
      querier.executeCursor();
      while (querier.hasNext()) {
        SourceRecord record = querier.extractRecord();
        results.add(record);
        i += 1;
        if(!querier.hasNext()) {
          resetAndRequeueHead(querier);
        }
      }
      if (i > 0) log.info(String.format("Fetched %s record(s)", i));
    }
    if(querier != null) {
      resetAndRequeueHead(querier);
      querier.closeCursor();
    }
    return results;
  }

  @Override
  public void stop() {
    log.info("Stopping MongoDB Source Connector task");
  }


  private void resetAndRequeueHead(TableQuerier expectedHead) {
    log.debug("Resetting querier {}", expectedHead.toString());
    TableQuerier removedQuerier = tableQueue.poll();
    assert removedQuerier == expectedHead;
    //expectedHead.reset(time.milliseconds());
    tableQueue.add(expectedHead);
  }
}