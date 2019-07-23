package com.orange.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.orange.kafka.MongodbSourceConnectorConfig.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class MongodbSourceConnectorConfigTest {

  private ConfigDef configDef = MongodbSourceConnectorConfig.conf();
  private Map<String, String> config;

  @Before
  public void setupInitialConfig() {
    config = new HashMap<>();
    config.put(MONGO_URI_CONFIG, "foo");
    config.put(MONGO_DB_CONFIG, "test");
    config.put(MONGO_QUERY_CONFIG, "db.test3.find()");
    config.put(BATCH_SIZE_CONFIG, "100");
    config.put(TOPIC_PREFIX_CONFIG, "test-");
    config.put(MODE_CONFIG, "bulk");
  }

  @Test
  public void doc() {
    System.out.println(MongodbSourceConnectorConfig.conf().toRst());
  }

  @Test
  public void initialConfigIsValid() {
    assertTrue(configDef.validate(config)
            .stream()
            .allMatch(configValue -> configValue.errorMessages().size() == 0));
  }

  @Test
  public void canReadConfigCorrectly() {
    MongodbSourceConnectorConfig config = new MongodbSourceConnectorConfig(this.config);
    config.getMongoDbName();
  }

  @Test
  public void validateDBname() {
    config.put(MONGO_DB_CONFIG, "dbname");
    ConfigValue configValue = configDef.validateAll(config).get(MONGO_DB_CONFIG);
    assertEquals(configValue.errorMessages().size(), 0);
  }

  @Test
  public void validateTopicPrefix() {
    config.put(TOPIC_PREFIX_CONFIG, "topic-prefix");
    ConfigValue configValue = configDef.validateAll(config).get(TOPIC_PREFIX_CONFIG);
    assertEquals(configValue.errorMessages().size(), 0);
  }

  @Test
  public void validatePollInterval() {
    config.put(POLL_INTERVAL_CONFIG, "-1");
    ConfigValue configValue = configDef.validateAll(config).get(POLL_INTERVAL_CONFIG);
    assertTrue(configValue.errorMessages().size() > 0);
  }

  @Test
  public void validateBatchSize() {
    config.put(BATCH_SIZE_CONFIG, "-1");
    ConfigValue configValue = configDef.validateAll(config).get(BATCH_SIZE_CONFIG);
    assertTrue(configValue.errorMessages().size() > 0);

//    config.put(BATCH_SIZE_CONFIG, "501");
//    configValue = configDef.validateAll(config).get(BATCH_SIZE_CONFIG);
//    assertTrue(configValue.errorMessages().size() > 0);
  }

//  @Test
//  public void validateMongoQuery() {
//    config.put(MONGO_QUERY_CONFIG, "not-valid-query");
//    ConfigValue configValue = configDef.validateAll(config).get(MONGO_QUERY_CONFIG);
//    assertTrue(configValue.errorMessages().size() > 0);
//  }

}