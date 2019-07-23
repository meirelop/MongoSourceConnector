package com.orange.kafka;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.orange.kafka.MongodbSourceConnectorConfig.*;
import static org.junit.Assert.assertEquals;

public class MongodbSourceConnectorTest {
  @Test
  public void test() {
    // Congrats on a passing test!
  }

  private Map<String, String> initialConfig() {
    Map<String, String> baseProps = new HashMap<>();
    baseProps.put(MONGO_URI_CONFIG, "mongodb://localhost:27017");
    baseProps.put(MONGO_DB_CONFIG, "test");
    baseProps.put(MONGO_QUERY_CONFIG, "db.test3.find()");
    baseProps.put(BATCH_SIZE_CONFIG, "100");
    baseProps.put(TOPIC_PREFIX_CONFIG, "test-");
    baseProps.put(MODE_CONFIG, "bulk");
    return baseProps;
  }


  @Test
  public void taskConfigsShouldReturnOneTaskConfig() {
    MongodbSourceConnector mongoSourceConnector = new MongodbSourceConnector();
    mongoSourceConnector.start(initialConfig());
    assertEquals(mongoSourceConnector.taskConfigs(1).size(),1);
    assertEquals(mongoSourceConnector.taskConfigs(10).size(),1);
  }
}
