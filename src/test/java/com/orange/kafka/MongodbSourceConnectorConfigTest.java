package com.orange.kafka;

import org.junit.Test;

public class MongodbSourceConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(MongodbSourceConnectorConfig.conf().toRst());
  }
}