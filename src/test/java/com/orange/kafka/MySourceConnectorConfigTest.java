package com.orange.kafka;

import org.junit.Test;

public class MySourceConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(MongodbSourceConnectorConfig.conf().toRst());
  }
}