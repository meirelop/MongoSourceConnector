package com.orange.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Kafka Connect source connector that creates {@link MongodbSourceTask tasks} that replicate the context of one or more
 * MongoDB replica sets.
 *
 * <h2>Sharded Clusters</h2>
 * This connector is able to fully replicate the content of one MongoDB sharded cluster.
 * In this case, simply configure the connector with the host addresses of the configuration replica set.
 * When the connector starts, it will discover and replicate the replica set for each shard.
 *
 * <h2>Replica Set</h2>
 * The connector is able to fully replicate the content of one MongoDB replica set.
 * In this case, simply configure the connector
 * with the host addresses of the replica set. When the connector starts, it will discover the primary node and use it to
 * replicate the contents of the replica set.
 * <p>
 *
 * <h2>Use of Topics</h2>
 * The connector will write to a single topic all of the source records that correspond to a single collection. The topic will
 * be named "{@code <topic.prefix>}", where {@code <topic.prefix>} is set via the
 * "{@link MongodbSourceConnectorConfig#getTopicPrefix()}" configuration property.
 *
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link MongodbSourceConnectorConfig}.
 *
 * @author Meirkhan Rakhmetzhanov
 */
public class MongodbSourceConnector extends SourceConnector {
  private static Logger log = LoggerFactory.getLogger(MongodbSourceConnector.class);
  private MongodbSourceConnectorConfig config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    config = new MongodbSourceConnectorConfig(map);

    //TODO: Add things you need to do to setup your connector.
  }

  @Override
  public Class<? extends Task> taskClass() {
    //TODO: Return your task implementation.
    return MongodbSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    //TODO: Define the individual task configurations that will be executed.
    ArrayList<Map<String, String>> configs = new ArrayList<>(1);
    configs.add(config.originalsStrings());
    return configs;

    //throw new UnsupportedOperationException("This has not been implemented.");
  }

  @Override
  public void stop() {
    //TODO: Do things that are necessary to stop your connector.
  }

  @Override
  public ConfigDef config() {
    return MongodbSourceConnectorConfig.conf();
  }
}
