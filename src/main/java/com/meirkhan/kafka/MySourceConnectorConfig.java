package com.meirkhan.kafka;

import com.meirkhan.kafka.Validators.BatchSizeValidator;
import com.meirkhan.kafka.Validators.TimestampValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySourceConnectorConfig extends AbstractConfig {
  static final Logger log = LoggerFactory.getLogger(MySourceConnectorConfig.class);

  public static final String TOPIC_CONFIG = "topic";
  private static final String TOPIC_DOC = "Topic to write to";

  public static final String BATCH_SIZE_CONFIG = "batch.size";
  private static final String BATCH_SIZE_DOC = "Number of data points to retrieve at a time. Defaults to 100 (max value)";

  public static final String MONGO_HOST_CONFIG = "mongo.host";
  private static final String MONGO_HOST_CONFIG_DOC = "MongoDB connection host";

  public static final String MONGO_PORT_CONFIG = "mongo.port";
  private static final String MONGO_PORT_CONFIG_DOC = "MongoDB connection port";

  public static final String MONGO_DB_CONFIG = "mongo.db";
  private static final String MONGO_DB_CONFIG_DOC = "MongoDB database from which to query";

  public static final String MONGO_QUERY_CONFIG = "mongo.query";
  private static final String MONGO_QUERY_CONFIG_DOC = "MongoDB query to database";

  public static final String POLL_INTERVAL_CONFIG = "poll.interval.sec";
  private static final String POLL_INTERVAL_CONFIG_DOC = "Polling delay in seconds";

  public static final String MODE_CONFIG = "mode";
  private static final String MODE_CONFIG_DOC =
            "The mode for updating a table each time it is polled. Options include:\n"
                    + "  * bulk - perform a bulk load of the entire table each time it is polled\n"
                    + "  * incrementing - use a strictly incrementing column on each table to "
                    + "detect only new rows. Note that this will not detect modifications or "
                    + "deletions of existing rows.\n"
                    + "  * timestamp - use a timestamp (or timestamp-like) column to detect new and modified "
                    + "rows. This assumes the column is updated with each write, and that values are "
                    + "monotonically incrementing, but not necessarily unique.\n"
                    + "  * timestamp+incrementing - use two columns, a timestamp column that detects new and "
                    + "modified rows and a strictly incrementing column which provides a globally unique ID for "
                    + "updates so each row can be assigned a unique stream offset.";

  public static final String MODE_UNSPECIFIED = "";
  public static final String MODE_BULK = "bulk";
  public static final String MODE_TIMESTAMP = "timestamp";
  public static final String MODE_INCREMENTING = "incrementing";
  public static final String MODE_TIMESTAMP_INCREMENTING = "timestamp+incrementing";

  public static final String INCREMENTING_COLUMN_NAME_CONFIG = "incrementing.column.name";
  private static final String INCREMENTING_COLUMN_NAME_DOC =
            "The name of the strictly incrementing column to use to detect new rows. Any empty value "
                    + "indicates the column should be autodetected by looking for an auto-incrementing column. "
                    + "This column may not be nullable.";
  public static final String INCREMENTING_COLUMN_NAME_DEFAULT = "";
  private static final String INCREMENTING_COLUMN_NAME_DISPLAY = "Incrementing Column Name";

  public static final String TIMESTAMP_COLUMN_NAME_CONFIG = "timestamp.column.name";
  private static final String TIMESTAMP_COLUMN_NAME_DOC =
            "Comma separated list of one or more timestamp columns to detect new or modified rows using "
                    + "the COALESCE SQL function. Rows whose first non-null timestamp value is greater than the "
                    + "largest previous timestamp value seen will be discovered with each poll. At least one "
                    + "column should not be nullable.";
  public static final String TIMESTAMP_COLUMN_NAME_DEFAULT = "";
  private static final String TIMESTAMP_COLUMN_NAME_DISPLAY = "Timestamp Column Name";



  public MySourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public MySourceConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
            .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, TOPIC_DOC)
            .define(BATCH_SIZE_CONFIG, Type.INT, 5, new BatchSizeValidator(), Importance.LOW, BATCH_SIZE_DOC)
            .define(MONGO_HOST_CONFIG, Type.STRING, "localhost", Importance.HIGH, MONGO_HOST_CONFIG_DOC)
            .define(MONGO_PORT_CONFIG, Type.INT, 27017, Importance.HIGH, MONGO_PORT_CONFIG_DOC)
            .define(MONGO_DB_CONFIG, Type.STRING, Importance.HIGH, MONGO_DB_CONFIG_DOC)
            .define(MONGO_QUERY_CONFIG, Type.STRING, Importance.HIGH, MONGO_QUERY_CONFIG_DOC)
            .define(POLL_INTERVAL_CONFIG, Type.INT, 1, Importance.HIGH, POLL_INTERVAL_CONFIG_DOC)
            .define(MODE_CONFIG, Type.STRING, Importance.MEDIUM, MODE_CONFIG_DOC)
            .define(INCREMENTING_COLUMN_NAME_CONFIG, Type.STRING, Importance.MEDIUM, INCREMENTING_COLUMN_NAME_DOC)
            .define(TIMESTAMP_COLUMN_NAME_CONFIG, Type.STRING, Importance.MEDIUM, TIMESTAMP_COLUMN_NAME_DOC);
  }

  public int getBatchSize() {
    return this.getInt(BATCH_SIZE_CONFIG);
  }

  public String getTopic() {
    return this.getString(TOPIC_CONFIG);
  }

  public String getMongoHost() {return this.getString(MONGO_HOST_CONFIG);}

  public Integer getMongoPort() {return this.getInt(MONGO_PORT_CONFIG);}

  public String getMongoDbName() {return this.getString(MONGO_DB_CONFIG);}

  public String getMongoQuery() {
    checkQuery();
    return this.getString(MONGO_QUERY_CONFIG);
  }

  public String getMongoCollectionName() {
    return StringUtils.substringBetween(this.getMongoQuery(), "db.", ".find");
  }

  public String getMongoQueryFilters() {
    String query = this.getMongoQuery();
    return query.substring(query.indexOf('(') + 1, query.lastIndexOf(')'));
  }

  public Integer getPollInterval() {return this.getInt(POLL_INTERVAL_CONFIG);}

  public String getModeName() {return this.getString(MODE_CONFIG);}

  public String getIncrementColumn() {return this.getString(INCREMENTING_COLUMN_NAME_CONFIG);}

  public String getTimestampColumn() {return this.getString(TIMESTAMP_COLUMN_NAME_CONFIG);}

  public void checkQuery() {
    // TODO: define what to do if query syntax is not correct
    boolean isRightPattern = this.getString(MONGO_QUERY_CONFIG).matches("^db\\.(.+)find([\\(])(.*)([\\)])(\\;*)$");
    if (!isRightPattern) {
      log.error("Query syntax to MongoDB is not correct");
      //System.exit(0);
    }
  }
}
