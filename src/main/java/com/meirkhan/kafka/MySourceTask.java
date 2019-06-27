package com.meirkhan.kafka;

import com.meirkhan.kafka.model.Issue;
import com.meirkhan.kafka.model.PullRequest;
import com.meirkhan.kafka.model.User;
import com.meirkhan.kafka.utils.DateUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import static com.meirkhan.kafka.GitHubSchemas.*;

public class MySourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(MySourceTask.class);
  public MySourceConnectorConfig config;


  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    //TODO: Do things here that are required to start your task. This could be open a connection to a database, etc.
    config = new MySourceConnectorConfig(map);
  }


  private Map<String, String> sourcePartition() {
    Map<String, String> map = new HashMap<>();
    map.put("1", "2");
    map.put("3", "4");
    return map;
  }

  private Map<String, String> sourceOffset() {
    Map<String, String> map = new HashMap<>();
    map.put("5", "6");
    map.put("7", "8");
    return map;
  }


  @Override
  public List<SourceRecord> poll() throws InterruptedException {

    // fetch data
    final ArrayList<SourceRecord> records = new ArrayList<>();


    String temp []=new String[4];
    temp[0]="hello";
    temp[1]="world";
    temp[2]="I am";
    temp[3]="Meirkhajn";

    for (int i=0; i < temp.length - 1; i++) {
      SourceRecord sourceRecord = generateSourceRecord(temp[i]);
      records.add(sourceRecord);
    }
    return records;
  }

  @Override
  public void stop() {
    //TODO: Do whatever is required to stop your task.
  }

  private SourceRecord generateSourceRecord(String issue) {
    return new SourceRecord(
            sourcePartition(),
            sourceOffset(),
            config.getTopic(),
            null, // partition will be inferred by the framework
            null,
            null,
            null,
            issue);

  }
}