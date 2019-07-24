package com.orange.kafka.Validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class PollIntervalValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(String name, Object value) {
        if(! (value instanceof Integer)) {
            throw new ConfigException(name, value, "Poll interval must be an integer");
        }
        Integer pollInterval = (Integer) value;
        if (pollInterval < 1){
            throw new ConfigException(name, value, "Poll Interval  must be a positive integer");
        }
    }
}