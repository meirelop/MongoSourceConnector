package com.orange.kafka.Validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class BatchSizeValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(String name, Object value) {
        if(! (value instanceof Integer)) {
            throw new ConfigException(name, value, "Batch size must be an integer");
        }
        Integer batchSize = (Integer) value;
        if (batchSize < 1){
            throw new ConfigException(name, value, "Batch Size must be a positive integer");
        }
    }
}