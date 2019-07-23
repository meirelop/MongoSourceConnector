package com.orange.kafka.Validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;


public class MongoQueryValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(String name, Object value) {
        String query = (String) value;
        if (!checkQuery(query)){
            throw new ConfigException(name, value, "Mongo Query pattern is not correct");
        }
    }

    private boolean checkQuery(String mongoQuery) {
        boolean isRightPattern = mongoQuery.matches("^db\\.(.+)find([\\(])(.*)([\\)])(\\;*)$");
        if (!isRightPattern) {
            return false;
        }
        return true;
    }
}