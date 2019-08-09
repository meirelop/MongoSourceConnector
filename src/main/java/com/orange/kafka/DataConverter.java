package com.orange.kafka;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;


public class DataConverter {

    public static final String SCHEMA_NAME_REGEX = "io.debezium.mongodb.regex";

    public Struct convertRecord(Map.Entry<String, BsonValue> keyvalueforStruct, Schema schema, Struct struct) {
        convertFieldValue(keyvalueforStruct, struct, schema);
        return struct;
    }

    public void convertFieldValue(Map.Entry<String, BsonValue> keyvalueforStruct, Struct struct, Schema schema) {
        Object colValue = null;
        String key = keyvalueforStruct.getKey();
        BsonType type = keyvalueforStruct.getValue().getBsonType();

        switch (type) {
            case NULL:
                colValue = null;
                break;
            case STRING:
                colValue = keyvalueforStruct.getValue().asString().getValue().toString();
                break;
            case INT32:
                colValue = keyvalueforStruct.getValue().asInt32().getValue();
                break;
            case INT64:
                colValue = keyvalueforStruct.getValue().asInt64().getValue();
                break;
            case DECIMAL128:
                colValue = keyvalueforStruct.getValue().asDecimal128().getValue().toString();
                break;
            default:
                return;
        }
        struct.put(key, keyvalueforStruct.getValue().isNull() ? null : colValue);
    }

    public void addFieldSchema(Map.Entry<String, BsonValue> keyValuesforSchema, SchemaBuilder builder) {
        String key = keyValuesforSchema.getKey();
        BsonType type = keyValuesforSchema.getValue().getBsonType();

        switch (type) {

            case NULL:
            case STRING:
            case JAVASCRIPT:
            case OBJECT_ID:
            case DECIMAL128:
                builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
                break;

            case DOUBLE:
                builder.field(key, Schema.OPTIONAL_FLOAT64_SCHEMA);
                break;

            case BINARY:
                builder.field(key, Schema.OPTIONAL_BYTES_SCHEMA);
                break;

            case INT32:
            case TIMESTAMP:
                builder.field(key, Schema.OPTIONAL_INT32_SCHEMA);
                break;

            case INT64:
            case DATE_TIME:
                builder.field(key, Schema.OPTIONAL_INT64_SCHEMA);
                break;

            case BOOLEAN:
                builder.field(key, Schema.OPTIONAL_BOOLEAN_SCHEMA);
                break;
            default:
                break;
        }
    }
}