package com.orange.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.apache.kafka.connect.data.Timestamp;
import java.util.*;

public class DataConverter {
    Schema keySchema = getKeySchema();

    public DataConverter() {
        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
        keySchemaBuilder.field("_id", Schema.OPTIONAL_STRING_SCHEMA);
        Schema keySchema = keySchemaBuilder.build();
    }

    public void addFieldSchema (Document record, SchemaBuilder builder){
        JSONObject jsonObject = new JSONObject(record.toJson());
        Set<String> keys = jsonObject.keySet();

        for (String key: keys) {
            Object value = record.get(key);

            if(value instanceof String) {
                builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
            }else if(value instanceof Integer
                    || value instanceof java.util.Date
                    || value instanceof BsonTimestamp) {
                builder.field(key, Schema.OPTIONAL_INT32_SCHEMA);
            }else if(value instanceof Double) {
                builder.field(key, Schema.OPTIONAL_FLOAT64_SCHEMA);
            }else if(value instanceof Long) {
                builder.field(key, Schema.OPTIONAL_INT64_SCHEMA);
            }else if(value instanceof Boolean) {
                builder.field(key, Schema.OPTIONAL_BOOLEAN_SCHEMA);
            }else if(value instanceof ObjectId) {
                builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
            }else if(value instanceof Document) {
                SchemaBuilder builderDoc = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
                addFieldSchema((Document) value, builderDoc);
                builder.field(key, builderDoc.build());
            }
            else {
                builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
            }
        }
    }

    public Struct setFieldStruct(Document record, Schema schema) {
        JSONObject jsonObject = new JSONObject(record.toJson());
        Set<String> keys = jsonObject.keySet();
        Struct struct = new Struct(schema);

        for(String key: keys) {
            Object value = record.get(key);
            if(value instanceof String) {
                struct.put(key, (String) value);
            }else if(value instanceof Integer) {
                struct.put(key, (int) value);
            }else if(value instanceof Double ) {
                struct.put(key, (Double) value);
            }else if(value instanceof Long) {
                struct.put(key, (Long) value);
            }else if(value instanceof Float) {
                struct.put(key, (Float) value);
            }else if(value instanceof Boolean) {
                struct.put(key, (boolean) value);
            }else if(value instanceof java.util.Date) {
                struct.put(key, (int) ((java.util.Date) value).getTime()/1000);
            }else if(value instanceof org.bson.BsonTimestamp) {
                struct.put(key, ((BsonTimestamp) value).getTime());
            }else if(value instanceof ObjectId) {
                struct.put(key, (String) value.toString());
            }else if(value instanceof Document) {
                Schema subSchema = schema.field(key).schema();
                Struct subStruct = setFieldStruct((Document) value, subSchema);
                struct.put(key, subStruct);
            }
            else {
                struct.put(key, (String) value.toString());
            }
        }

        return struct;
    }

    public Schema getKeySchema() {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        schemaBuilder.field("_id", Schema.OPTIONAL_STRING_SCHEMA);
        return schemaBuilder.build();
    }

    public Struct getKeyStruct(ObjectId objectID) {
        Struct struct = new Struct(keySchema);
        struct.put("_id", objectID.toString());
        return struct;
    }
}

