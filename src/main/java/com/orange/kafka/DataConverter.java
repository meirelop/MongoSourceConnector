package com.orange.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.apache.kafka.connect.data.Timestamp;
import java.util.*;

public class DataConverter {

    public DataConverter() {
    }

    public void addFieldSchema (Document record, SchemaBuilder builder){
        JSONObject jsonObject = new JSONObject(record.toJson());
        Set<String> keys = jsonObject.keySet();

        for (String key: keys) {
            Object value = record.get(key);

            if(value instanceof String) {
                builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
            }else if(value instanceof Integer) {
                builder.field(key, Schema.OPTIONAL_INT32_SCHEMA);
            }else if(value instanceof Double) {
                builder.field(key, Schema.OPTIONAL_FLOAT64_SCHEMA);
            }else if(value instanceof Boolean) {
                builder.field(key, Schema.OPTIONAL_BOOLEAN_SCHEMA);
            }else if(value instanceof java.util.Date) {
                builder.field(key, Schema.OPTIONAL_INT32_SCHEMA);
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
            }else if(value instanceof Double) {
                struct.put(key, (Double) value);
            }else if(value instanceof Float) {
                struct.put(key, (Float) value);
            }else if(value instanceof Boolean) {
                struct.put(key, (boolean) value);
            }else if(value instanceof java.util.Date) {
                struct.put(key, (int) ((java.util.Date) value).getTime());
            }else if(value instanceof Timestamp) {
                struct.put(key, (Timestamp) value);
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
}