package com.orange.kafka;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Date;

import javax.print.Doc;
import java.util.*;


public class DataConverter {

    public static final String SCHEMA_NAME = "com.orange.kafka.schema";
//    public SchemaBuilder builder = SchemaBuilder.struct().name("builder");
    Schema subSchema;
    Schema schema;

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

    public Schema getSchema(Document record, SchemaBuilder builder){
        JSONObject jsonObject = new JSONObject(record.toJson());
        Set<String> keys = jsonObject.keySet();

        for (String key: keys) {
            Object value = record.get(key);
            System.out.println(key);
            System.out.println(value.getClass());

            if(value instanceof String) {
                builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
            }else if(value instanceof Integer) {
                builder.field(key, Schema.OPTIONAL_INT32_SCHEMA);
            }else if(value instanceof Double) {
                builder.field(key, Schema.OPTIONAL_FLOAT64_SCHEMA);
            }else if(value instanceof Boolean) {
                builder.field(key, Schema.OPTIONAL_BOOLEAN_SCHEMA);
            }else if(value instanceof Date) {
                System.out.println("DAAAATEEEeeeeeeeeEE");
                builder.field(key, Date.SCHEMA);
            }else if(value instanceof Timestamp) {
                System.out.println("TIMESTAAAAAAAAAAAAAAAMP");
                builder.field(key, Timestamp.SCHEMA);
            }else if(value instanceof ObjectId) {
                builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
            }else if(value instanceof Document) {
                builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
//                SchemaBuilder builderDoc = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
//                getSchema((Document) value, builderDoc);
//                builder.field(key, builderDoc.build());
            }
            else {
                builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
            }
        }
        schema = builder.build();
        System.out.println(schema.fields());
        return schema;
    }

    public Struct getStruct(Document record, Schema schema) {
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
            }else if(value instanceof Date) {
                struct.put(key, (Date) value);
            }else if(value instanceof Timestamp) {
                struct.put(key, (Timestamp) value);
            }else if(value instanceof ObjectId) {
                struct.put(key, (String) value.toString());
            }else if(value instanceof Document) {
//                getStruct((Document) value, subSchema);
                struct.put(key, (String) value.toString());
//                struct.put(key, (Document) value);
            }
            else {
                struct.put(key, (String) value.toString());
            }
        }

        return struct;
    }
}