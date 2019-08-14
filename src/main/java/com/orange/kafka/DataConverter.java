package com.orange.kafka;

import com.mongodb.MongoClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.bson.*;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import com.orange.kafka.utils.EnumeratedValue;
import java.util.*;

public class DataConverter {
    public static final String SCHEMA_NAME_REGEX = "com.orange.kafka.regex";
    Schema keySchema = getKeySchema();

    public DataConverter() {
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

    public void setField(Map.Entry<String, BsonValue> keyValuesforSchema, SchemaBuilder builder){
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

            case JAVASCRIPT_WITH_SCOPE:
                SchemaBuilder jswithscope = SchemaBuilder.struct().name(builder.name() + "." + key);
                jswithscope.field("code", Schema.OPTIONAL_STRING_SCHEMA);
                SchemaBuilder scope = SchemaBuilder.struct().name(jswithscope.name() + ".scope").optional();
                BsonDocument jwsDocument = keyValuesforSchema.getValue().asJavaScriptWithScope().getScope().asDocument();

                for (Map.Entry<String, BsonValue> jwsDocumentKey : jwsDocument.entrySet()) {
                    setField(jwsDocumentKey, scope);
                }

                Schema scopeBuild = scope.build();
                jswithscope.field("scope", scopeBuild).build();
                builder.field(key, jswithscope);
                break;

            case REGULAR_EXPRESSION:
                SchemaBuilder regexwop = SchemaBuilder.struct().name(SCHEMA_NAME_REGEX).optional();
                regexwop.field("regex", Schema.OPTIONAL_STRING_SCHEMA);
                regexwop.field("options", Schema.OPTIONAL_STRING_SCHEMA);
                builder.field(key, regexwop.build());
                break;

            case DOCUMENT:
                SchemaBuilder builderDoc = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
                BsonDocument docs = keyValuesforSchema.getValue().asDocument();

                for (Map.Entry<String, BsonValue> doc : docs.entrySet()) {
                    setField(doc, builderDoc);
                }
                builder.field(key, builderDoc.build());
                break;

//            case ARRAY:
//                if (keyValuesforSchema.getValue().asArray().isEmpty()) {
//                    switch (arrayEncoding) {
//                        case ARRAY:
//                            builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
//                            break;
//                        case DOCUMENT:
//                            builder.field(key, SchemaBuilder.struct().name(builder.name() + "." + key).optional().build());
//                            break;
//                    }
//                }
//                else {
//                    switch (arrayEncoding) {
//                        case ARRAY:
//                            BsonType valueType = keyValuesforSchema.getValue().asArray().get(0).getBsonType();
//                            for (BsonValue element: keyValuesforSchema.getValue().asArray()) {
//                                if (element.getBsonType() != valueType) {
//                                    throw new ConnectException("Field " + key + " of schema " + builder.name() + " is not a homogenous array.\n"
//                                            + "Check option 'struct' of parameter 'array.encoding'");
//                                }
//                            }
//
//                            switch (valueType) {
//                                case NULL:
//                                case STRING:
//                                case JAVASCRIPT:
//                                case OBJECT_ID:
//                                case DECIMAL128:
//                                    builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
//                                    break;
//                                case DOUBLE:
//                                    builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build());
//                                    break;
//                                case BINARY:
//                                    builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_BYTES_SCHEMA).optional().build());
//                                    break;
//
//                                case INT32:
//                                case TIMESTAMP:
//                                    builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build());
//                                    break;
//
//                                case INT64:
//                                case DATE_TIME:
//                                    builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build());
//                                    break;
//
//                                case BOOLEAN:
//                                    builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_BOOLEAN_SCHEMA).optional().build());
//                                    break;
//
//                                case DOCUMENT:
//                                    final SchemaBuilder documentSchemaBuilder = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
//                                    final Map<String, BsonType> union = new HashMap<>();
//                                    for (BsonValue element: keyValuesforSchema.getValue().asArray()) {
//                                        final BsonDocument arrayDocs = element.asDocument();
//                                        for (Map.Entry<String, BsonValue> arrayDoc: arrayDocs.entrySet()) {
//                                            final BsonType prevType = union.putIfAbsent(arrayDoc.getKey(), arrayDoc.getValue().getBsonType());
//                                            if (prevType == null) {
//                                                setField(arrayDoc, documentSchemaBuilder);
//                                            }
//                                            else if (prevType != arrayDoc.getValue().getBsonType()) {
//                                                throw new ConnectException("Field " + arrayDoc.getKey() + " of schema " + documentSchemaBuilder.name() + " is not the same type for all documents in the array.\n"
//                                                        + "Check option 'struct' of parameter 'array.encoding'");
//                                            }
//                                        }
//                                    }
//
//                                    Schema build = documentSchemaBuilder.build();
//                                    builder.field(key, SchemaBuilder.array(build).optional().build());
//                                    break;
//
//                                default:
//                                    break;
//                            }
//                            break;
//                        case DOCUMENT:
//                            final BsonArray array = keyValuesforSchema.getValue().asArray();
//                            final SchemaBuilder arrayStructBuilder = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
//                            final Map<String, BsonValue> convertedArray = new HashMap<>();
//                            for (int i = 0; i < array.size(); i++) {
//                                convertedArray.put(arrayElementStructName(i), array.get(i));
//                            }
//                            convertedArray.entrySet().forEach(x -> setField(x, arrayStructBuilder));
//                            builder.field(key, arrayStructBuilder.build());
//                            break;
//                    }
//                }
//                break;
            default:
                break;
        }

    }

    public void  iterateRecord(Document record, SchemaBuilder builder) {
        BsonDocument bsonRecord = record.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        Set<Map.Entry<String, BsonValue>> pairs = bsonRecord.entrySet();
        for (Map.Entry<String, BsonValue> PairsForSchema : pairs) {
            setField(PairsForSchema, builder);
        }
    }
}

