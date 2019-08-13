package com.orange.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Map;
import java.util.Set;

public class ExtractNewDocumentState<R extends ConnectRecord<R>> implements Transformation<R> {
    private DataConverter converter;

    @Override
    public R apply(R record) {
        BsonDocument valueDocument = new BsonDocument();
        return newRecord(record, valueDocument);
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        return config;
    }

    private R newRecord(R record, BsonDocument valueDocument) {
        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
        Set<Map.Entry<String, BsonValue>> keyPairs = valueDocument.entrySet();
        for (Map.Entry<String, BsonValue> keyPairsForSchema : keyPairs) {
//            converter.addFieldSchema(keySchemaBuilder);
        }

        Schema finalKeySchema = keySchemaBuilder.build();
        Struct finalKeyStruct = new Struct(finalKeySchema);

        R newRecord = record.newRecord(record.topic(), record.kafkaPartition(), finalKeySchema,
                finalKeyStruct, finalKeySchema, finalKeyStruct, record.timestamp());
        return newRecord;
    }

    @Override
    public void configure(final Map<String, ?> map) {

    }
}
