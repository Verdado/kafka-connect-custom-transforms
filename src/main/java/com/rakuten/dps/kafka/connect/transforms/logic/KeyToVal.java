package com.rakuten.dps.kafka.connect.transforms.logic;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.List;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class KeyToVal<R extends ConnectRecord<R>> {
    public KeyToVal() {
    }

    public static Struct getValueStruct(Object recordValue) { return requireStruct(recordValue, "Extract Value record"); }

    public static Struct getKeyStruct(Object recordKey) { return requireStruct(recordKey, "Extract key record"); }

    public static Struct getNewValues(Schema newValueSchema, List<Field> keySchema, Schema valueSchema, Struct values, Struct key) {
        Struct newValues = new Struct(newValueSchema);
        for (Field field: valueSchema.fields()) {
            newValues.put(field.name(), values.get(field));
        }
        for (Field field: keySchema) {
            newValues.put(field.name(), key.get(field.name()));
        }
        return newValues;
    }

    public static Schema getNewValuesSchema(List<Field> valuesFields, List<Field> KeySchema) {
        SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct();
        for (Field field : valuesFields) {
            valueSchemaBuilder.field(field.name(), field.schema());
        }
        for (Field field : KeySchema) {
            valueSchemaBuilder.field(field.name(), field.schema());
        }
        return valueSchemaBuilder.build();
    }

    public static Schema getCandidateKeySchema(Object recordKey, List<String> fieldsToCopy) {
        Struct key = getKeyStruct(recordKey);
        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
        for (String field : fieldsToCopy) {
            Field fieldFromKey = key.schema().field(field);
            if (fieldFromKey == null) {
                throw new DataException("Field you want to copy does not exist in key: " + field);
            }
            keySchemaBuilder.field(field, fieldFromKey.schema());
        }
        return keySchemaBuilder.build();
        }
}
