/**
 *
 * Transformation to copy Key Field(s) to Value
 *
 * Sample SinkConnector properties
 *
 * (Single Key)
 * transforms=copyToValue
 * transforms.copyToValue.type=com.rakuten.dps.kafka.connect.transforms.KeyToValue
 * transforms.copyToValue.field.name=columnName1
 *
 * (Multi Key)
 * transforms=copyToValue
 * transforms.copyToValue.type=com.rakuten.dps.kafka.connect.transforms.KeyToValue
 * transforms.copyToValue.field.name=columnName1,columnName2
 *
 */

package com.rakuten.dps.kafka.connect.transforms;

import com.rakuten.dps.kafka.connect.transforms.logic.KeyToVal;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class KeyToValue<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String FIELDS_CONFIG = "field.name";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
          .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH,
                  "Field names from the record key to extract as the record value.");

  private static final String PURPOSE = "copying fields from key to value";
  private List<String> fields;
  private Cache<Schema, Schema> keyToValueSchemaCache;

  @Override
  public void configure(Map<String, ?> configs) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    fields = config.getList(FIELDS_CONFIG);
    keyToValueSchemaCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
  }

  @Override
  public R apply(R record) {
    if (record.valueSchema() == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applyWithSchema(R record) {
    Struct key = KeyToVal.getKeyStruct(record.key());
    Struct values = KeyToVal.getValueStruct(record.value());
    Schema keySchema = keyToValueSchemaCache.get(key.schema());
    Schema newValueSchema = keyToValueSchemaCache.get(values.schema());
    if (keySchema == null) {
      keySchema  = KeyToVal.getCandidateKeySchema(key, fields);
      keyToValueSchemaCache.put(key.schema(), keySchema);
    }

    if (newValueSchema == null) {
      newValueSchema = KeyToVal.getNewValuesSchema(values.schema().fields(), keySchema.fields());
      keyToValueSchemaCache.put(values.schema(), newValueSchema);
    }
    final Struct newValues = KeyToVal.getNewValues(newValueSchema, keySchema.fields(), values.schema(), values, key);
    return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), newValueSchema, newValues, record.timestamp());
  }

  private R applySchemaless(R record) {
    final Map<String, Object> key = requireMap(record.key(), PURPOSE);
    final Map<String, Object> value = requireMap(record.value(), PURPOSE);

    for (String field : fields) {
      Object fieldFromKey = key.get(field);
      if (fieldFromKey == null) {
        throw new DataException("Field you want to copy does not exist in key: " + field);
      }
      value.put(field, key.get(field));
    }
    return record.newRecord(record.topic(), record.kafkaPartition(), null, record.key(), null, value, record.timestamp());
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {
    keyToValueSchemaCache = null;
  }

}
