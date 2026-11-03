/*
 * Copyright 2026 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yelp.nrtsearch.vectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the type of each filterable metadata key in a vector index. S3 Vectors metadata is
 * schemaless, but nrtsearch fields are typed, so each key gets a typed field ({@code m_<key>}) that
 * is registered the first time the key is seen. A key keeps its first type; later values of a
 * different type are rejected.
 */
public class MetadataSchema {
  public static final String FIELD_PREFIX = "m_";

  /** Type of a filterable metadata key. List values take the type of their elements. */
  public enum MetadataType {
    STRING,
    NUMBER,
    BOOLEAN
  }

  private Map<String, MetadataType> keyTypes = new ConcurrentHashMap<>();
  // metadata key -> nrtsearch field name
  private Map<String, String> fieldNames = new ConcurrentHashMap<>();

  public Map<String, MetadataType> getKeyTypes() {
    return keyTypes;
  }

  public void setKeyTypes(Map<String, MetadataType> keyTypes) {
    this.keyTypes = new ConcurrentHashMap<>(keyTypes);
  }

  public Map<String, String> getFieldNames() {
    return fieldNames;
  }

  public void setFieldNames(Map<String, String> fieldNames) {
    this.fieldNames = new ConcurrentHashMap<>(fieldNames);
  }

  public MetadataType getType(String key) {
    return keyTypes.get(key);
  }

  /** nrtsearch field name for a key, or null if the key has not been seen. */
  public String getFieldName(String key) {
    return fieldNames.get(key);
  }

  /**
   * Work out the type of each filterable key in a metadata object, and check it against the types
   * already known.
   *
   * @return keys that are new to the schema, with their types
   * @throws VectorsException if a value has an unsupported type or conflicts with the known type
   */
  public Map<String, MetadataType> inferNewKeys(JsonNode metadata, IndexConfig config) {
    Map<String, MetadataType> newKeys = new LinkedHashMap<>();
    if (metadata == null || metadata.isNull()) {
      return newKeys;
    }
    Iterator<Map.Entry<String, JsonNode>> it = metadata.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      String key = entry.getKey();
      if (!config.isFilterable(key)) {
        continue;
      }
      MetadataType type = inferType(key, entry.getValue());
      if (type == null) {
        // empty list, the type cannot be known yet
        continue;
      }
      MetadataType existing = keyTypes.get(key);
      if (existing == null) {
        existing = newKeys.get(key);
      }
      if (existing == null) {
        newKeys.put(key, type);
      } else if (existing != type) {
        throw VectorsException.validation(
            "Metadata key '"
                + key
                + "' has type "
                + type
                + " but this index already stores it as "
                + existing);
      }
    }
    return newKeys;
  }

  /**
   * Add new keys to the schema, giving each a unique nrtsearch field name.
   *
   * @return nrtsearch field definitions to register
   */
  public synchronized List<Field> addKeys(Map<String, MetadataType> newKeys) {
    List<Field> fields = new ArrayList<>();
    for (Map.Entry<String, MetadataType> entry : newKeys.entrySet()) {
      if (keyTypes.containsKey(entry.getKey())) {
        continue;
      }
      String fieldName = uniqueFieldName(entry.getKey());
      keyTypes.put(entry.getKey(), entry.getValue());
      fieldNames.put(entry.getKey(), fieldName);
      fields.add(toField(fieldName, entry.getValue()));
    }
    return fields;
  }

  /** Remove keys, used to roll back a failed field registration. */
  public synchronized void removeKeys(Iterable<String> keys) {
    for (String key : keys) {
      keyTypes.remove(key);
      fieldNames.remove(key);
    }
  }

  /**
   * Metadata keys may hold any characters, but nrtsearch field names must match {@code
   * [a-zA-Z_][a-zA-Z_0-9]*}. Replace other characters and add a numeric suffix if two keys map to
   * the same name.
   */
  private String uniqueFieldName(String key) {
    StringBuilder sb = new StringBuilder(FIELD_PREFIX);
    for (char c : key.toCharArray()) {
      if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
        sb.append(c);
      } else {
        sb.append('_');
      }
    }
    String base = sb.toString();
    String candidate = base;
    int suffix = 1;
    while (fieldNames.containsValue(candidate)) {
      candidate = base + "_" + suffix++;
    }
    return candidate;
  }

  static Field toField(String fieldName, MetadataType type) {
    Field.Builder builder =
        Field.newBuilder()
            .setName(fieldName)
            .setSearch(true)
            .setStoreDocValues(true)
            .setMultiValued(true);
    switch (type) {
      case STRING -> builder.setType(FieldType.ATOM);
      case NUMBER -> builder.setType(FieldType.DOUBLE);
      case BOOLEAN -> builder.setType(FieldType.BOOLEAN);
    }
    return builder.build();
  }

  /**
   * Type of a metadata value, or null for an empty list.
   *
   * @throws VectorsException for nulls, nested objects, nested or mixed-type lists
   */
  static MetadataType inferType(String key, JsonNode value) {
    if (value.isArray()) {
      MetadataType listType = null;
      for (JsonNode element : value) {
        if (element.isArray() || element.isObject()) {
          throw VectorsException.validation(
              "Metadata key '" + key + "': list elements must be strings, numbers or booleans");
        }
        MetadataType elementType = inferType(key, element);
        if (listType == null) {
          listType = elementType;
        } else if (listType != elementType) {
          throw VectorsException.validation(
              "Metadata key '" + key + "': list elements must all have the same type");
        }
      }
      return listType;
    }
    if (value.isTextual()) {
      return MetadataType.STRING;
    }
    if (value.isNumber()) {
      return MetadataType.NUMBER;
    }
    if (value.isBoolean()) {
      return MetadataType.BOOLEAN;
    }
    throw VectorsException.validation(
        "Metadata key '"
            + key
            + "': values must be a string, number, boolean or list, got "
            + value.getNodeType());
  }

  /** String values to index for a metadata value (one per list element). */
  static List<String> toFieldValues(JsonNode value) {
    if (value.isArray()) {
      List<String> values = new ArrayList<>(value.size());
      for (JsonNode element : value) {
        values.add(scalarToString(element));
      }
      return values;
    }
    return Collections.singletonList(scalarToString(value));
  }

  static String scalarToString(JsonNode value) {
    if (value.isNumber()) {
      return Double.toString(value.asDouble());
    }
    return value.asText();
  }
}
