/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.utils;

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Utility class providing helper methods for converting between native java types and protobuf
 * {@link Struct} values.
 */
public class StructJsonUtils {

  private StructJsonUtils() {}

  /**
   * Convert a Map of native java types into a protobuf Struct. This Map is expected to be json
   * compatible. May contain null, Boolean, String, Number, Iterable, or Map (String key). Long
   * values will be truncated to Double.
   *
   * @param map java native map
   * @return protobuf Struct
   * @throws NullPointerException if map is null
   * @throws IllegalArgumentException on error converting struct value
   */
  public static Struct convertMapToStruct(Map<String, Object> map) {
    return convertMapToStruct(map, false);
  }

  /**
   * Convert a Map of native java types into a protobuf Struct. This Map is expected to be json
   * compatible. May contain null, Boolean, String, Number, Iterable, or Map (String key).
   *
   * @param map java native map
   * @param longAsString if Long values should be encoded as String
   * @return protobuf Struct
   * @throws NullPointerException if map is null
   * @throws IllegalArgumentException on error converting struct value
   */
  public static Struct convertMapToStruct(Map<String, Object> map, boolean longAsString) {
    Objects.requireNonNull(map);
    Struct.Builder builder = Struct.newBuilder();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      try {
        builder.putFields(entry.getKey(), convertObjectToValue(entry.getValue(), longAsString));
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            "Error converting native map to Struct Message: " + entry, e);
      }
    }
    return builder.build();
  }

  /**
   * Convert an Iterable of native java types into a protobuf Value. This iterable may contain null,
   * Boolean, String, Number, Iterable, or Map (String key).
   *
   * @param iterable iterable of java native types
   * @param longAsString if Long values should be encoded as String
   * @return protobuf value for iterable
   */
  public static Value convertIterableToValue(Iterable<?> iterable, boolean longAsString) {
    List<Value> valueList = new ArrayList<>();
    for (Object e : iterable) {
      valueList.add(convertObjectToValue(e, longAsString));
    }
    ListValue listValue = ListValue.newBuilder().addAllValues(valueList).build();
    return Value.newBuilder().setListValue(listValue).build();
  }

  /**
   * Convert a java native Object into a protobuf value. This Object may be null, Boolean, String,
   * Number, Iterable, or Map (String key).
   *
   * @param object java native object
   * @param longAsString if Long values should be encoded as String
   * @return protobuf value for object
   * @throws IllegalArgumentException if object is of an invalid type
   */
  public static Value convertObjectToValue(Object object, boolean longAsString) {
    if (object == null) {
      return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    } else if (object instanceof Iterable) {
      return convertIterableToValue((Iterable<?>) object, longAsString);
    } else if (object instanceof Map) {
      Struct struct = convertMapToStruct((Map<String, Object>) object, longAsString);
      return Value.newBuilder().setStructValue(struct).build();
    } else if (object instanceof Boolean) {
      return Value.newBuilder().setBoolValue((Boolean) object).build();
    } else if (object instanceof String) {
      return Value.newBuilder().setStringValue((String) object).build();
    } else if (longAsString && object instanceof Long) {
      return Value.newBuilder().setStringValue(object.toString()).build();
    } else if (object instanceof Number) {
      return Value.newBuilder().setNumberValue(((Number) object).doubleValue()).build();
    }
    throw new IllegalArgumentException("Cannot convert to protobuf value, object: " + object);
  }

  /**
   * Convert a protobuf Struct into a Map of native java types.
   *
   * @param struct protobuf struct
   * @return equivalent java native map
   * @throws NullPointerException if struct is null
   */
  public static Map<String, Object> convertStructToMap(Struct struct) {
    Objects.requireNonNull(struct);
    Map<String, Object> nativeMap = new HashMap<>();
    for (Map.Entry<String, Value> entry : struct.getFieldsMap().entrySet()) {
      nativeMap.put(entry.getKey(), convertValueToObject(entry.getValue()));
    }
    return nativeMap;
  }

  /**
   * Convert a protobuf ListValue into List of java native types.
   *
   * @param listValue protobuf list value
   * @return equivalent list of java native types
   */
  public static List<Object> convertListValueToList(ListValue listValue) {
    List<Object> nativeList = new ArrayList<>(listValue.getValuesCount());
    for (Value value : listValue.getValuesList()) {
      nativeList.add(convertValueToObject(value));
    }
    return nativeList;
  }

  /**
   * Convert a protobuf Value into its java native type.
   *
   * @param value protobuf value
   * @return equivalent java native type
   */
  public static Object convertValueToObject(Value value) {
    switch (value.getKindCase()) {
      case NULL_VALUE:
        return null;
      case BOOL_VALUE:
        return value.getBoolValue();
      case NUMBER_VALUE:
        return value.getNumberValue();
      case STRING_VALUE:
        return value.getStringValue();
      case LIST_VALUE:
        return convertListValueToList(value.getListValue());
      case STRUCT_VALUE:
        return convertStructToMap(value.getStructValue());
      default:
        throw new IllegalArgumentException("Unable to convert value type: " + value.getKindCase());
    }
  }
}
