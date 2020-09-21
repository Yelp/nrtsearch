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

import com.yelp.nrtsearch.server.grpc.Script;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Utility class to convert between the gRPC {@link Script.ParamValue} included with a {@link
 * Script} and java native types.
 */
public class ScriptParamsUtils {

  private ScriptParamsUtils() {}

  /**
   * Decode the parameter map from a {@link Script} message into an equivalent map containing java
   * native types.
   *
   * @param params script parameters
   * @return native type parameter map
   */
  public static Map<String, Object> decodeParams(Map<String, Script.ParamValue> params) {
    Objects.requireNonNull(params);
    Map<String, Object> nativeParams = new HashMap<>();
    for (Map.Entry<String, Script.ParamValue> entry : params.entrySet()) {
      nativeParams.put(entry.getKey(), decodeValue(entry.getValue()));
    }
    return nativeParams;
  }

  /**
   * Decode a {@link com.yelp.nrtsearch.server.grpc.Script.ParamStructValue} message into an
   * equivalent map containing java native types.
   *
   * @param structValue struct message
   * @return native type map
   */
  public static Map<String, Object> decodeStruct(Script.ParamStructValue structValue) {
    return decodeParams(structValue.getFieldsMap());
  }

  /**
   * Decode a {@link com.yelp.nrtsearch.server.grpc.Script.ParamListValue} message into an
   * equivalent list containing java native types.
   *
   * @param listValue list message
   * @return native type list
   */
  public static List<Object> decodeList(Script.ParamListValue listValue) {
    List<Object> nativeList = new ArrayList<>(listValue.getValuesCount());
    for (Script.ParamValue value : listValue.getValuesList()) {
      nativeList.add(decodeValue(value));
    }
    return nativeList;
  }

  /**
   * Decode a {@link com.yelp.nrtsearch.server.grpc.Script.ParamValue} message into an equivalent
   * java native type.
   *
   * @param value value message
   * @return native type object
   * @throws IllegalArgumentException if value type is unknown
   */
  public static Object decodeValue(Script.ParamValue value) {
    switch (value.getParamValuesCase()) {
      case TEXTVALUE:
        return value.getTextValue();
      case BOOLEANVALUE:
        return value.getBooleanValue();
      case INTVALUE:
        return value.getIntValue();
      case LONGVALUE:
        return value.getLongValue();
      case FLOATVALUE:
        return value.getFloatValue();
      case DOUBLEVALUE:
        return value.getDoubleValue();
      case NULLVALUE:
        return null;
      case LISTVALUE:
        return decodeList(value.getListValue());
      case STRUCTVALUE:
        return decodeStruct(value.getStructValue());
      default:
        throw new IllegalArgumentException(
            "Unknown script parameter type: " + value.getParamValuesCase().name());
    }
  }

  /**
   * Encode a map containing native java types into script parameter messages, filling the params of
   * the given {@link Script.Builder}.
   *
   * @param scriptBuilder script to fill params
   * @param params java native parameters
   */
  public static void encodeParams(Script.Builder scriptBuilder, Map<String, Object> params) {
    Objects.requireNonNull(scriptBuilder);
    Objects.requireNonNull(params);
    params.forEach((key, value) -> scriptBuilder.putParams(key, encodeValue(value)));
  }

  /**
   * Encode a map containing native java types into script parameter messages, filling the fields of
   * the given {@link com.yelp.nrtsearch.server.grpc.Script.ParamStructValue.Builder}.
   *
   * @param structBuilder struct message to fill
   * @param itemMap java native map
   */
  public static void encodeStruct(
      Script.ParamStructValue.Builder structBuilder, Map<String, Object> itemMap) {
    itemMap.forEach((key, value) -> structBuilder.putFields(key, encodeValue(value)));
  }

  /**
   * Encode a list containing java types into script parameter messages, filling the values of the
   * given {@link com.yelp.nrtsearch.server.grpc.Script.ParamListValue.Builder}.
   *
   * @param listBuilder list message to fill
   * @param items java native list
   */
  public static void encodeList(Script.ParamListValue.Builder listBuilder, Iterable<Object> items) {
    items.forEach(item -> listBuilder.addValues(encodeValue(item)));
  }

  /**
   * Encode a java native type value into an equivalent {@link
   * com.yelp.nrtsearch.server.grpc.Script.ParamValue}.
   *
   * <p>Supports null, Boolean, String, Number (Int, Long, Float, Double), Map, Iterable. Maps must
   * use a String key.
   *
   * @param item java native object
   * @return parameter message
   * @throws IllegalArgumentException if Object type is not supported or Map has a non String key
   * @throws NullPointerException if Map has a null key value
   */
  public static Script.ParamValue encodeValue(Object item) {
    if (item == null) {
      return Script.ParamValue.newBuilder().setNullValue(Script.ParamNullValue.NULL_VALUE).build();
    } else if (item instanceof Double) {
      return Script.ParamValue.newBuilder().setDoubleValue((Double) item).build();
    } else if (item instanceof Long) {
      return Script.ParamValue.newBuilder().setLongValue((Long) item).build();
    } else if (item instanceof Integer) {
      return Script.ParamValue.newBuilder().setIntValue((Integer) item).build();
    } else if (item instanceof Float) {
      return Script.ParamValue.newBuilder().setFloatValue((Float) item).build();
    } else if (item instanceof String) {
      return Script.ParamValue.newBuilder().setTextValue((String) item).build();
    } else if (item instanceof Boolean) {
      return Script.ParamValue.newBuilder().setBooleanValue((Boolean) item).build();
    } else if (item instanceof Map) {
      Script.ParamStructValue.Builder builder = Script.ParamStructValue.newBuilder();
      // This is an unsafe cast because the key may not be a String.
      // Catch the Exception to throw a better error.
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) item;
      try {
        encodeStruct(builder, map);
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            "Error converting native map to ParamStructValue Message: " + item, e);
      }
      return Script.ParamValue.newBuilder().setStructValue(builder).build();
    } else if (item instanceof Iterable) {
      Script.ParamListValue.Builder builder = Script.ParamListValue.newBuilder();
      @SuppressWarnings("unchecked")
      Iterable<Object> iterable = (Iterable<Object>) item;
      encodeList(builder, iterable);
      return Script.ParamValue.newBuilder().setListValue(builder).build();
    }
    throw new IllegalArgumentException(
        "Item is not valid script param type: " + item + ", type: " + item.getClass());
  }
}
