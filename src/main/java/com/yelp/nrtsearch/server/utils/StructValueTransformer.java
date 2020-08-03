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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Transformer that converts a {@link Struct} to native java types. This conversion is done lazily
 * on access.
 */
public class StructValueTransformer
    implements com.google.common.base.Function<Value, Object>, Function<Value, Object> {
  public static final StructValueTransformer INSTANCE = new StructValueTransformer();

  /**
   * Convert a protobuf Struct into an equivalent Map containing java native types. Value conversion
   * is done lazily on access.
   *
   * @param struct struct message to convert
   * @return native representation of struct
   */
  public static Map<String, ?> transformStruct(Struct struct) {
    return Maps.transformValues(struct.getFieldsMap(), INSTANCE);
  }

  /**
   * Convert a {@link ListValue} message into a List of java native types. Value conversion is done
   * lazily on access.
   *
   * @param listValue list message to convert
   * @return native representation of list
   */
  public static List<?> transformList(ListValue listValue) {
    return Lists.transform(listValue.getValuesList(), INSTANCE);
  }

  /**
   * Convert a {@link Value} message into a java native type. Value conversion of Collections is
   * done lazily on access.
   *
   * @param value value to convert
   * @return native representation of value
   */
  public static Object transformValue(Value value) {
    return INSTANCE.apply(value);
  }

  @Nullable
  @Override
  public Object apply(@Nullable Value input) {
    if (input == null) {
      return null;
    }
    switch (input.getKindCase()) {
      case NULL_VALUE:
        return null;
      case STRING_VALUE:
        return input.getStringValue();
      case NUMBER_VALUE:
        return input.getNumberValue();
      case BOOL_VALUE:
        return input.getBoolValue();
      case LIST_VALUE:
        return transformList(input.getListValue());
      case STRUCT_VALUE:
        return transformStruct(input.getStructValue());
      default:
        throw new IllegalArgumentException("Unknown Struct value type: " + input.getKindCase());
    }
  }
}
