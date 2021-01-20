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
import java.util.List;
import java.util.Map;

public class StructJsonUtils {

  public static Struct convertMapToStruct(Map<String, Object> map) {
    Struct.Builder builder = Struct.newBuilder();
    for (Map.Entry<String, Object> e : map.entrySet()) {
      builder.putFields(e.getKey(), convertObjectToValue(e.getValue()));
    }
    return builder.build();
  }

  public static Value convertObjectToValue(Object object) {
    if (object == null) {
      return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    } else if (object instanceof List) {
      List<Value> valueList = new ArrayList<>();
      for (Object e : (List<Object>) object) {
        valueList.add(convertObjectToValue(e));
      }
      ListValue listValue = ListValue.newBuilder().addAllValues(valueList).build();
      return Value.newBuilder().setListValue(listValue).build();
    } else if (object instanceof Map) {
      Struct struct = convertMapToStruct((Map<String, Object>) object);
      return Value.newBuilder().setStructValue(struct).build();
    } else if (object instanceof Boolean) {
      return Value.newBuilder().setBoolValue((Boolean) object).build();
    } else if (object instanceof String) {
      return Value.newBuilder().setStringValue((String) object).build();
    } else if (object instanceof Number) {
      return Value.newBuilder().setNumberValue(((Number) object).doubleValue()).build();
    }
    throw new RuntimeException("Cannot convert to protobuf value, object: " + object);
  }
}
