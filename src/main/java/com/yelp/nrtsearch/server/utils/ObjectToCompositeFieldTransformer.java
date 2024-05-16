/*
 * Copyright 2024 Yelp Inc.
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

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import java.util.Map;

public class ObjectToCompositeFieldTransformer {

  public static void enrichCompositeField(
      Object obj, SearchResponse.Hit.CompositeFieldValue.Builder compositeFieldValue) {
    if (obj instanceof Float) {
      compositeFieldValue.addFieldValue(
          SearchResponse.Hit.FieldValue.newBuilder().setFloatValue((Float) obj));
    } else if (obj instanceof String) {
      compositeFieldValue.addFieldValue(
          SearchResponse.Hit.FieldValue.newBuilder().setTextValue(String.valueOf(obj)));
    } else if (obj instanceof Double) {
      compositeFieldValue.addFieldValue(
          SearchResponse.Hit.FieldValue.newBuilder().setDoubleValue((Double) obj));
    } else if (obj instanceof Long) {
      compositeFieldValue.addFieldValue(
          SearchResponse.Hit.FieldValue.newBuilder().setLongValue((Long) obj));
    } else if (obj instanceof Integer) {
      compositeFieldValue.addFieldValue(
          SearchResponse.Hit.FieldValue.newBuilder().setIntValue((Integer) obj));
    } else if (obj instanceof Map) {
      compositeFieldValue.addFieldValue(
          SearchResponse.Hit.FieldValue.newBuilder()
              .setStructValue(StructJsonUtils.convertMapToStruct((Map<String, Object>) obj)));
    } else if (obj instanceof Boolean) {
      compositeFieldValue.addFieldValue(
          SearchResponse.Hit.FieldValue.newBuilder().setBooleanValue((Boolean) obj));
    } else if (obj instanceof Iterable<?>) {
      compositeFieldValue.addFieldValue(
          SearchResponse.Hit.FieldValue.newBuilder()
              .setListValue(StructJsonUtils.convertIterableToListValue((Iterable<?>) obj, false)));
    }
  }
}
