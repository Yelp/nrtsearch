/*
 * Copyright 2023 Yelp Inc.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/** Class with utility methods for working with json using the jackson library. */
public class JsonUtils {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private JsonUtils() {}

  /**
   * Convert the provided object into a json string.
   *
   * @param obj object to convert
   * @return json string representation
   * @throws IOException
   */
  public static String objectToJsonStr(Object obj) throws IOException {
    return OBJECT_MAPPER.writeValueAsString(obj);
  }

  /**
   * Convert the given java Object into a class instance.
   *
   * @param fromValue source object
   * @param toValueType created class type
   * @return instance of desired class
   * @param <T> class type
   */
  public static <T> T convertValue(Object fromValue, Class<T> toValueType) {
    return OBJECT_MAPPER.convertValue(fromValue, toValueType);
  }

  /**
   * Read the provided json string into the provided class type.
   *
   * @param content json string
   * @param valueType class type
   * @return instance of the class type
   * @param <T> class type
   * @throws IOException on error reading the json string
   */
  public static <T> T readValue(String content, Class<T> valueType) throws IOException {
    return OBJECT_MAPPER.readValue(content, valueType);
  }
}
