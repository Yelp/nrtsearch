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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class IndexIdUtil {

  private static final DateTimeFormatter INDEX_ID_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");

  /** Generate a unique index id based on the current time formatted as yyyyMMddHHmmssSSS. */
  public static String generateIndexId() {
    return INDEX_ID_FORMATTER.format(LocalDateTime.now(ZoneId.of("UTC")));
  }

  /** Check if the given string is a valid index id. */
  public static boolean isIndexId(String indexId) {
    try {
      INDEX_ID_FORMATTER.parse(indexId);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
