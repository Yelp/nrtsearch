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
package com.yelp.nrtsearch.server.luceneserver.logging;

import com.yelp.nrtsearch.server.grpc.LoggingHits;

import java.util.Map;

/**
 * Interface for getting a {@link HitsLogger} implementation initialized with the given
 * parameters map.
 *
 * @param <T> HitsLogger type
 */
public interface HitsLoggerProvider<T extends HitsLogger> {
  /**
   * Get task instance with the given parameters.
   *
   * @param params java native representation of {@link LoggingHits#getParams()}
   * @return {@link com.yelp.nrtsearch.server.luceneserver.logging.HitsLogger} instance
   */
  T get(Map<String, Object> params);
}
