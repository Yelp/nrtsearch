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
package com.yelp.nrtsearch.server.rescore;

import java.util.Map;

/**
 * Interface for getting a {@link RescoreOperation} implementation initialized with the given
 * parameters map.
 *
 * @param <T> rescorer type
 */
public interface RescorerProvider<T extends RescoreOperation> {

  /**
   * Get a {@link RescoreOperation} implementation initialized with the given parameters.
   *
   * @param params rescorer parameters decoded from {@link
   *     com.yelp.nrtsearch.server.grpc.PluginRescorer}
   * @return {@link RescoreOperation} instance
   */
  T get(Map<String, Object> params);
}
