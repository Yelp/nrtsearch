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
package com.yelp.nrtsearch.server.luceneserver.search;

import com.yelp.nrtsearch.server.grpc.FetchTask;
import java.util.Map;

/**
 * Interface for providing an instance of a {@link
 * com.yelp.nrtsearch.server.luceneserver.search.FetchTasks.FetchTask}.
 *
 * @param <T> type for {@link com.yelp.nrtsearch.server.luceneserver.search.FetchTasks.FetchTask}
 */
@FunctionalInterface
public interface FetchTaskProvider<T extends FetchTasks.FetchTask> {
  /**
   * Get task instance with the given parameters.
   *
   * @param params java native representation of {@link FetchTask#getParams()}
   * @return {@link com.yelp.nrtsearch.server.luceneserver.search.FetchTasks.FetchTask} instance
   */
  T get(Map<String, Object> params);
}
