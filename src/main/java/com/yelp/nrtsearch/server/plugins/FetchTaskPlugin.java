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
package com.yelp.nrtsearch.server.plugins;

import com.yelp.nrtsearch.server.search.FetchTaskProvider;
import com.yelp.nrtsearch.server.search.FetchTasks;
import java.util.Collections;
import java.util.Map;

/**
 * Plugin interface for providing custom fetch tasks. Provides info for registration of tasks by
 * name. These task can be invoked by sending {@link com.yelp.nrtsearch.server.grpc.FetchTask}
 * messages as part of the {@link com.yelp.nrtsearch.server.grpc.SearchRequest}.
 */
public interface FetchTaskPlugin {
  /** Get map of task name to {@link FetchTaskProvider} for registration. */
  default Map<String, FetchTaskProvider<? extends FetchTasks.FetchTask>> getFetchTasks() {
    return Collections.emptyMap();
  }
}
