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

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.FetchTask;
import com.yelp.nrtsearch.server.plugins.FetchTaskPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.utils.StructValueTransformer;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory class that handles registration and creation of {@link
 * com.yelp.nrtsearch.server.luceneserver.search.FetchTasks.FetchTask}s.
 */
public class FetchTaskCreator {
  private static FetchTaskCreator instance;

  private final Map<String, FetchTaskProvider<? extends FetchTasks.FetchTask>> fetchTaskMap =
      new HashMap<>();

  /**
   * Constructor.
   *
   * @param configuration server configuration
   */
  public FetchTaskCreator(LuceneServerConfiguration configuration) {}

  /**
   * Create a {@link com.yelp.nrtsearch.server.luceneserver.search.FetchTasks.FetchTask} instance
   * given the {@link FetchTask} message from the {@link
   * com.yelp.nrtsearch.server.grpc.SearchRequest}.
   *
   * @param grpcFetchTask task definition message
   * @return {@link com.yelp.nrtsearch.server.luceneserver.search.FetchTasks.FetchTask} instance
   */
  public FetchTasks.FetchTask createFetchTask(FetchTask grpcFetchTask) {
    FetchTaskProvider<?> provider = fetchTaskMap.get(grpcFetchTask.getName());
    if (provider == null) {
      throw new IllegalArgumentException("Invalid fetch task: " + grpcFetchTask.getName());
    }
    return provider.get(StructValueTransformer.transformStruct(grpcFetchTask.getParams()));
  }

  private void register(Map<String, FetchTaskProvider<? extends FetchTasks.FetchTask>> fetchTasks) {
    fetchTasks.forEach(this::register);
  }

  private void register(String name, FetchTaskProvider<? extends FetchTasks.FetchTask> fetchTask) {
    if (fetchTaskMap.containsKey(name)) {
      throw new IllegalArgumentException("FetchTask " + name + " already exists");
    }
    fetchTaskMap.put(name, fetchTask);
  }

  /**
   * Initialize singleton instance of {@link FetchTaskCreator}. Registers any standard tasks and any
   * additional tasks provided by {@link FetchTaskPlugin}s.
   *
   * @param configuration service configuration
   * @param plugins list of loaded plugins
   */
  public static void initialize(LuceneServerConfiguration configuration, Iterable<Plugin> plugins) {
    instance = new FetchTaskCreator(configuration);
    for (Plugin plugin : plugins) {
      if (plugin instanceof FetchTaskPlugin) {
        FetchTaskPlugin fetchTaskPlugin = (FetchTaskPlugin) plugin;
        instance.register(fetchTaskPlugin.getFetchTasks());
      }
    }
  }

  /** Get singleton instance. */
  public static FetchTaskCreator getInstance() {
    return instance;
  }
}
