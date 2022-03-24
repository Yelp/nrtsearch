/*
 * Copyright 2022 Yelp Inc.
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

import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.AdditionalCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorProvider;
import java.util.Collections;
import java.util.Map;
import org.apache.lucene.search.Collector;

/**
 * Plugin interface for providing custom {@link AdditionalCollectorManager}s. Provides info for
 * registration of collectors by name. These collectors can be invoked by sending {@link
 * com.yelp.nrtsearch.server.grpc.PluginCollector} messages as part of the {@link
 * com.yelp.nrtsearch.server.grpc.SearchRequest}.
 */
public interface CollectorPlugin {
  /** Get map of collector name to {@link AdditionalCollectorManager} for registration. */
  default Map<
          String,
          CollectorProvider<
              ? extends AdditionalCollectorManager<? extends Collector, CollectorResult>>>
      getCollectors() {
    return Collections.emptyMap();
  }
}
