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
package com.yelp.nrtsearch.server.luceneserver.search.collectors;

import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.PluginCollector;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.lucene.search.Collector;

/**
 * Interface for getting an {@link AdditionalCollectorManager} implementation initialized with the
 * given context and parameters
 *
 * @param <T> collector manager type
 */
@FunctionalInterface
public interface CollectorProvider<
    T extends AdditionalCollectorManager<? extends Collector, CollectorResult>> {

  /**
   * Get an {@link AdditionalCollectorManager} implementation initialized with the given context and
   * parameters.
   *
   * @param name collection name
   * @param context collector creator context
   * @param params collector parameters decoded from {@link PluginCollector}
   * @param nestedCollectorSuppliers suppliers to produce instances of nested sub-aggregations
   *     defined in request
   * @return {@link AdditionalCollectorManager} instance
   */
  T get(
      String name,
      CollectorCreatorContext context,
      Map<String, Object> params,
      Map<String, Supplier<AdditionalCollectorManager<? extends Collector, CollectorResult>>>
          nestedCollectorSuppliers);
}
