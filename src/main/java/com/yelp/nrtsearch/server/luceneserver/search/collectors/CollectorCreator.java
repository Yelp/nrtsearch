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
package com.yelp.nrtsearch.server.luceneserver.search.collectors;

import com.yelp.nrtsearch.server.collectors.BucketOrder;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.PluginCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.additional.FilterCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.additional.MaxCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.additional.TermsCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.additional.TopHitsCollectorManager;
import com.yelp.nrtsearch.server.plugins.CollectorPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.utils.StructValueTransformer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Helper class for creating instances of {@link AdditionalCollectorManager} from the grpc {@link
 * Collector} messages.
 */
public class CollectorCreator {

  private static CollectorCreator instance;

  private final Map<
          String,
          CollectorProvider<
              ? extends
                  AdditionalCollectorManager<
                      ? extends org.apache.lucene.search.Collector, CollectorResult>>>
      collectorsMap = new HashMap<>();

  private CollectorCreator(LuceneServerConfiguration configuration) {}

  /**
   * Create {@link AdditionalCollectorManager} for the given {@link Collector} definition message.
   *
   * @param context search context
   * @param name collection name
   * @param collector collector definition message
   * @return collector manager usable for search
   */
  public AdditionalCollectorManager<? extends org.apache.lucene.search.Collector, CollectorResult>
      createCollectorManager(CollectorCreatorContext context, String name, Collector collector) {
    return createCollectorManagerSupplier(context, name, collector).get();
  }

  private Supplier<
          AdditionalCollectorManager<? extends org.apache.lucene.search.Collector, CollectorResult>>
      createCollectorManagerSupplier(
          CollectorCreatorContext context, String name, Collector collector) {
    Map<String, Supplier<AdditionalCollectorManager<?, CollectorResult>>> nestedCollectorSuppliers =
        collector.getNestedCollectorsMap().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Entry::getKey,
                    e -> createCollectorManagerSupplier(context, e.getKey(), e.getValue())));
    switch (collector.getCollectorsCase()) {
      case TERMS:
        BucketOrder bucketOrder;
        if (collector.getTerms().hasOrder()) {
          bucketOrder =
              BucketOrder.createBucketOrder(
                  collector.getTerms().getOrder(), collector.getNestedCollectorsMap());
        } else {
          bucketOrder = BucketOrder.DEFAULT_ORDER;
        }
        return () ->
            TermsCollectorManager.buildManager(
                name, collector.getTerms(), context, nestedCollectorSuppliers, bucketOrder);
      case PLUGINCOLLECTOR:
        PluginCollector pluginCollector = collector.getPluginCollector();
        CollectorProvider<?> provider = collectorsMap.get(pluginCollector.getName());
        if (provider == null) {
          throw new IllegalArgumentException(
              "Invalid collector name: "
                  + pluginCollector.getName()
                  + ", must be one of: "
                  + collectorsMap.keySet());
        }
        return () ->
            provider.get(
                name,
                context,
                StructValueTransformer.transformStruct(pluginCollector.getParams()),
                nestedCollectorSuppliers);
      case TOPHITSCOLLECTOR:
        return () -> new TopHitsCollectorManager(name, collector.getTopHitsCollector(), context);
      case FILTER:
        return () ->
            new FilterCollectorManager(
                name, collector.getFilter(), context, nestedCollectorSuppliers);
      case MAX:
        if (!nestedCollectorSuppliers.isEmpty()) {
          throw new IllegalArgumentException("MaxCollector cannot have nested collectors");
        }
        return () -> new MaxCollectorManager(name, collector.getMax(), context);
      default:
        throw new IllegalArgumentException(
            "Unknown Collector type: " + collector.getCollectorsCase());
    }
  }

  private void register(
      Map<
              String,
              CollectorProvider<
                  ? extends
                      AdditionalCollectorManager<
                          ? extends org.apache.lucene.search.Collector, CollectorResult>>>
          collectors) {
    collectors.forEach(this::register);
  }

  private void register(
      String name,
      CollectorProvider<
              ? extends
                  AdditionalCollectorManager<
                      ? extends org.apache.lucene.search.Collector, CollectorResult>>
          collector) {
    if (collectorsMap.containsKey(name)) {
      throw new IllegalArgumentException("Collector " + name + " already exists");
    }
    collectorsMap.put(name, collector);
  }

  /**
   * Initialize singleton instance of {@link CollectorCreator}. Registers any standard tasks and any
   * additional tasks provided by {@link com.yelp.nrtsearch.server.grpc.PluginCollector}s.
   *
   * @param configuration service configuration
   * @param plugins list of loaded plugins
   */
  public static void initialize(LuceneServerConfiguration configuration, Iterable<Plugin> plugins) {
    instance = new CollectorCreator(configuration);
    for (Plugin plugin : plugins) {
      if (plugin instanceof CollectorPlugin) {
        CollectorPlugin collectorPlugin = (CollectorPlugin) plugin;
        instance.register(collectorPlugin.getCollectors());
      }
    }
  }

  /** Get singleton instance. */
  public static CollectorCreator getInstance() {
    return instance;
  }
}
