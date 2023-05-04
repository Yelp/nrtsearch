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
package com.yelp.nrtsearch.server.luceneserver.search.collectors.additional;

import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.AdditionalCollectorManager;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

/** Class to manage collection of documents for nested sub-aggregations. */
public class NestedCollectorManagers {
  private final Map<Object, Map<String, AdditionalCollectorManager<?, CollectorResult>>>
      nestedCollectorManagersByValue = new ConcurrentHashMap<>();
  private final Map<String, Supplier<AdditionalCollectorManager<?, CollectorResult>>>
      collectorSupplierMap;
  private final ScoreMode scoreMode;
  private SearchContext searchContext;

  /**
   * Constructor.
   *
   * @param collectorSupplierMap suppliers to create nested collector managers
   */
  protected NestedCollectorManagers(
      Map<String, Supplier<AdditionalCollectorManager<?, CollectorResult>>> collectorSupplierMap) {
    if (collectorSupplierMap.isEmpty()) {
      throw new IllegalArgumentException("Supplier map cannot be empty");
    }
    this.collectorSupplierMap = collectorSupplierMap;
    this.scoreMode = getScoreMode();
  }

  /**
   * Set search context, so that it can be set for nested collectors.
   *
   * @param searchContext search context
   */
  public void setSearchContext(SearchContext searchContext) {
    this.searchContext = searchContext;
  }

  private ScoreMode getScoreMode() {
    for (Map.Entry<String, Supplier<AdditionalCollectorManager<?, CollectorResult>>> entry :
        collectorSupplierMap.entrySet()) {
      AdditionalCollectorManager<?, CollectorResult> collectorManager = entry.getValue().get();
      try {
        if (collectorManager.newCollector().scoreMode() != ScoreMode.COMPLETE_NO_SCORES) {
          return ScoreMode.COMPLETE;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  /** Get representative score mode for nested collectors. */
  public ScoreMode scoreMode() {
    return scoreMode;
  }

  /** Get collector level instance for nested aggregations. */
  public NestedCollectors newCollectors() {
    return new NestedCollectors();
  }

  /**
   * Reduce all nested collectors into a mapping of collector results.
   *
   * @param value value key
   * @param nestedCollectors all nested collectors
   * @return mapping of sub-aggregation name to collector result
   * @throws IOException
   */
  public Map<String, CollectorResult> reduce(
      Object value, Collection<NestedCollectors> nestedCollectors) throws IOException {
    return reduce(value, nestedCollectors, Collections.emptySet());
  }

  /**
   * Reduce all nested collectors into a mapping of collector results, except those specified to be
   * excluded.
   *
   * @param value value key
   * @param nestedCollectors all nested collectors
   * @param excludes collectors to exclude
   * @return mapping of sub-aggregation name to collector result
   * @throws IOException
   */
  public Map<String, CollectorResult> reduce(
      Object value, Collection<NestedCollectors> nestedCollectors, Set<String> excludes)
      throws IOException {
    Map<String, AdditionalCollectorManager<?, CollectorResult>> collectorManagers =
        nestedCollectorManagersByValue.get(value);
    if (collectorManagers == null) {
      throw new IllegalArgumentException("Unknown value: " + value);
    }
    Map<String, CollectorResult> resultMap = new Object2ObjectOpenHashMap<>();
    for (Map.Entry<String, AdditionalCollectorManager<?, CollectorResult>> entry :
        collectorManagers.entrySet()) {
      if (excludes.contains(entry.getKey())) {
        continue;
      }
      Collection<Collector> collectors =
          nestedCollectors.stream()
              .map(m -> m.nestedCollectorsByValue.get(value))
              .filter(c -> c != null && c.containsKey(entry.getKey()))
              .map(c -> c.get(entry.getKey()))
              .collect(Collectors.toList());
      @SuppressWarnings("unchecked")
      CollectorManager<Collector, CollectorResult> manager =
          (CollectorManager<Collector, CollectorResult>) entry.getValue();
      resultMap.put(entry.getKey(), manager.reduce(collectors));
    }
    return resultMap;
  }

  /**
   * Reduce a single nested collector to its collector result.
   *
   * @param value value key
   * @param collectorName nested collector name
   * @param nestedCollectors all nested collectors
   * @return reduced collector result
   * @throws IOException
   */
  public CollectorResult reduceSingle(
      Object value, String collectorName, Collection<NestedCollectors> nestedCollectors)
      throws IOException {
    Map<String, AdditionalCollectorManager<?, CollectorResult>> collectorManagers =
        nestedCollectorManagersByValue.get(value);
    if (collectorManagers == null) {
      throw new IllegalArgumentException("Unknown value: " + value);
    }
    @SuppressWarnings("unchecked")
    CollectorManager<Collector, CollectorResult> collectorManager =
        (CollectorManager<Collector, CollectorResult>) collectorManagers.get(collectorName);
    if (collectorManager == null) {
      throw new IllegalArgumentException("No collector found: " + collectorName);
    }
    Collection<Collector> collectors =
        nestedCollectors.stream()
            .map(m -> m.nestedCollectorsByValue.get(value))
            .filter(c -> c != null && c.containsKey(collectorName))
            .map(c -> c.get(collectorName))
            .collect(Collectors.toList());
    return collectorManager.reduce(collectors);
  }

  /** Collector level nested aggregation object. Tracks collectors for all values. */
  public class NestedCollectors {
    private final Object2ObjectOpenHashMap<Object, Map<String, Collector>> nestedCollectorsByValue =
        new Object2ObjectOpenHashMap<>();

    /**
     * Get leaf level nested collector.
     *
     * @param context segment context
     * @throws IOException
     */
    public NestedLeafCollectors getLeafCollector(LeafReaderContext context) throws IOException {
      return new NestedLeafCollectors(context);
    }

    /** Leaf collector level nested aggregation object. Tracks leaf collectors for all values. */
    public class NestedLeafCollectors {
      private final Object2ObjectOpenHashMap<Object, Map<String, LeafCollector>>
          nestedLeafCollectorsByValue = new Object2ObjectOpenHashMap<>();
      private final LeafReaderContext context;
      private Scorable currentScorer;

      /**
       * Constructor.
       *
       * @param context segment context
       */
      public NestedLeafCollectors(LeafReaderContext context) {
        this.context = context;
      }

      /**
       * Set scorer for current document.
       *
       * @param scorer document scorer
       * @throws IOException
       */
      public void setScorer(Scorable scorer) throws IOException {
        currentScorer = scorer;
      }

      /**
       * Collect a document for a given value key.
       *
       * @param value value key
       * @param doc lucene segment document
       * @throws IOException
       */
      public void collect(Object value, int doc) throws IOException {
        Map<String, LeafCollector> leafCollectors = nestedLeafCollectorsByValue.get(value);
        if (leafCollectors == null) {
          Map<String, Collector> collectors = nestedCollectorsByValue.get(value);
          if (collectors == null) {
            // get collector managers for all nested aggregations for this value, create if
            // it doesn't exist
            Map<String, AdditionalCollectorManager<?, CollectorResult>> collectorManagers =
                nestedCollectorManagersByValue.computeIfAbsent(
                    value,
                    key -> {
                      Map<String, AdditionalCollectorManager<?, CollectorResult>> additional =
                          new Object2ObjectOpenHashMap<>();
                      for (Map.Entry<
                              String, Supplier<AdditionalCollectorManager<?, CollectorResult>>>
                          entry : collectorSupplierMap.entrySet()) {
                        AdditionalCollectorManager<?, CollectorResult> acm = entry.getValue().get();
                        acm.setSearchContext(searchContext);
                        additional.put(entry.getKey(), acm);
                      }
                      return additional;
                    });
            // create nested collectors
            collectors = new Object2ObjectOpenHashMap<>();
            for (Map.Entry<String, AdditionalCollectorManager<?, CollectorResult>> entry :
                collectorManagers.entrySet()) {
              collectors.put(entry.getKey(), entry.getValue().newCollector());
            }
            nestedCollectorsByValue.put(value, collectors);
          }
          // create nested leaf collectors
          leafCollectors = new Object2ObjectOpenHashMap<>();
          for (Map.Entry<String, Collector> entry : collectors.entrySet()) {
            leafCollectors.put(entry.getKey(), entry.getValue().getLeafCollector(context));
          }
          nestedLeafCollectorsByValue.put(value, leafCollectors);
        }
        // collect doc with all nested leaf collectors
        for (Map.Entry<String, LeafCollector> entry : leafCollectors.entrySet()) {
          entry.getValue().setScorer(currentScorer);
          entry.getValue().collect(doc);
        }
      }
    }
  }
}
