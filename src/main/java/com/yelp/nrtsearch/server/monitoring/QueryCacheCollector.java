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
package com.yelp.nrtsearch.server.monitoring;

import com.yelp.nrtsearch.server.search.cache.NrtQueryCache;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCache;

/** Class to manage collection of metrics related to the query cache. */
public class QueryCacheCollector implements MultiCollector {
  private static final Gauge queryCacheHits =
      Gauge.builder()
          .name("nrt_query_cache_hits")
          .help("Total number of query cache hits.")
          .build();
  private static final Gauge queryCacheMisses =
      Gauge.builder()
          .name("nrt_query_cache_misses")
          .help("Total number of query cache misses.")
          .build();
  private static final Gauge queryCacheSize =
      Gauge.builder()
          .name("nrt_query_cache_size")
          .help("Total number of entries in query cache.")
          .build();
  private static final Gauge queryCacheSizeBytes =
      Gauge.builder()
          .name("nrt_query_cache_size_bytes")
          .help("Total memory used by query cache.")
          .build();
  private static final Gauge queryCacheCount =
      Gauge.builder()
          .name("nrt_query_cache_count")
          .help("Total number of entries added to the query cache.")
          .build();
  private static final Gauge queryCacheEvictionCount =
      Gauge.builder()
          .name("nrt_query_cache_eviction_count")
          .help("Total number of query cache evictions.")
          .build();
  private static final Gauge queryCacheQuerySize =
      Gauge.builder()
          .name("nrt_query_cache_query_size")
          .help("Total number of queries in query cache.")
          .build();
  private static final Gauge queryCacheQueryCount =
      Gauge.builder()
          .name("nrt_query_cache_query_count")
          .help("Total number of queries added to the query cache.")
          .build();
  private static final Gauge queryCacheQueryEvictionCount =
      Gauge.builder()
          .name("nrt_query_cache_query_eviction_count")
          .help("Total number of query cache query evictions.")
          .build();

  @Override
  public MetricSnapshots collect() {
    QueryCache queryCache = IndexSearcher.getDefaultQueryCache();
    if (!(queryCache instanceof NrtQueryCache nrtQueryCache)) {
      return new MetricSnapshots();
    }

    queryCacheHits.set(nrtQueryCache.getHitCount());
    queryCacheMisses.set(nrtQueryCache.getMissCount());
    queryCacheSize.set(nrtQueryCache.getCacheSize());
    queryCacheSizeBytes.set(nrtQueryCache.ramBytesUsed());
    queryCacheCount.set(nrtQueryCache.getCacheCount());
    queryCacheEvictionCount.set(nrtQueryCache.getEvictionCount());

    long cacheQueryCount = nrtQueryCache.getCacheQueryCount();
    long cacheQuerySize = nrtQueryCache.getCacheQuerySize();
    queryCacheQuerySize.set(cacheQuerySize);
    queryCacheQueryCount.set(cacheQueryCount);
    queryCacheQueryEvictionCount.set(cacheQueryCount - cacheQuerySize);

    List<MetricSnapshot> metrics = new ArrayList<>();
    metrics.add(queryCacheHits.collect());
    metrics.add(queryCacheMisses.collect());
    metrics.add(queryCacheSize.collect());
    metrics.add(queryCacheSizeBytes.collect());
    metrics.add(queryCacheCount.collect());
    metrics.add(queryCacheEvictionCount.collect());
    metrics.add(queryCacheQuerySize.collect());
    metrics.add(queryCacheQueryCount.collect());
    metrics.add(queryCacheQueryEvictionCount.collect());

    return new MetricSnapshots(metrics);
  }
}
