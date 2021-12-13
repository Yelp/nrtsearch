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

import com.yelp.nrtsearch.server.luceneserver.search.cache.NrtQueryCache;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCache;

/** Class to manage collection of metrics related to the query cache. */
public class QueryCacheCollector extends Collector {

  @Override
  public List<MetricFamilySamples> collect() {
    QueryCache queryCache = IndexSearcher.getDefaultQueryCache();
    if (!(queryCache instanceof NrtQueryCache)) {
      return Collections.emptyList();
    }
    NrtQueryCache nrtQueryCache = (NrtQueryCache) queryCache;

    List<MetricFamilySamples> mfs = new ArrayList<>();
    mfs.add(
        new GaugeMetricFamily(
            "nrt_query_cache_hits",
            "Total number of query cache hits.",
            nrtQueryCache.getHitCount()));
    mfs.add(
        new GaugeMetricFamily(
            "nrt_query_cache_misses",
            "Total number of query cache misses.",
            nrtQueryCache.getMissCount()));
    mfs.add(
        new GaugeMetricFamily(
            "nrt_query_cache_size",
            "Total number of entries in query cache.",
            nrtQueryCache.getCacheSize()));
    mfs.add(
        new GaugeMetricFamily(
            "nrt_query_cache_size_bytes",
            "Total memory used by query cache.",
            nrtQueryCache.ramBytesUsed()));
    mfs.add(
        new GaugeMetricFamily(
            "nrt_query_cache_count",
            "Total number of entries added to the query cache.",
            nrtQueryCache.getCacheCount()));
    mfs.add(
        new GaugeMetricFamily(
            "nrt_query_cache_eviction_count",
            "Total number of query cache evictions.",
            nrtQueryCache.getEvictionCount()));

    long cacheQueryCount = nrtQueryCache.getCacheQueryCount();
    long cacheQuerySize = nrtQueryCache.getCacheQuerySize();
    mfs.add(
        new GaugeMetricFamily(
            "nrt_query_cache_query_size",
            "Total number of queries in query cache.",
            cacheQuerySize));
    mfs.add(
        new GaugeMetricFamily(
            "nrt_query_cache_query_count",
            "Total number of queries added to the query cache.",
            cacheQueryCount));
    mfs.add(
        new GaugeMetricFamily(
            "nrt_query_cache_query_eviction_count",
            "Total number of query cache query evictions.",
            (cacheQueryCount - cacheQuerySize)));
    return mfs;
  }
}
