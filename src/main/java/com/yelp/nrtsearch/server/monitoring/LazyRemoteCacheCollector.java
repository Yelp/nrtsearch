/*
 * Copyright 2026 Yelp Inc.
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

import com.yelp.nrtsearch.server.nrt.lazy.RemoteBlockCache;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.ArrayList;
import java.util.List;

/**
 * Metrics for the block cache of lazily loaded remote index files (see {@code lazyRemoteData.*}
 * config). Reports nothing until the cache is created, which happens when the first index with lazy
 * files starts.
 */
public class LazyRemoteCacheCollector implements MultiCollector {
  static final String PREFIX = "nrt_lazy_remote_cache_";

  private final GlobalState globalState;

  public LazyRemoteCacheCollector(GlobalState globalState) {
    this.globalState = globalState;
  }

  @Override
  public MetricSnapshots collect() {
    RemoteBlockCache cache = globalState.getLazyRemoteBlockCacheIfCreated();
    if (cache == null) {
      return new MetricSnapshots();
    }
    Labels labels = Labels.of("cache_type", cache.getType());
    List<MetricSnapshot> metrics = new ArrayList<>();
    metrics.add(counter("hits", "Block reads served from the cache.", cache.getHits(), labels));
    metrics.add(
        counter(
            "misses", "Block reads loaded from the remote backend.", cache.getMisses(), labels));
    metrics.add(
        counter(
            "remote_bytes", "Bytes read from the remote backend.", cache.getRemoteBytes(), labels));
    metrics.add(
        counter(
            "remote_load_seconds",
            "Total time spent loading blocks from the remote backend.",
            cache.getRemoteLoadNanos() / 1e9,
            labels));
    metrics.add(
        counter(
            "load_errors",
            "Block loads from the remote backend that failed.",
            cache.getLoadErrors(),
            labels));
    metrics.add(
        counter(
            "evictions",
            "Blocks evicted to stay within the cache size.",
            cache.getEvictions(),
            labels));
    metrics.add(
        gauge("size_bytes", "Bytes of blocks in the cache.", cache.getCurrentBytes(), labels));
    metrics.add(gauge("blocks", "Number of blocks in the cache.", cache.getBlockCount(), labels));
    metrics.add(gauge("max_bytes", "Maximum size of the cache.", cache.getMaxBytes(), labels));
    metrics.add(
        gauge("block_size_bytes", "Size of each cache block.", cache.getBlockSize(), labels));
    return new MetricSnapshots(metrics);
  }

  private static CounterSnapshot counter(String name, String help, double value, Labels labels) {
    return CounterSnapshot.builder()
        .name(PREFIX + name)
        .help(help)
        .dataPoint(
            CounterSnapshot.CounterDataPointSnapshot.builder().labels(labels).value(value).build())
        .build();
  }

  private static GaugeSnapshot gauge(String name, String help, double value, Labels labels) {
    return GaugeSnapshot.builder()
        .name(PREFIX + name)
        .help(help)
        .dataPoint(
            GaugeSnapshot.GaugeDataPointSnapshot.builder().labels(labels).value(value).build())
        .build();
  }
}
