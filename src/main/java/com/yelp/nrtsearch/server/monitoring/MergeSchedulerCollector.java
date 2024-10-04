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
package com.yelp.nrtsearch.server.monitoring;

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Collector to for metrics from the {@link IndexWriter}'s {@link MergeScheduler}. */
public class MergeSchedulerCollector implements MultiCollector {
  private static final Logger logger = LoggerFactory.getLogger(MergeSchedulerCollector.class);

  @VisibleForTesting
  static final Gauge indexPendingMergeCount =
      Gauge.builder()
          .name("nrt_pending_merge_count")
          .help("Current number of pending merges")
          .labelNames("index")
          .build();

  @VisibleForTesting
  static final Gauge indexMaxMergeThreadCount =
      Gauge.builder()
          .name("nrt_max_merge_thread_count")
          .help("Max running merge threads")
          .labelNames("index")
          .build();

  @VisibleForTesting
  static final Gauge indexMaxMergeCount =
      Gauge.builder()
          .name("nrt_max_merge_count")
          .help("Max existing merge threads")
          .labelNames("index")
          .build();

  private final GlobalState globalState;

  public MergeSchedulerCollector(GlobalState globalState) {
    this.globalState = globalState;
  }

  @Override
  public MetricSnapshots collect() {
    List<MetricSnapshot> metrics = new ArrayList<>();

    try {
      Set<String> indexNames = globalState.getIndexNames();
      for (String indexName : indexNames) {
        IndexWriter writer = globalState.getIndex(indexName).getShard(0).getWriter();
        if (writer != null) {
          MergeScheduler mergeScheduler = writer.getConfig().getMergeScheduler();
          if (mergeScheduler instanceof ConcurrentMergeScheduler concurrentMergeScheduler) {
            indexPendingMergeCount
                .labelValues(indexName)
                .set(concurrentMergeScheduler.mergeThreadCount());
            indexMaxMergeThreadCount
                .labelValues(indexName)
                .set(concurrentMergeScheduler.getMaxThreadCount());
            indexMaxMergeCount
                .labelValues(indexName)
                .set(concurrentMergeScheduler.getMaxMergeCount());
          }
        }
      }
      metrics.add(indexMaxMergeCount.collect());
      metrics.add(indexMaxMergeThreadCount.collect());
      metrics.add(indexPendingMergeCount.collect());
    } catch (Exception e) {
      logger.warn("Error getting merge scheduler metrics: ", e);
    }
    return new MetricSnapshots(metrics);
  }
}
