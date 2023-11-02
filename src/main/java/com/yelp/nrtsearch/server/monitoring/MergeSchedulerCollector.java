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

import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Collector to for metrics from the {@link IndexWriter}'s {@link MergeScheduler}. */
public class MergeSchedulerCollector extends Collector {
  private static final Logger logger = LoggerFactory.getLogger(MergeSchedulerCollector.class);
  private final GlobalState globalState;

  public MergeSchedulerCollector(GlobalState globalState) {
    this.globalState = globalState;
  }

  @Override
  public List<MetricFamilySamples> collect() {
    List<MetricFamilySamples> mfs = new ArrayList<>();

    GaugeMetricFamily indexPendingMergeCount =
        new GaugeMetricFamily(
            "nrt_pending_merge_count",
            "Current number of pending merges",
            Collections.singletonList("index"));
    mfs.add(indexPendingMergeCount);

    GaugeMetricFamily indexMaxMergeThreadCount =
        new GaugeMetricFamily(
            "nrt_max_merge_thread_count",
            "Max running merge threads",
            Collections.singletonList("index"));
    mfs.add(indexMaxMergeThreadCount);

    GaugeMetricFamily indexMaxMergeCount =
        new GaugeMetricFamily(
            "nrt_max_merge_count",
            "Max existing merge threads",
            Collections.singletonList("index"));
    mfs.add(indexMaxMergeCount);

    try {
      Set<String> indexNames = globalState.getIndexNames();
      for (String indexName : indexNames) {
        IndexWriter writer = globalState.getIndex(indexName).getShard(0).getWriter();
        List<String> labels = Collections.singletonList(indexName);
        if (writer != null) {
          MergeScheduler mergeScheduler = writer.getConfig().getMergeScheduler();
          if (mergeScheduler instanceof ConcurrentMergeScheduler) {
            ConcurrentMergeScheduler concurrentMergeScheduler =
                (ConcurrentMergeScheduler) mergeScheduler;
            indexPendingMergeCount.addMetric(labels, concurrentMergeScheduler.mergeThreadCount());
            indexMaxMergeThreadCount.addMetric(
                labels, concurrentMergeScheduler.getMaxThreadCount());
            indexMaxMergeCount.addMetric(labels, concurrentMergeScheduler.getMaxMergeCount());
          }
        }
      }
    } catch (Exception e) {
      logger.warn("Error getting merge scheduler metrics: ", e);
    }
    return mfs;
  }
}
