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
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Diagnostics;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Summary;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collector for metrics related to the {@link SearchResponse}. Has the option to collect verbose
 * metrics, which may be expensive to produce or publish
 */
public class SearchResponseCollector implements MultiCollector {
  private static final Logger logger = LoggerFactory.getLogger(SearchResponseCollector.class);
  private final GlobalState globalState;

  @VisibleForTesting
  static final Summary searchResponseSizeBytes =
      Summary.builder()
          .name("nrt_search_response_size_bytes")
          .help("Size of response protobuf message")
          .quantile(0.0, 0)
          .quantile(0.5, 0.01)
          .quantile(0.95, 0.005)
          .quantile(0.99, 0.005)
          .quantile(1.0, 0)
          .labelNames("index")
          .build();

  @VisibleForTesting
  static final Summary searchResponseTotalHits =
      Summary.builder()
          .name("nrt_search_response_total_hits")
          .help("Number of total hits for queries")
          .quantile(0.0, 0)
          .quantile(0.5, 0.01)
          .quantile(0.95, 0.005)
          .quantile(0.99, 0.005)
          .quantile(1.0, 0)
          .labelNames("index")
          .build();

  @VisibleForTesting
  static final Summary searchStageLatencyMs =
      Summary.builder()
          .name("nrt_search_stage_latency_ms")
          .help("Latency of various search operations (ms)")
          .quantile(0.0, 0)
          .quantile(0.5, 0.01)
          .quantile(0.95, 0.005)
          .quantile(0.99, 0.005)
          .quantile(1.0, 0)
          .labelNames("index", "stage")
          .build();

  @VisibleForTesting
  static final Counter searchTimeoutCount =
      Counter.builder()
          .name("nrt_search_timeout_count")
          .help("Number of requests that hit the recall timeout")
          .labelNames("index")
          .build();

  @VisibleForTesting
  static final Counter searchTerminatedEarlyCount =
      Counter.builder()
          .name("nrt_search_terminated_early_count")
          .help("Number of requests that terminated early")
          .labelNames("index")
          .build();

  public static void updateSearchResponseMetrics(
      SearchResponse searchResponse, String index, boolean verbose) {
    if (searchResponse.getHitTimeout()) {
      searchTimeoutCount.labelValues(index).inc();
    }
    if (searchResponse.getTerminatedEarly()) {
      searchTerminatedEarlyCount.labelValues(index).inc();
    }

    if (verbose) {
      searchResponseSizeBytes.labelValues(index).observe(searchResponse.getSerializedSize());
      searchResponseTotalHits.labelValues(index).observe(searchResponse.getTotalHits().getValue());

      Diagnostics diagnostics = searchResponse.getDiagnostics();
      searchStageLatencyMs
          .labelValues(index, "recall")
          .observe(diagnostics.getFirstPassSearchTimeMs());
      searchStageLatencyMs
          .labelValues(index, "highlight")
          .observe(diagnostics.getHighlightTimeMs());
      searchStageLatencyMs.labelValues(index, "fetch").observe(diagnostics.getGetFieldsTimeMs());
      for (Map.Entry<String, Double> entry : diagnostics.getFacetTimeMsMap().entrySet()) {
        searchStageLatencyMs
            .labelValues(index, "facet:" + entry.getKey())
            .observe(entry.getValue());
      }
      searchStageLatencyMs.labelValues(index, "rescore").observe(diagnostics.getRescoreTimeMs());//adding extra rescore metric to avoid calculating average of all rescorers
      for (Map.Entry<String, Double> entry : diagnostics.getRescorersTimeMsMap().entrySet()) {
        searchStageLatencyMs
            .labelValues(index, "rescorer:" + entry.getKey())
            .observe(entry.getValue());
      }
    }
  }

  public SearchResponseCollector(GlobalState globalState) {
    this.globalState = globalState;
  }

  @Override
  public MetricSnapshots collect() {
    List<MetricSnapshot> metrics = new ArrayList<>();

    try {
      metrics.add(searchTimeoutCount.collect());
      metrics.add(searchTerminatedEarlyCount.collect());
      metrics.add(searchStageLatencyMs.collect());// Just adding this here should mean it gets published to prometheus, is that what we want?
      // when is publishVerboseMetrics set to true, I couldn't find this metric in the grafana shard without any filters?
      // maybe it makes sense to add an extra parameter like publishSearchStageLatencyMs with default value true (?)

      boolean publishVerboseMetrics = false;
      Set<String> indexNames = globalState.getIndexNames();
      for (String indexName : indexNames) {
        if (globalState.getIndexOrThrow(indexName).getVerboseMetrics()) {
          publishVerboseMetrics = true;
          break;
        }
      }
      if (publishVerboseMetrics) {
        metrics.add(searchResponseSizeBytes.collect());
        metrics.add(searchResponseTotalHits.collect());
      }
    } catch (Exception e) {
      logger.warn("Error getting search response metrics: ", e);
    }

    return new MetricSnapshots(metrics);
  }
}
