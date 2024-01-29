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

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Diagnostics;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collector for detailed index metrics that may be expensive to produce or publish. Collection can
 * be toggled with the verbose metrics index live setting.
 */
public class VerboseIndexCollector extends Collector {
  private static final Logger logger = LoggerFactory.getLogger(VerboseIndexCollector.class);
  private final GlobalState globalState;

  private static final Summary searchResponseSizeBytes =
      Summary.build()
          .name("nrt_search_response_size_bytes")
          .help("Size of response protobuf message")
          .quantile(0.0, 0)
          .quantile(0.5, 0.01)
          .quantile(0.95, 0.005)
          .quantile(0.99, 0.005)
          .quantile(1.0, 0)
          .labelNames("index")
          .create();

  private static final Summary searchResponseTotalHits =
      Summary.build()
          .name("nrt_search_response_total_hits")
          .help("Number of total hits for queries")
          .quantile(0.0, 0)
          .quantile(0.5, 0.01)
          .quantile(0.95, 0.005)
          .quantile(0.99, 0.005)
          .quantile(1.0, 0)
          .labelNames("index")
          .create();

  private static final Summary searchStageLatencyMs =
      Summary.build()
          .name("nrt_search_stage_latency_ms")
          .help("Latency of various search operations (ms)")
          .quantile(0.0, 0)
          .quantile(0.5, 0.01)
          .quantile(0.95, 0.005)
          .quantile(0.99, 0.005)
          .quantile(1.0, 0)
          .labelNames("index", "stage")
          .create();

  private static final Counter searchTimeoutCount =
      Counter.build()
          .name("nrt_search_timeout_count")
          .help("Number of requests that hit the recall timeout")
          .labelNames("index")
          .create();

  private static final Counter searchTerminatedEarlyCount =
      Counter.build()
          .name("nrt_search_terminated_early_count")
          .help("Number of requests that terminated early")
          .labelNames("index")
          .create();

  public static void updateSearchResponseMetrics(SearchResponse searchResponse, String index) {
    searchResponseSizeBytes.labels(index).observe(searchResponse.getSerializedSize());
    searchResponseTotalHits.labels(index).observe(searchResponse.getTotalHits().getValue());
    if (searchResponse.getHitTimeout()) {
      searchTimeoutCount.labels(index).inc();
    }
    if (searchResponse.getTerminatedEarly()) {
      searchTerminatedEarlyCount.labels(index).inc();
    }

    Diagnostics diagnostics = searchResponse.getDiagnostics();
    searchStageLatencyMs.labels(index, "recall").observe(diagnostics.getFirstPassSearchTimeMs());
    searchStageLatencyMs.labels(index, "highlight").observe(diagnostics.getHighlightTimeMs());
    searchStageLatencyMs.labels(index, "fetch").observe(diagnostics.getGetFieldsTimeMs());
    for (Map.Entry<String, Double> entry : diagnostics.getFacetTimeMsMap().entrySet()) {
      searchStageLatencyMs.labels(index, "facet:" + entry.getKey()).observe(entry.getValue());
    }
    for (Map.Entry<String, Double> entry : diagnostics.getRescorersTimeMsMap().entrySet()) {
      searchStageLatencyMs.labels(index, "rescorer:" + entry.getKey()).observe(entry.getValue());
    }
  }

  public VerboseIndexCollector(GlobalState globalState) {
    this.globalState = globalState;
  }

  @Override
  public List<MetricFamilySamples> collect() {
    List<MetricFamilySamples> mfs = new ArrayList<>();

    try {
      boolean publishMetrics = false;
      Set<String> indexNames = globalState.getIndexNames();
      for (String indexName : indexNames) {
        if (globalState.getIndex(indexName).getVerboseMetrics()) {
          publishMetrics = true;
          break;
        }
      }
      if (publishMetrics) {
        mfs.addAll(searchResponseSizeBytes.collect());
        mfs.addAll(searchResponseTotalHits.collect());
        mfs.addAll(searchStageLatencyMs.collect());
        mfs.addAll(searchTimeoutCount.collect());
        mfs.addAll(searchTerminatedEarlyCount.collect());
      }
    } catch (Exception e) {
      logger.warn("Error getting verbose index metrics: ", e);
    }

    return mfs;
  }
}
