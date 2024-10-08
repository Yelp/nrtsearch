/*
 * Copyright 2023 Yelp Inc.
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Diagnostics;
import com.yelp.nrtsearch.server.grpc.TotalHits;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SearchResponseCollectorTest {

  @BeforeClass
  public static void init() {
    SearchResponse testResponse =
        SearchResponse.newBuilder()
            .setTotalHits(TotalHits.newBuilder().setValue(10).build())
            .setDiagnostics(
                Diagnostics.newBuilder()
                    .setFirstPassSearchTimeMs(1.0)
                    .setHighlightTimeMs(2.0)
                    .setGetFieldsTimeMs(3.0)
                    .putFacetTimeMs("facet1", 4.0)
                    .putRescorersTimeMs("rescorer", 5.0)
                    .build())
            .build();
    SearchResponseCollector.updateSearchResponseMetrics(testResponse, "test_index", true);
  }

  @Before
  public void setUp() {
    SearchResponseCollector.searchResponseSizeBytes.clear();
    SearchResponseCollector.searchResponseTotalHits.clear();
    SearchResponseCollector.searchStageLatencyMs.clear();
    SearchResponseCollector.searchTimeoutCount.clear();
    SearchResponseCollector.searchTerminatedEarlyCount.clear();
  }

  @Test
  public void testVerboseMetricsDisabled() throws IOException {
    GlobalState mockGlobalState = mock(GlobalState.class);
    IndexState mockIndexState = mock(IndexState.class);

    when(mockIndexState.getVerboseMetrics()).thenReturn(false);
    when(mockGlobalState.getIndexNames()).thenReturn(Collections.singleton("test_index"));
    when(mockGlobalState.getIndexOrThrow("test_index")).thenReturn(mockIndexState);

    SearchResponseCollector collector = new SearchResponseCollector(mockGlobalState);
    MetricSnapshots metrics = collector.collect();
    assertEquals(2, metrics.size());

    Map<String, MetricSnapshot> metricsMap = new HashMap<>();
    for (MetricSnapshot metric : metrics) {
      metricsMap.put(metric.getMetadata().getName(), metric);
    }
    assertEquals(
        Set.of("nrt_search_timeout_count", "nrt_search_terminated_early_count"),
        metricsMap.keySet());
  }

  @Test
  public void testVerboseMetricsEnabled() throws IOException {
    GlobalState mockGlobalState = mock(GlobalState.class);
    IndexState mockIndexState = mock(IndexState.class);

    when(mockIndexState.getVerboseMetrics()).thenReturn(true);
    when(mockGlobalState.getIndexNames()).thenReturn(Collections.singleton("test_index"));
    when(mockGlobalState.getIndexOrThrow("test_index")).thenReturn(mockIndexState);

    SearchResponseCollector collector = new SearchResponseCollector(mockGlobalState);
    MetricSnapshots metrics = collector.collect();
    assertEquals(5, metrics.size());

    Map<String, MetricSnapshot> metricsMap = new HashMap<>();
    for (MetricSnapshot metric : metrics) {
      metricsMap.put(metric.getMetadata().getName(), metric);
    }
    assertEquals(
        Set.of(
            "nrt_search_response_size_bytes",
            "nrt_search_response_total_hits",
            "nrt_search_stage_latency_ms",
            "nrt_search_timeout_count",
            "nrt_search_terminated_early_count"),
        metricsMap.keySet());
  }
}
