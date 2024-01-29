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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MergeSchedulerCollectorTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testCollectMetrics() throws IOException {
    GlobalState mockGlobalState = mock(GlobalState.class);
    IndexState mockIndexState = mock(IndexState.class);
    ShardState mockShardState = mock(ShardState.class);
    IndexWriter mockIndexWriter = mock(IndexWriter.class);
    LiveIndexWriterConfig mockLiveConfig = mock(LiveIndexWriterConfig.class);
    ConcurrentMergeScheduler mockMergeScheduler = mock(ConcurrentMergeScheduler.class);

    when(mockMergeScheduler.mergeThreadCount()).thenReturn(3);
    when(mockMergeScheduler.getMaxThreadCount()).thenReturn(4);
    when(mockMergeScheduler.getMaxMergeCount()).thenReturn(6);
    when(mockLiveConfig.getMergeScheduler()).thenReturn(mockMergeScheduler);
    when(mockIndexWriter.getConfig()).thenReturn(mockLiveConfig);
    when(mockShardState.getWriter()).thenReturn(mockIndexWriter);
    when(mockIndexState.getShard(0)).thenReturn(mockShardState);
    when(mockGlobalState.getIndexNames()).thenReturn(Collections.singleton("test_index"));
    when(mockGlobalState.getIndex("test_index")).thenReturn(mockIndexState);

    MergeSchedulerCollector collector = new MergeSchedulerCollector(mockGlobalState);
    List<MetricFamilySamples> metrics = collector.collect();

    assertEquals(3, metrics.size());
    Map<String, MetricFamilySamples> metricsMap = new HashMap<>();
    for (MetricFamilySamples samples : metrics) {
      metricsMap.put(samples.name, samples);
    }

    MetricFamilySamples samples = metricsMap.get("nrt_pending_merge_count");
    assertNotNull(samples);
    assertEquals(1, samples.samples.size());
    Sample sample = samples.samples.get(0);
    assertEquals(Collections.singletonList("index"), sample.labelNames);
    assertEquals(Collections.singletonList("test_index"), sample.labelValues);
    assertEquals(3, sample.value, 0);

    samples = metricsMap.get("nrt_max_merge_thread_count");
    assertNotNull(samples);
    assertEquals(1, samples.samples.size());
    sample = samples.samples.get(0);
    assertEquals(Collections.singletonList("index"), sample.labelNames);
    assertEquals(Collections.singletonList("test_index"), sample.labelValues);
    assertEquals(4, sample.value, 0);

    samples = metricsMap.get("nrt_max_merge_count");
    assertNotNull(samples);
    assertEquals(1, samples.samples.size());
    sample = samples.samples.get(0);
    assertEquals(Collections.singletonList("index"), sample.labelNames);
    assertEquals(Collections.singletonList("test_index"), sample.labelValues);
    assertEquals(6, sample.value, 0);
  }

  @Test
  public void testNoIndex() {
    GlobalState mockGlobalState = mock(GlobalState.class);
    when(mockGlobalState.getIndexNames()).thenReturn(Collections.emptySet());
    MergeSchedulerCollector collector = new MergeSchedulerCollector(mockGlobalState);
    List<MetricFamilySamples> metrics = collector.collect();

    assertEquals(3, metrics.size());
    for (MetricFamilySamples samples : metrics) {
      assertTrue(samples.samples.isEmpty());
    }
  }
}
