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
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MergeSchedulerCollectorTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    MergeSchedulerCollector.indexPendingMergeCount.clear();
    MergeSchedulerCollector.indexMaxMergeThreadCount.clear();
    MergeSchedulerCollector.indexMaxMergeCount.clear();
  }

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
    MetricSnapshots metrics = collector.collect();

    assertEquals(3, metrics.size());
    Map<String, GaugeSnapshot.GaugeDataPointSnapshot> sampleMap = new HashMap<>();
    for (MetricSnapshot metric : metrics) {
      assertEquals(1, metric.getDataPoints().size());
      GaugeSnapshot.GaugeDataPointSnapshot sample =
          (GaugeSnapshot.GaugeDataPointSnapshot) metric.getDataPoints().getFirst();
      sampleMap.put(metric.getMetadata().getName(), sample);
    }

    GaugeSnapshot.GaugeDataPointSnapshot sample = sampleMap.get("nrt_pending_merge_count");
    assertNotNull(sample);
    assertEquals("index", sample.getLabels().getName(0));
    assertEquals("test_index", sample.getLabels().getValue(0));
    assertEquals(3, sample.getValue(), 0);

    sample = sampleMap.get("nrt_max_merge_thread_count");
    assertNotNull(sample);
    assertEquals("index", sample.getLabels().getName(0));
    assertEquals("test_index", sample.getLabels().getValue(0));
    assertEquals(4, sample.getValue(), 0);

    sample = sampleMap.get("nrt_max_merge_count");
    assertNotNull(sample);
    assertEquals("index", sample.getLabels().getName(0));
    assertEquals("test_index", sample.getLabels().getValue(0));
    assertEquals(6, sample.getValue(), 0);
  }

  @Test
  public void testNoIndex() {
    GlobalState mockGlobalState = mock(GlobalState.class);
    when(mockGlobalState.getIndexNames()).thenReturn(Collections.emptySet());
    MergeSchedulerCollector collector = new MergeSchedulerCollector(mockGlobalState);
    MetricSnapshots metrics = collector.collect();

    assertEquals(3, metrics.size());
    for (MetricSnapshot metric : metrics) {
      assertTrue(metric.getDataPoints().isEmpty());
    }
  }
}
