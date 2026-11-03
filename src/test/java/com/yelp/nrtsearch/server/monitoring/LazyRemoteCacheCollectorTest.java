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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.nrt.lazy.RemoteBlockCache;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class LazyRemoteCacheCollectorTest {

  @Test
  public void testOffHeapAllocatedBytes() throws Exception {
    // capacity for 8 blocks, but only one 4 block slab allocated so far
    RemoteBlockCache cache = new RemoteBlockCache.OffHeap(80, 10, 40);
    cache.read("f", 0, 0, new byte[10], 0, 10, () -> new byte[10]);
    GlobalState globalState = mock(GlobalState.class);
    when(globalState.getLazyRemoteBlockCacheIfCreated()).thenReturn(cache);
    Map<String, Double> values = new HashMap<>();
    for (MetricSnapshot snapshot : new LazyRemoteCacheCollector(globalState).collect()) {
      DataPointSnapshot point = snapshot.getDataPoints().get(0);
      assertEquals("off_heap", point.getLabels().get("cache_type"));
      if (snapshot instanceof GaugeSnapshot) {
        values.put(
            snapshot.getMetadata().getName(),
            ((GaugeSnapshot.GaugeDataPointSnapshot) point).getValue());
      }
    }
    assertEquals(40.0, values.get("nrt_lazy_remote_cache_allocated_bytes"), 0);
    assertEquals(10.0, values.get("nrt_lazy_remote_cache_size_bytes"), 0);
    assertEquals(80.0, values.get("nrt_lazy_remote_cache_max_bytes"), 0);
  }

  @Test
  public void testNoCache() {
    GlobalState globalState = mock(GlobalState.class);
    when(globalState.getLazyRemoteBlockCacheIfCreated()).thenReturn(null);
    assertEquals(0, new LazyRemoteCacheCollector(globalState).collect().size());
  }

  @Test
  public void testMetrics() throws Exception {
    RemoteBlockCache cache = new RemoteBlockCache.Memory(20, 10);
    for (int block : new int[] {0, 0, 1, 2}) {
      cache.read("f", block, 0, new byte[10], 0, 10, () -> new byte[10]);
    }

    GlobalState globalState = mock(GlobalState.class);
    when(globalState.getLazyRemoteBlockCacheIfCreated()).thenReturn(cache);
    MetricSnapshots snapshots = new LazyRemoteCacheCollector(globalState).collect();

    Map<String, Double> values = new HashMap<>();
    for (MetricSnapshot snapshot : snapshots) {
      DataPointSnapshot point = snapshot.getDataPoints().get(0);
      assertEquals("memory", point.getLabels().get("cache_type"));
      double value =
          snapshot instanceof CounterSnapshot
              ? ((CounterSnapshot.CounterDataPointSnapshot) point).getValue()
              : ((GaugeSnapshot.GaugeDataPointSnapshot) point).getValue();
      values.put(snapshot.getMetadata().getName(), value);
    }
    String p = "nrt_lazy_remote_cache_";
    assertEquals(1.0, values.get(p + "hits"), 0);
    assertEquals(3.0, values.get(p + "misses"), 0);
    assertEquals(30.0, values.get(p + "remote_bytes"), 0);
    assertEquals(1.0, values.get(p + "evictions"), 0);
    assertEquals(0.0, values.get(p + "load_errors"), 0);
    assertEquals(20.0, values.get(p + "size_bytes"), 0);
    assertEquals(2.0, values.get(p + "blocks"), 0);
    assertEquals(20.0, values.get(p + "max_bytes"), 0);
    assertEquals(10.0, values.get(p + "block_size_bytes"), 0);
    assertEquals(20.0, values.get(p + "allocated_bytes"), 0);
    assertTrue(values.get(p + "remote_load_seconds") >= 0);
    assertEquals(11, values.size());
  }
}
