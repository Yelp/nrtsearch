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
package com.yelp.nrtsearch.server.luceneserver.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.luceneserver.search.SearchCutoffWrapper.CollectionTimeoutException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.junit.Test;

public class SearchCutoffWrapperTest {
  private static final String EXPECTED_TIMEOUT_MESSAGE =
      "Search collection exceeded timeout of 5.0s";

  private static class TimeSettableWrapper extends SearchCutoffWrapper<Collector> {
    long currentTime = 0;

    public TimeSettableWrapper(
        CollectorManager<Collector, SearcherResult> in,
        double timeoutSec,
        boolean noPartialResults,
        Runnable onTimeout) {
      super(in, timeoutSec, noPartialResults, onTimeout);
    }

    @Override
    public long getTimeMs() {
      return currentTime;
    }
  }

  private static class TestCollectorManager implements CollectorManager<Collector, SearcherResult> {

    @Override
    public Collector newCollector() throws IOException {
      return new TestCollector();
    }

    @Override
    public SearcherResult reduce(Collection<Collector> collectors) throws IOException {
      return null;
    }

    private static class TestCollector implements Collector {

      @Override
      public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        return null;
      }

      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
      }
    }
  }

  @Test
  public void testCollectionStopTimeout() throws IOException {
    TestCollectorManager manager = new TestCollectorManager();
    TimeSettableWrapper wrapper = new TimeSettableWrapper(manager, 5, false, () -> {});
    wrapper.currentTime = 10000;
    Collector collector = wrapper.newCollector();
    collector.getLeafCollector(null);
    wrapper.currentTime += 2000;
    collector.getLeafCollector(null);
    wrapper.currentTime += 2000;
    collector.getLeafCollector(null);
    wrapper.currentTime += 2000;
    try {
      collector.getLeafCollector(null);
      fail();
    } catch (CollectionTerminatedException ignored) {

    }
  }

  @Test
  public void testCollectionStopTimeoutMulti() throws IOException {
    TestCollectorManager manager = new TestCollectorManager();
    TimeSettableWrapper wrapper = new TimeSettableWrapper(manager, 5, false, () -> {});
    wrapper.currentTime = 10000;
    Collector collector1 = wrapper.newCollector();
    Collector collector2 = wrapper.newCollector();
    collector1.getLeafCollector(null);
    collector2.getLeafCollector(null);
    wrapper.currentTime += 2000;
    collector1.getLeafCollector(null);
    collector2.getLeafCollector(null);
    wrapper.currentTime += 2000;
    collector1.getLeafCollector(null);
    collector2.getLeafCollector(null);
    wrapper.currentTime += 2000;
    try {
      collector1.getLeafCollector(null);
      fail();
    } catch (CollectionTerminatedException ignored) {

    }
    try {
      collector2.getLeafCollector(null);
      fail();
    } catch (CollectionTerminatedException ignored) {

    }
  }

  @Test
  public void testCollectionErrorTimeout() throws IOException {
    TestCollectorManager manager = new TestCollectorManager();
    TimeSettableWrapper wrapper = new TimeSettableWrapper(manager, 5, true, () -> {});
    wrapper.currentTime = 10000;
    Collector collector = wrapper.newCollector();
    collector.getLeafCollector(null);
    wrapper.currentTime += 2000;
    collector.getLeafCollector(null);
    wrapper.currentTime += 2000;
    collector.getLeafCollector(null);
    wrapper.currentTime += 2000;
    try {
      collector.getLeafCollector(null);
      fail();
    } catch (CollectionTimeoutException e) {
      assertEquals(EXPECTED_TIMEOUT_MESSAGE, e.getMessage());
    }
  }

  @Test
  public void testCollectionErrorTimeoutMulti() throws IOException {
    TestCollectorManager manager = new TestCollectorManager();
    TimeSettableWrapper wrapper = new TimeSettableWrapper(manager, 5, true, () -> {});
    wrapper.currentTime = 10000;
    Collector collector1 = wrapper.newCollector();
    Collector collector2 = wrapper.newCollector();
    collector1.getLeafCollector(null);
    collector2.getLeafCollector(null);
    wrapper.currentTime += 2000;
    collector1.getLeafCollector(null);
    collector2.getLeafCollector(null);
    wrapper.currentTime += 2000;
    collector1.getLeafCollector(null);
    collector2.getLeafCollector(null);
    wrapper.currentTime += 2000;
    try {
      collector1.getLeafCollector(null);
      fail();
    } catch (CollectionTimeoutException e) {
      assertEquals(EXPECTED_TIMEOUT_MESSAGE, e.getMessage());
    }
    try {
      collector2.getLeafCollector(null);
      fail();
    } catch (CollectionTimeoutException e) {
      assertEquals(EXPECTED_TIMEOUT_MESSAGE, e.getMessage());
    }
  }

  @Test
  public void testOnTimeoutCalled() throws IOException {
    boolean[] hadTimeout = new boolean[1];
    TestCollectorManager manager = new TestCollectorManager();
    TimeSettableWrapper wrapper =
        new TimeSettableWrapper(manager, 5, false, () -> hadTimeout[0] = true);
    List<SearchCutoffWrapper<Collector>.TimeoutCollectorWrapper> collectors = new ArrayList<>();
    wrapper.currentTime = 10000;
    for (int i = 0; i < 5; ++i) {
      collectors.add(wrapper.newCollector());
    }
    assertFalse(hadTimeout[0]);
    wrapper.reduce(collectors);
    assertFalse(hadTimeout[0]);

    // time out one collector
    wrapper.currentTime += 5001;
    try {
      collectors.get(2).getLeafCollector(null);
      fail();
    } catch (CollectionTerminatedException ignored) {

    }

    assertFalse(hadTimeout[0]);
    wrapper.reduce(collectors);
    assertTrue(hadTimeout[0]);

    // time out all collectors
    for (var collector : collectors) {
      try {
        collector.getLeafCollector(null);
        fail();
      } catch (CollectionTerminatedException ignored) {

      }
    }

    hadTimeout[0] = false;
    wrapper.reduce(collectors);
    assertTrue(hadTimeout[0]);
  }
}
