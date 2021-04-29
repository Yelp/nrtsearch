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
package com.yelp.nrtsearch.server.luceneserver.search.collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.Rescorer;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder;
import com.yelp.nrtsearch.server.grpc.SearchResponse.SearchState;
import com.yelp.nrtsearch.server.luceneserver.search.SearchCutoffWrapper;
import com.yelp.nrtsearch.server.luceneserver.search.SearchStatsWrapper;
import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

public class DocCollectorTest {

  private static class TestDocCollector extends DocCollector {
    private final CollectorManager<? extends Collector, ? extends TopDocs> manager;

    public TestDocCollector(SearchRequest request) {
      super(request);
      manager = new TestCollectorManager();
    }

    @Override
    public CollectorManager<? extends Collector, ? extends TopDocs> getManager() {
      return manager;
    }

    @Override
    public void fillHitRanking(Builder hitResponse, ScoreDoc scoreDoc) {}

    @Override
    public void fillLastHit(SearchState.Builder stateBuilder, ScoreDoc lastHit) {}

    public static class TestCollectorManager implements CollectorManager<Collector, TopDocs> {

      @Override
      public Collector newCollector() throws IOException {
        return null;
      }

      @Override
      public TopDocs reduce(Collection<Collector> collectors) throws IOException {
        return null;
      }
    }
  }

  @Test
  public void testNoTimeoutWrapper() {
    SearchRequest request = SearchRequest.newBuilder().setTopHits(10).build();
    TestDocCollector docCollector = new TestDocCollector(request);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertSame(docCollector.getManager(), docCollector.getWrappedManager());
  }

  @Test
  public void testHasTimeoutWrapper() {
    SearchRequest request = SearchRequest.newBuilder().setTopHits(10).setTimeoutSec(5).build();
    TestDocCollector docCollector = new TestDocCollector(request);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchCutoffWrapper);
    assertNotSame(docCollector.getManager(), docCollector.getWrappedManager());
  }

  @Test
  public void testHasStatsWrapper() {
    SearchRequest request = SearchRequest.newBuilder().setTopHits(10).setProfile(true).build();
    TestDocCollector docCollector = new TestDocCollector(request);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchStatsWrapper);
    assertSame(
        docCollector.getManager(),
        ((SearchStatsWrapper<?, ?>) docCollector.getWrappedManager()).getWrapped());
    assertNotSame(docCollector.getManager(), docCollector.getWrappedManager());
  }

  @Test
  public void testHasStatsAndTimeoutWrapper() {
    SearchRequest request =
        SearchRequest.newBuilder().setTopHits(10).setTimeoutSec(5).setProfile(true).build();
    TestDocCollector docCollector = new TestDocCollector(request);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchStatsWrapper);
    assertTrue(
        ((SearchStatsWrapper<?, ?>) docCollector.getWrappedManager()).getWrapped()
            instanceof SearchCutoffWrapper);
  }

  @Test
  public void testNumHitsToCollect() {
    SearchRequest.Builder builder = SearchRequest.newBuilder();
    builder.setTopHits(200);
    builder.addRescorers(Rescorer.newBuilder().setWindowSize(1000).build());
    TestDocCollector docCollector = new TestDocCollector(builder.build());
    assertEquals(1000, docCollector.getNumHitsToCollect());
  }
}
