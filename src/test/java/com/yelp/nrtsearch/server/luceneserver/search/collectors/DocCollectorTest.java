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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.grpc.Rescorer;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder;
import com.yelp.nrtsearch.server.grpc.SearchResponse.SearchState;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.search.SearchCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.SearchCutoffWrapper;
import com.yelp.nrtsearch.server.luceneserver.search.SearchStatsWrapper;
import com.yelp.nrtsearch.server.luceneserver.search.TerminateAfterWrapper;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;
import org.mockito.Mockito;

public class DocCollectorTest {

  private static class TestDocCollector extends DocCollector {
    private final CollectorManager<? extends Collector, ? extends TopDocs> manager;

    public static IndexState getMockState() {
      IndexState indexState = Mockito.mock(IndexState.class);
      when(indexState.getDefaultSearchTimeoutSec()).thenReturn(0.0);
      when(indexState.getDefaultSearchTimeoutCheckEvery()).thenReturn(0);
      return indexState;
    }

    public TestDocCollector(SearchRequest request) {
      this(request, getMockState());
    }

    public TestDocCollector(SearchRequest request, IndexState indexState) {
      super(
          new CollectorCreatorContext(request, indexState, null, Collections.emptyMap(), null),
          Collections.emptyList());
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
    assertTrue(docCollector.getWrappedManager() instanceof SearchCollectorManager);
    assertSame(
        docCollector.getManager(),
        ((SearchCollectorManager) docCollector.getWrappedManager()).getDocCollectorManger());
  }

  @Test
  public void testHasTimeoutWrapper() {
    SearchRequest request = SearchRequest.newBuilder().setTopHits(10).setTimeoutSec(5).build();
    TestDocCollector docCollector = new TestDocCollector(request);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchCutoffWrapper);
  }

  @Test
  public void testUsesDefaultTimeout() {
    IndexState indexState = Mockito.mock(IndexState.class);
    when(indexState.getDefaultSearchTimeoutSec()).thenReturn(3.0);
    when(indexState.getDefaultSearchTimeoutCheckEvery()).thenReturn(0);

    SearchRequest request = SearchRequest.newBuilder().setTopHits(10).build();
    TestDocCollector docCollector = new TestDocCollector(request, indexState);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchCutoffWrapper);
    SearchCutoffWrapper<?> cutoffWrapper =
        (SearchCutoffWrapper<?>) docCollector.getWrappedManager();
    assertEquals(3.0, cutoffWrapper.getTimeoutSec(), 0.0);
    assertEquals(0, cutoffWrapper.getCheckEvery());
  }

  @Test
  public void testQueryOverridesDefaultTimeout() {
    IndexState indexState = Mockito.mock(IndexState.class);
    when(indexState.getDefaultSearchTimeoutSec()).thenReturn(3.0);
    when(indexState.getDefaultSearchTimeoutCheckEvery()).thenReturn(0);

    SearchRequest request = SearchRequest.newBuilder().setTopHits(10).setTimeoutSec(2.0).build();
    TestDocCollector docCollector = new TestDocCollector(request, indexState);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchCutoffWrapper);
    SearchCutoffWrapper<?> cutoffWrapper =
        (SearchCutoffWrapper<?>) docCollector.getWrappedManager();
    assertEquals(2.0, cutoffWrapper.getTimeoutSec(), 0.0);
    assertEquals(0, cutoffWrapper.getCheckEvery());
  }

  @Test
  public void testUsesDefaultTimeoutCheckEvery() {
    IndexState indexState = Mockito.mock(IndexState.class);
    when(indexState.getDefaultSearchTimeoutSec()).thenReturn(6.0);
    when(indexState.getDefaultSearchTimeoutCheckEvery()).thenReturn(10);

    SearchRequest request = SearchRequest.newBuilder().setTopHits(10).build();
    TestDocCollector docCollector = new TestDocCollector(request, indexState);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchCutoffWrapper);
    SearchCutoffWrapper<?> cutoffWrapper =
        (SearchCutoffWrapper<?>) docCollector.getWrappedManager();
    assertEquals(6.0, cutoffWrapper.getTimeoutSec(), 0.0);
    assertEquals(10, cutoffWrapper.getCheckEvery());
  }

  @Test
  public void testQueryOverridesDefaultTimeoutCheckEvery() {
    IndexState indexState = Mockito.mock(IndexState.class);
    when(indexState.getDefaultSearchTimeoutSec()).thenReturn(0.0);
    when(indexState.getDefaultSearchTimeoutCheckEvery()).thenReturn(20);

    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(10)
            .setTimeoutSec(7.0)
            .setTimeoutCheckEvery(30)
            .build();
    TestDocCollector docCollector = new TestDocCollector(request, indexState);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchCutoffWrapper);
    SearchCutoffWrapper<?> cutoffWrapper =
        (SearchCutoffWrapper<?>) docCollector.getWrappedManager();
    assertEquals(7.0, cutoffWrapper.getTimeoutSec(), 0.0);
    assertEquals(30, cutoffWrapper.getCheckEvery());
  }

  @Test
  public void testHasStatsWrapper() {
    SearchRequest request = SearchRequest.newBuilder().setTopHits(10).setProfile(true).build();
    TestDocCollector docCollector = new TestDocCollector(request);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchStatsWrapper);
    SearchStatsWrapper<?> searchStatsWrapper =
        (SearchStatsWrapper<?>) docCollector.getWrappedManager();
    assertTrue(searchStatsWrapper.getWrapped() instanceof SearchCollectorManager);
    SearchCollectorManager searchCollectorManager =
        (SearchCollectorManager) searchStatsWrapper.getWrapped();
    assertSame(docCollector.getManager(), searchCollectorManager.getDocCollectorManger());
  }

  @Test
  public void testHasStatsAndTimeoutWrapper() {
    SearchRequest request =
        SearchRequest.newBuilder().setTopHits(10).setTimeoutSec(5).setProfile(true).build();
    TestDocCollector docCollector = new TestDocCollector(request);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchStatsWrapper);
    assertTrue(
        ((SearchStatsWrapper<?>) docCollector.getWrappedManager()).getWrapped()
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

  @Test
  public void testHasTerminateAfterWrapper() {
    SearchRequest request = SearchRequest.newBuilder().setTopHits(10).setTerminateAfter(5).build();
    TestDocCollector docCollector = new TestDocCollector(request);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof TerminateAfterWrapper);
    assertEquals(
        5, ((TerminateAfterWrapper<?>) docCollector.getWrappedManager()).getTerminateAfter());
  }

  @Test
  public void testUsesDefaultTerminateAfter() {
    IndexState indexState = Mockito.mock(IndexState.class);
    when(indexState.getDefaultTerminateAfter()).thenReturn(100);

    SearchRequest request = SearchRequest.newBuilder().setTopHits(10).build();
    TestDocCollector docCollector = new TestDocCollector(request, indexState);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof TerminateAfterWrapper);
    assertEquals(
        100, ((TerminateAfterWrapper<?>) docCollector.getWrappedManager()).getTerminateAfter());
  }

  @Test
  public void testOverrideDefaultTerminateAfter() {
    IndexState indexState = Mockito.mock(IndexState.class);
    when(indexState.getDefaultTerminateAfter()).thenReturn(100);

    SearchRequest request = SearchRequest.newBuilder().setTopHits(10).setTerminateAfter(75).build();
    TestDocCollector docCollector = new TestDocCollector(request, indexState);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof TerminateAfterWrapper);
    assertEquals(
        75, ((TerminateAfterWrapper<?>) docCollector.getWrappedManager()).getTerminateAfter());
  }

  @Test
  public void testWithAllWrappers() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(10)
            .setTerminateAfter(3)
            .setTimeoutSec(5)
            .setProfile(true)
            .build();
    TestDocCollector docCollector = new TestDocCollector(request);
    assertTrue(docCollector.getManager() instanceof TestDocCollector.TestCollectorManager);
    assertTrue(docCollector.getWrappedManager() instanceof SearchStatsWrapper);
    SearchStatsWrapper<?> statsWrapper = (SearchStatsWrapper<?>) docCollector.getWrappedManager();
    assertTrue(statsWrapper.getWrapped() instanceof TerminateAfterWrapper);
    TerminateAfterWrapper<?> terminateAfterWrapper =
        (TerminateAfterWrapper<?>) statsWrapper.getWrapped();
    assertTrue(terminateAfterWrapper.getWrapped() instanceof SearchCutoffWrapper);
  }
}
