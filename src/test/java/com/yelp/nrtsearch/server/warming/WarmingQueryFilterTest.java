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
package com.yelp.nrtsearch.server.warming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.MatchAllQuery;
import com.yelp.nrtsearch.server.grpc.MultiRetrieverRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.warming.WarmingQueryFilter.Reason;
import org.junit.Test;

public class WarmingQueryFilterTest {
  private static final String INDEX = "test_index";

  private static SearchRequest termRequest(int startHit, int topHits) {
    return SearchRequest.newBuilder()
        .setIndexName(INDEX)
        .setStartHit(startHit)
        .setTopHits(topHits)
        .setQuery(Query.newBuilder().setTermQuery(TermQuery.newBuilder().setField("field")))
        .build();
  }

  @Test
  public void testIsMatchAll() {
    assertTrue(WarmingQueryFilter.isMatchAll(SearchRequest.newBuilder().build()));
    assertTrue(
        WarmingQueryFilter.isMatchAll(
            SearchRequest.newBuilder()
                .setQuery(Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder()))
                .build()));
    assertFalse(WarmingQueryFilter.isMatchAll(termRequest(0, 10)));
    assertFalse(
        WarmingQueryFilter.isMatchAll(SearchRequest.newBuilder().setQueryText("field:a").build()));
    assertFalse(
        WarmingQueryFilter.isMatchAll(
            SearchRequest.newBuilder().addKnn(KnnQuery.newBuilder().setField("vector")).build()));
    assertFalse(
        WarmingQueryFilter.isMatchAll(
            SearchRequest.newBuilder()
                .setMultiRetriever(MultiRetrieverRequest.newBuilder())
                .build()));
  }

  @Test
  public void testAcceptAll() {
    WarmingQueryFilter filter = WarmingQueryFilter.acceptAll(INDEX);
    assertNull(filter.checkCollected(SearchRequest.newBuilder().build(), 100000));
    assertNull(filter.checkCollected(termRequest(0, 100000), 100000));
    assertNull(filter.checkBackup(SearchRequest.newBuilder().build()));
  }

  @Test
  public void testFilterMatchAll() {
    WarmingQueryFilter filter = new WarmingQueryFilter(INDEX, true, 0, 0);
    assertEquals(Reason.MATCH_ALL, filter.checkCollected(SearchRequest.newBuilder().build(), 1));
    assertEquals(Reason.MATCH_ALL, filter.checkBackup(SearchRequest.newBuilder().build()));
    assertNull(filter.checkCollected(termRequest(0, 10), 1));
  }

  @Test
  public void testMaxQueryTopHits() {
    WarmingQueryFilter filter = new WarmingQueryFilter(INDEX, false, 100, 0);
    assertNull(filter.checkCollected(termRequest(0, 100), 1));
    assertEquals(Reason.TOP_HITS, filter.checkCollected(termRequest(0, 101), 1));
    // deep pagination fetches startHit + topHits documents
    assertEquals(Reason.TOP_HITS, filter.checkCollected(termRequest(95, 10), 1));
    assertEquals(Reason.TOP_HITS, filter.checkBackup(termRequest(0, 101)));
    assertNull(filter.checkBackup(termRequest(0, 100)));
  }

  @Test
  public void testMaxQueryLatency() {
    WarmingQueryFilter filter = new WarmingQueryFilter(INDEX, false, 0, 500);
    assertNull(filter.checkCollected(termRequest(0, 10), 500));
    assertEquals(Reason.LATENCY, filter.checkCollected(termRequest(0, 10), 500.1));
    // latency is unknown at backup time
    assertNull(filter.checkBackup(termRequest(0, 10)));
  }
}
