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

import com.yelp.nrtsearch.server.analysis.AnalyzerCreator;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.Analyzer;
import com.yelp.nrtsearch.server.grpc.BooleanClause;
import com.yelp.nrtsearch.server.grpc.BooleanClause.Occur;
import com.yelp.nrtsearch.server.grpc.BooleanQuery;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.KnnRetriever;
import com.yelp.nrtsearch.server.grpc.MatchAllQuery;
import com.yelp.nrtsearch.server.grpc.MatchPhraseQuery;
import com.yelp.nrtsearch.server.grpc.MatchPhraseQuery.ZeroTerms;
import com.yelp.nrtsearch.server.grpc.MultiRetrieverRequest;
import com.yelp.nrtsearch.server.grpc.NestedQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.grpc.TextRetriever;
import com.yelp.nrtsearch.server.warming.WarmingQueryFilter.Reason;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import org.junit.BeforeClass;
import org.junit.Test;

public class WarmingQueryFilterTest {
  private static final String INDEX = "test_index";

  @BeforeClass
  public static void init() {
    NrtsearchConfig configuration =
        new NrtsearchConfig(new ByteArrayInputStream("node: node1".getBytes()));
    AnalyzerCreator.initialize(configuration, Collections.emptyList());
  }

  private static SearchRequest termRequest(int startHit, int topHits) {
    return SearchRequest.newBuilder()
        .setIndexName(INDEX)
        .setStartHit(startHit)
        .setTopHits(topHits)
        .setQuery(Query.newBuilder().setTermQuery(TermQuery.newBuilder().setField("field")))
        .build();
  }

  private static Query termQuery() {
    return Query.newBuilder().setTermQuery(TermQuery.newBuilder().setField("field")).build();
  }

  /** Match phrase query of only stop words, which the english analyzer reduces to no terms. */
  private static Query matchPhraseQuery(ZeroTerms zeroTerms) {
    return matchPhraseQuery(zeroTerms, "the");
  }

  private static Query matchPhraseQuery(ZeroTerms zeroTerms, String text) {
    return Query.newBuilder()
        .setMatchPhraseQuery(
            MatchPhraseQuery.newBuilder()
                .setField("field")
                .setQuery(text)
                .setAnalyzer(Analyzer.newBuilder().setPredefined("en.English"))
                .setZeroTermsQuery(zeroTerms))
        .build();
  }

  private static Query booleanQuery(Occur occur1, Query query1, Occur occur2, Query query2) {
    return Query.newBuilder()
        .setBooleanQuery(
            BooleanQuery.newBuilder()
                .addClauses(BooleanClause.newBuilder().setOccur(occur1).setQuery(query1))
                .addClauses(BooleanClause.newBuilder().setOccur(occur2).setQuery(query2)))
        .build();
  }

  private static SearchRequest multiRetrieverRequest(Retriever... retrievers) {
    MultiRetrieverRequest.Builder builder = MultiRetrieverRequest.newBuilder();
    for (Retriever retriever : retrievers) {
      builder.addRetrievers(retriever);
    }
    return SearchRequest.newBuilder().setIndexName(INDEX).setMultiRetriever(builder).build();
  }

  private static Retriever textRetriever(Query query, int topHits) {
    return Retriever.newBuilder()
        .setName("text")
        .setTextRetriever(TextRetriever.newBuilder().setQuery(query).setTopHits(topHits))
        .build();
  }

  private static Retriever knnRetriever(int k, int numCandidates) {
    return Retriever.newBuilder()
        .setName("knn")
        .setKnnRetriever(KnnRetriever.newBuilder().setKnnQuery(knnQuery(k, numCandidates)))
        .build();
  }

  private static KnnQuery knnQuery(int k, int numCandidates) {
    return KnnQuery.newBuilder().setField("vector").setK(k).setNumCandidates(numCandidates).build();
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
    assertNull(filter.checkCollected(SearchRequest.newBuilder().build(), 100000, null));
    assertNull(filter.checkCollected(termRequest(0, 100000), 100000, null));
    assertNull(filter.checkReplay(SearchRequest.newBuilder().build(), null));
  }

  @Test
  public void testFilterMatchAll() {
    WarmingQueryFilter filter = new WarmingQueryFilter(INDEX, true, 0, 0, 0);
    assertEquals(
        Reason.MATCH_ALL, filter.checkCollected(SearchRequest.newBuilder().build(), 1, null));
    assertEquals(Reason.MATCH_ALL, filter.checkReplay(SearchRequest.newBuilder().build(), null));
    assertNull(filter.checkCollected(termRequest(0, 10), 1, null));
  }

  @Test
  public void testMaxQueryTopHits() {
    WarmingQueryFilter filter = new WarmingQueryFilter(INDEX, false, 100, 0, 0);
    assertNull(filter.checkCollected(termRequest(0, 100), 1, null));
    assertEquals(Reason.TOP_HITS, filter.checkCollected(termRequest(0, 101), 1, null));
    // topHits already includes startHit, so a page ending at the limit is allowed
    assertNull(filter.checkCollected(termRequest(90, 100), 1, null));
    // deep pagination needs topHits beyond the limit
    assertEquals(Reason.TOP_HITS, filter.checkCollected(termRequest(95, 105), 1, null));
    assertEquals(Reason.TOP_HITS, filter.checkCollected(termRequest(101, 10), 1, null));
    assertEquals(Reason.TOP_HITS, filter.checkReplay(termRequest(0, 101), null));
    assertNull(filter.checkReplay(termRequest(0, 100), null));
  }

  @Test
  public void testMaxQueryLatency() {
    WarmingQueryFilter filter = new WarmingQueryFilter(INDEX, false, 0, 500, 0);
    assertNull(filter.checkCollected(termRequest(0, 10), 500, null));
    assertEquals(Reason.LATENCY, filter.checkCollected(termRequest(0, 10), 500.1, null));
    // latency is unknown at backup time
    assertNull(filter.checkReplay(termRequest(0, 10), null));
  }

  @Test
  public void testHasAllZeroTermsScan() {
    assertTrue(
        WarmingQueryFilter.hasAllZeroTermsScan(matchPhraseQuery(ZeroTerms.ALL_ZERO_TERMS), null));
    assertFalse(
        WarmingQueryFilter.hasAllZeroTermsScan(matchPhraseQuery(ZeroTerms.NONE_ZERO_TERMS), null));
    assertFalse(WarmingQueryFilter.hasAllZeroTermsScan(termQuery(), null));
    // terms remain after analysis, so the option has no effect
    assertFalse(
        WarmingQueryFilter.hasAllZeroTermsScan(
            matchPhraseQuery(ZeroTerms.ALL_ZERO_TERMS, "the pizza"), null));
    // no analyzer on the query and no field lookup, assume terms remain
    assertFalse(
        WarmingQueryFilter.hasAllZeroTermsScan(
            Query.newBuilder()
                .setMatchPhraseQuery(
                    MatchPhraseQuery.newBuilder()
                        .setField("field")
                        .setQuery("the")
                        .setZeroTermsQuery(ZeroTerms.ALL_ZERO_TERMS))
                .build(),
            null));
    // found anywhere in the query tree, when not restricted by another clause
    Query nested =
        Query.newBuilder()
            .setNestedQuery(
                NestedQuery.newBuilder()
                    .setPath("nested")
                    .setQuery(
                        Query.newBuilder()
                            .setBooleanQuery(
                                BooleanQuery.newBuilder()
                                    .addClauses(BooleanClause.newBuilder().setQuery(termQuery()))
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setQuery(
                                                matchPhraseQuery(ZeroTerms.ALL_ZERO_TERMS))))))
            .build();
    assertTrue(WarmingQueryFilter.hasAllZeroTermsScan(nested, null));
  }

  @Test
  public void testAllZeroTermsUnderRestrictiveClause() {
    Query zeroTerms = matchPhraseQuery(ZeroTerms.ALL_ZERO_TERMS);
    // a restrictive clause limits the scan
    assertFalse(
        WarmingQueryFilter.hasAllZeroTermsScan(
            booleanQuery(Occur.FILTER, termQuery(), Occur.MUST, zeroTerms), null));
    assertFalse(
        WarmingQueryFilter.hasAllZeroTermsScan(
            booleanQuery(Occur.MUST, termQuery(), Occur.SHOULD, zeroTerms), null));
    // a should or must not clause does not
    assertTrue(
        WarmingQueryFilter.hasAllZeroTermsScan(
            booleanQuery(Occur.SHOULD, termQuery(), Occur.MUST, zeroTerms), null));
    assertTrue(
        WarmingQueryFilter.hasAllZeroTermsScan(
            booleanQuery(Occur.MUST_NOT, termQuery(), Occur.SHOULD, zeroTerms), null));
    // a restrictive clause that is itself a full scan does not limit it
    assertTrue(
        WarmingQueryFilter.hasAllZeroTermsScan(
            booleanQuery(Occur.MUST, zeroTerms, Occur.FILTER, zeroTerms), null));
  }

  @Test
  public void testFilterAllZeroTerms() {
    WarmingQueryFilter filter = new WarmingQueryFilter(INDEX, true, 0, 0, 0);
    assertEquals(
        Reason.ALL_ZERO_TERMS,
        filter.checkCollected(
            SearchRequest.newBuilder().setQuery(matchPhraseQuery(ZeroTerms.ALL_ZERO_TERMS)).build(),
            1,
            null));
    assertNull(
        filter.checkCollected(
            SearchRequest.newBuilder()
                .setQuery(matchPhraseQuery(ZeroTerms.NONE_ZERO_TERMS))
                .build(),
            1,
            null));
    // only filtered with filterMatchAllQueries
    assertNull(
        WarmingQueryFilter.acceptAll(INDEX)
            .checkCollected(
                SearchRequest.newBuilder()
                    .setQuery(matchPhraseQuery(ZeroTerms.ALL_ZERO_TERMS))
                    .build(),
                1,
                null));
  }

  @Test
  public void testMultiRetrieverMatchAll() {
    WarmingQueryFilter filter = new WarmingQueryFilter(INDEX, true, 0, 0, 0);
    assertNull(
        filter.checkCollected(multiRetrieverRequest(textRetriever(termQuery(), 10)), 1, null));
    assertNull(filter.checkCollected(multiRetrieverRequest(knnRetriever(10, 100)), 1, null));
    assertEquals(
        Reason.MATCH_ALL,
        filter.checkCollected(
            multiRetrieverRequest(
                textRetriever(termQuery(), 10),
                textRetriever(
                    Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder()).build(), 10)),
            1,
            null));
    assertEquals(
        Reason.MATCH_ALL,
        filter.checkReplay(
            multiRetrieverRequest(textRetriever(Query.newBuilder().build(), 10)), null));
    assertEquals(
        Reason.ALL_ZERO_TERMS,
        filter.checkReplay(
            multiRetrieverRequest(
                knnRetriever(10, 100),
                textRetriever(matchPhraseQuery(ZeroTerms.ALL_ZERO_TERMS), 10)),
            null));
  }

  @Test
  public void testMultiRetrieverTopHits() {
    WarmingQueryFilter filter = new WarmingQueryFilter(INDEX, false, 100, 0, 0);
    assertNull(
        filter.checkCollected(multiRetrieverRequest(textRetriever(termQuery(), 100)), 1, null));
    assertEquals(
        Reason.TOP_HITS,
        filter.checkCollected(
            multiRetrieverRequest(textRetriever(termQuery(), 10), textRetriever(termQuery(), 101)),
            1,
            null));
    // knn k is bounded by maxVectorCandidates instead
    assertNull(filter.checkCollected(multiRetrieverRequest(knnRetriever(500, 500)), 1, null));
  }

  @Test
  public void testMaxVectorCandidates() {
    WarmingQueryFilter filter = new WarmingQueryFilter(INDEX, false, 0, 0, 1000);
    assertNull(
        filter.checkCollected(
            SearchRequest.newBuilder().addKnn(knnQuery(10, 1000)).build(), 1, null));
    assertEquals(
        Reason.VECTOR_CANDIDATES,
        filter.checkCollected(
            SearchRequest.newBuilder().addKnn(knnQuery(10, 100)).addKnn(knnQuery(10, 1001)).build(),
            1,
            null));
    assertNull(filter.checkCollected(multiRetrieverRequest(knnRetriever(10, 1000)), 1, null));
    assertEquals(
        Reason.VECTOR_CANDIDATES,
        filter.checkReplay(
            multiRetrieverRequest(textRetriever(termQuery(), 10), knnRetriever(10, 5000)), null));
    assertEquals(
        Reason.VECTOR_CANDIDATES,
        filter.checkReplay(SearchRequest.newBuilder().addKnn(knnQuery(10, 5000)).build(), null));
    // disabled by default
    assertNull(
        WarmingQueryFilter.acceptAll(INDEX)
            .checkCollected(
                SearchRequest.newBuilder().addKnn(knnQuery(10, 10000)).build(), 1, null));
  }
}
