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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.SearchResponse.Diagnostics;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.facet.DrillSidewaysImpl;
import com.yelp.nrtsearch.server.luceneserver.search.SearchCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.luceneserver.search.SearchCutoffWrapper;
import com.yelp.nrtsearch.server.luceneserver.search.SearchCutoffWrapper.CollectionTimeoutException;
import com.yelp.nrtsearch.server.luceneserver.search.SearchRequestProcessor;
import com.yelp.nrtsearch.server.luceneserver.search.SearcherResult;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.ClassRule;
import org.junit.Test;

public class TimeoutTest extends ServerTestCase {
  private static final String TEST_INDEX = "test_index";
  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;
  private static final List<String> RETRIEVE_LIST =
      Arrays.asList("doc_id", "int_score", "int_field");

  private final ThreadPoolExecutor searchThreadPoolExecutor =
      new ThreadPoolExecutor(
          1,
          1,
          0,
          TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(10),
          new NamedThreadFactory("LuceneSearchExecutor"));

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public List<String> getIndices() {
    return Collections.singletonList(TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/registerFieldsTimeout.json");
  }

  @Override
  public String getExtraConfig() {
    return String.join("\n", "threadPoolConfiguration:", "  maxSearchingThreads: 1");
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    // add documents one chunk at a time to ensure multiple index segments
    List<AddDocumentRequest> requestChunk = new ArrayList<>();
    for (int id = 0; id < NUM_DOCS; ++id) {
      requestChunk.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(NUM_DOCS - id))
                      .build())
              .putFields(
                  "int_score",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "int_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "sorted_doc_values_facet_field",
                  AddDocumentRequest.MultiValuedField.newBuilder().addValue("0").build())
              .build());

      if (requestChunk.size() == SEGMENT_CHUNK) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }
  }

  private static class TimeoutWrapper<T extends Collector> extends SearchCutoffWrapper<T> {
    long currentTime = 10000;

    public TimeoutWrapper(
        CollectorManager<T, SearcherResult> in,
        double timeoutSec,
        int checkEvery,
        boolean noPartialResults,
        Runnable onTimeout) {
      super(in, timeoutSec, checkEvery, noPartialResults, onTimeout);
    }

    @Override
    public synchronized long getTimeMs() {
      // advance time on each call
      long now = currentTime;
      currentTime += 2000;
      return now;
    }
  }

  // precondition for other tests
  @Test
  public void testNumSegments() throws Exception {
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = getGlobalState().getIndex(TEST_INDEX).getShard(0).acquire();
      assertEquals(NUM_DOCS / SEGMENT_CHUNK, s.searcher.getIndexReader().leaves().size());
      for (LeafReaderContext context : s.searcher.getIndexReader().leaves()) {
        assertEquals(SEGMENT_CHUNK, context.reader().maxDoc());
      }
    } finally {
      if (s != null) {
        getGlobalState().getIndex(TEST_INDEX).getShard(0).release(s);
      }
    }
  }

  @Test
  public void testQueryNoTimeout() throws Exception {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(RETRIEVE_LIST)
            .setQuery(getQuery())
            .setTimeoutSec(1000)
            .build();

    verifyNoTimeout(request);
  }

  @Test
  public void testFacetQueryNoTimeout() throws Exception {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(RETRIEVE_LIST)
            .setQuery(getQuery())
            .setTimeoutSec(1000)
            .addFacets(getTestFacet())
            .build();

    verifyNoTimeout(request);
  }

  @Test
  public void testQueryCheckEveryNoTimeout() throws Exception {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(RETRIEVE_LIST)
            .setQuery(getQuery())
            .setTimeoutSec(1000)
            .setTimeoutCheckEvery(5)
            .build();

    verifyNoTimeout(request);
  }

  @Test
  public void testFacetQueryCheckEveryNoTimeout() throws Exception {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(RETRIEVE_LIST)
            .setQuery(getQuery())
            .setTimeoutSec(1000)
            .setTimeoutCheckEvery(5)
            .addFacets(getTestFacet())
            .build();

    verifyNoTimeout(request);
  }

  private void verifyNoTimeout(SearchRequest request) throws Exception {
    TopDocs hits =
        queryWithFunction(
            request,
            context -> {
              try {
                TopDocs topDocs = getTopDocs(context, context.getCollector().getWrappedManager());
                assertFalse(context.getCollector().hadTimeout());
                return topDocs;
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    assertEquals(NUM_DOCS, hits.totalHits.value);
    assertEquals(NUM_DOCS - 1, hits.scoreDocs[0].score, 0);
  }

  @Test
  public void testQueryPartialResults() throws Exception {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(RETRIEVE_LIST)
            .setQuery(getQuery())
            .setTimeoutSec(5)
            .build();

    verifyPartialResults(request, 20);
  }

  @Test
  public void testFacetQueryPartialResults() throws Exception {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(RETRIEVE_LIST)
            .setQuery(getQuery())
            .setTimeoutSec(5)
            .addFacets(getTestFacet())
            .build();

    verifyPartialResults(request, 20);
  }

  @Test
  public void testQueryCheckEveryPartialResults() throws Exception {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(RETRIEVE_LIST)
            .setQuery(getQuery())
            .setTimeoutSec(7)
            .setTimeoutCheckEvery(5)
            .build();

    verifyPartialResults(request, 15);
  }

  @Test
  public void testFacetQueryCheckEveryPartialResults() throws Exception {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(RETRIEVE_LIST)
            .setQuery(getQuery())
            .setTimeoutSec(7)
            .setTimeoutCheckEvery(5)
            .addFacets(getTestFacet())
            .build();

    verifyPartialResults(request, 15);
  }

  private void verifyPartialResults(SearchRequest request, int expectedHits) throws Exception {
    TopDocs hits =
        queryWithFunction(
            request,
            context -> {
              boolean[] hasTimeout = new boolean[1];
              try {
                TopDocs topDocs =
                    getTopDocs(
                        context,
                        new TimeoutWrapper(
                            new SearchCollectorManager(
                                context.getCollector(), Collections.emptyList()),
                            request.getTimeoutSec(),
                            request.getTimeoutCheckEvery(),
                            false,
                            () -> hasTimeout[0] = true));
                assertTrue(hasTimeout[0]);
                return topDocs;
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    assertEquals(expectedHits, hits.totalHits.value);
  }

  @Test(expected = CollectionTimeoutException.class)
  public void testQueryTimeoutException() throws Exception {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(RETRIEVE_LIST)
            .setQuery(getQuery())
            .setTimeoutSec(5)
            .build();

    verifyTimeoutException(request);
  }

  @Test(expected = CollectionTimeoutException.class)
  public void testFacetQueryTimeoutException() throws Exception {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(RETRIEVE_LIST)
            .setQuery(getQuery())
            .setTimeoutSec(5)
            .addFacets(getTestFacet())
            .build();

    // re-wrap the exception type if it was caused by a timeout,
    // duplicates behavior in SearchHandler
    try {
      verifyTimeoutException(request);
    } catch (RuntimeException e) {
      CollectionTimeoutException timeoutException = findTimeoutException(e);
      if (timeoutException != null) {
        throw new CollectionTimeoutException(timeoutException.getMessage(), e);
      }
      throw e;
    }
  }

  @Test(expected = CollectionTimeoutException.class)
  public void testQueryCheckEveryTimeoutException() throws Exception {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(RETRIEVE_LIST)
            .setQuery(getQuery())
            .setTimeoutSec(7)
            .setTimeoutCheckEvery(5)
            .build();

    verifyTimeoutException(request);
  }

  @Test(expected = CollectionTimeoutException.class)
  public void testFacetQueryCheckEveryTimeoutException() throws Exception {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(RETRIEVE_LIST)
            .setQuery(getQuery())
            .setTimeoutSec(7)
            .setTimeoutCheckEvery(5)
            .addFacets(getTestFacet())
            .build();

    // re-wrap the exception type if it was caused by a timeout,
    // duplicates behavior in SearchHandler
    try {
      verifyTimeoutException(request);
    } catch (RuntimeException e) {
      CollectionTimeoutException timeoutException = findTimeoutException(e);
      if (timeoutException != null) {
        throw new CollectionTimeoutException(timeoutException.getMessage(), e);
      }
      throw e;
    }
  }

  private CollectionTimeoutException findTimeoutException(Throwable e) {
    if (e instanceof CollectionTimeoutException) {
      return (CollectionTimeoutException) e;
    }
    if (e.getCause() != null) {
      return findTimeoutException(e.getCause());
    }
    return null;
  }

  private void verifyTimeoutException(SearchRequest request) throws Exception {
    queryWithFunction(
        request,
        context -> {
          boolean[] hasTimeout = new boolean[1];
          try {
            TopDocs topDocs =
                getTopDocs(
                    context,
                    new TimeoutWrapper(
                        new SearchCollectorManager(context.getCollector(), Collections.emptyList()),
                        request.getTimeoutSec(),
                        request.getTimeoutCheckEvery(),
                        true,
                        () -> hasTimeout[0] = true));
            assertTrue(hasTimeout[0]);
            return topDocs;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private TopDocs queryWithFunction(SearchRequest request, Function<SearchContext, TopDocs> func)
      throws Exception {
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    IndexState indexState = getGlobalState().getIndex(TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    try {
      s = shardState.acquire();
      SearchContext context =
          SearchRequestProcessor.buildContextForRequest(
              request, indexState, shardState, s, ProfileResult.newBuilder());
      return func.apply(context);
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  private Query getQuery() {
    return Query.newBuilder()
        .setFunctionScoreQuery(
            FunctionScoreQuery.newBuilder()
                .setScript(Script.newBuilder().setLang("js").setSource("int_score").build())
                .setQuery(
                    Query.newBuilder()
                        .setRangeQuery(
                            RangeQuery.newBuilder()
                                .setField("int_field")
                                .setLower(String.valueOf(0))
                                .setUpper(String.valueOf(NUM_DOCS))
                                .build())
                        .build())
                .build())
        .build();
  }

  private TopDocs getTopDocs(SearchContext context, CollectorManager<?, SearcherResult> manager)
      throws IOException {
    TopDocs topDocs;
    if (context.getQuery() instanceof DrillDownQuery) {
      List<FacetResult> grpcFacetResults = new ArrayList<>();
      DrillSideways drillS =
          new DrillSidewaysImpl(
              context.getSearcherAndTaxonomy().searcher,
              context.getIndexState().getFacetsConfig(),
              context.getSearcherAndTaxonomy().taxonomyReader,
              Collections.singletonList(getTestFacet()),
              context.getSearcherAndTaxonomy(),
              context.getIndexState(),
              context.getIndexState().getShard(0),
              context.getQueryFields(),
              grpcFacetResults,
              searchThreadPoolExecutor,
              Diagnostics.newBuilder());
      DrillSideways.ConcurrentDrillSidewaysResult<SearcherResult> concurrentDrillSidewaysResult =
          drillS.search((DrillDownQuery) context.getQuery(), manager);
      topDocs = concurrentDrillSidewaysResult.collectorResult.getTopDocs();
    } else {
      topDocs =
          context
              .getSearcherAndTaxonomy()
              .searcher
              .search(context.getQuery(), manager)
              .getTopDocs();
    }
    return topDocs;
  }

  private Facet getTestFacet() {
    return Facet.newBuilder().setDim("sorted_doc_values_facet_field").setTopN(10).build();
  }
}
