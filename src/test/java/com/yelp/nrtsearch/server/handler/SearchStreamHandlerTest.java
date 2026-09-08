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
package com.yelp.nrtsearch.server.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.LoggingHits;
import com.yelp.nrtsearch.server.grpc.LuceneDocIdSet;
import com.yelp.nrtsearch.server.grpc.MatchAllQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.RecallRequest;
import com.yelp.nrtsearch.server.grpc.ReducedHitList;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SortFields;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.grpc.StreamSearchRequest;
import com.yelp.nrtsearch.server.grpc.StreamSearchResponse;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.logging.HitsLogger;
import com.yelp.nrtsearch.server.logging.HitsLoggerProvider;
import com.yelp.nrtsearch.server.plugins.HitsLoggerPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.search.SearchContext;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.junit.Before;
import org.junit.Test;

public class SearchStreamHandlerTest extends ServerTestCase {
  private static final String FACET_INDEX = "test_facet_index";
  private static final String FACET_FIELD = "sorted_doc_values_facet_field_single_valued";
  private static final int NUM_DOCS = 10;
  private static final long AWAIT_MS = 10_000;

  /** doc_id values passed to {@link HitsLogger#log}, one entry per invocation. */
  private static final List<List<String>> logCalls =
      Collections.synchronizedList(new ArrayList<>());

  @Override
  protected List<Plugin> getPlugins(NrtsearchConfig configuration) {
    return Collections.singletonList(new TestHitsLoggerPlugin());
  }

  @Override
  protected List<String> getIndices() {
    return List.of(DEFAULT_TEST_INDEX, FACET_INDEX);
  }

  @Override
  protected FieldDefRequest getIndexDef(String name) throws IOException {
    String resource =
        FACET_INDEX.equals(name) ? "/facet/text_field_facets.json" : "/registerFieldsBasic.json";
    return getFieldsFromResourceFile(resource).toBuilder().setIndexName(name).build();
  }

  @Override
  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    for (int i = 1; i <= NUM_DOCS; i++) {
      AddDocumentRequest.Builder doc = AddDocumentRequest.newBuilder().setIndexName(name);
      if (FACET_INDEX.equals(name)) {
        doc.putFields(
            FACET_FIELD,
            MultiValuedField.newBuilder().addValue(i % 2 == 0 ? "even" : "odd").build());
      } else {
        doc.putFields("doc_id", MultiValuedField.newBuilder().addValue(String.valueOf(i)).build())
            .putFields("vendor_name", MultiValuedField.newBuilder().addValue("vendor " + i).build())
            .putFields(
                "long_field",
                MultiValuedField.newBuilder().addValue(String.valueOf(i * 10)).build());
      }
      docs.add(doc.build());
    }
    addDocuments(docs.stream());
  }

  @Before
  public void clearLogCalls() {
    logCalls.clear();
  }

  /** Hits logger that records the doc_ids it was handed, in order. */
  public static class TestHitsLoggerPlugin extends Plugin implements HitsLoggerPlugin {
    @Override
    public Map<String, HitsLoggerProvider<? extends HitsLogger>> getHitsLoggers() {
      return Map.of("test_stream_logger", params -> new RecordingHitsLogger());
    }

    static class RecordingHitsLogger implements HitsLogger {
      @Override
      public void log(SearchContext context, List<SearchResponse.Hit.Builder> hits) {
        logCalls.add(
            hits.stream()
                .map(h -> h.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue())
                .collect(Collectors.toList()));
      }
    }
  }

  /**
   * Collects stream responses so tests can wait on them rather than sleeping. Responses are handed
   * off through a queue, which gives the test thread a happens-before edge on everything the grpc
   * thread wrote.
   */
  private static class ResponseRecorder implements StreamObserver<StreamSearchResponse> {
    private final LinkedBlockingQueue<StreamSearchResponse> responses = new LinkedBlockingQueue<>();
    private final CountDownLatch closed = new CountDownLatch(1);
    private final AtomicReference<Throwable> error = new AtomicReference<>();

    @Override
    public void onNext(StreamSearchResponse response) {
      responses.add(response);
    }

    @Override
    public void onError(Throwable t) {
      error.set(t);
      closed.countDown();
    }

    @Override
    public void onCompleted() {
      closed.countDown();
    }

    /** Wait for the next response, failing if the stream errors or goes quiet instead. */
    StreamSearchResponse awaitResponse() throws InterruptedException {
      StreamSearchResponse response = responses.poll(AWAIT_MS, TimeUnit.MILLISECONDS);
      if (response == null) {
        fail("Timed out waiting for stream response, error: " + error.get());
      }
      return response;
    }

    /** Wait for the stream to terminate, either successfully or with an error. */
    void awaitClose() throws InterruptedException {
      assertTrue(
          "Timed out waiting for stream to close", closed.await(AWAIT_MS, TimeUnit.MILLISECONDS));
    }

    StatusRuntimeException awaitError() throws InterruptedException {
      awaitClose();
      Throwable t = error.get();
      assertNotNull("Expected the stream to fail", t);
      assertTrue(
          "Expected a StatusRuntimeException, got " + t, t instanceof StatusRuntimeException);
      return (StatusRuntimeException) t;
    }

    int responseCount() {
      return responses.size();
    }
  }

  private StreamObserver<StreamSearchRequest> openStream(ResponseRecorder recorder) {
    return getGrpcServer().getStub().searchStream(recorder);
  }

  private static StreamSearchRequest recallMessage(SearchRequest request) {
    return recallMessage(request, List.of());
  }

  /** Recall request that also asks for fields on the intermediate response. */
  private static StreamSearchRequest recallMessage(
      SearchRequest request, List<String> intermediateFields) {
    return StreamSearchRequest.newBuilder()
        .setRecallRequest(
            RecallRequest.newBuilder()
                .setSearchRequest(request)
                .addAllRetrieveFields(intermediateFields)
                .build())
        .build();
  }

  private static LuceneDocIdSet docIdSet(List<Integer> docIds) {
    return LuceneDocIdSet.newBuilder().addAllLuceneDocIds(docIds).build();
  }

  /** Reduced hit list with both sets set explicitly, so that an empty one selects nothing. */
  private static StreamSearchRequest reducedMessage(List<Integer> toReturn, List<Integer> toLog) {
    return reducedMessage(
        ReducedHitList.newBuilder()
            .setLuceneDocIdsToReturn(docIdSet(toReturn))
            .setLuceneDocIdsToLog(docIdSet(toLog)));
  }

  private static StreamSearchRequest reducedMessage(ReducedHitList.Builder reducedHitList) {
    return StreamSearchRequest.newBuilder().setReducedHitList(reducedHitList).build();
  }

  private static SearchRequest.Builder basicRequest(int topHits) {
    return SearchRequest.newBuilder()
        .setIndexName(DEFAULT_TEST_INDEX)
        .setTopHits(topHits)
        .setQuery(Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder().build()))
        .addRetrieveFields("doc_id")
        .addRetrieveFields("vendor_name");
  }

  private static List<Integer> docIds(SearchResponse response) {
    return response.getHitsList().stream()
        .map(SearchResponse.Hit::getLuceneDocId)
        .collect(Collectors.toList());
  }

  private static List<String> docIdFields(SearchResponse response) {
    return response.getHitsList().stream()
        .map(h -> h.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue())
        .collect(Collectors.toList());
  }

  @Test
  public void testBasicSearchStreamFlow() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(basicRequest(NUM_DOCS).build()));

    StreamSearchResponse recallResponse = recorder.awaitResponse();
    assertEquals(StreamSearchResponse.Phase.RESCORE, recallResponse.getPhase());
    assertTrue(recallResponse.hasSearchResponse());
    SearchResponse recall = recallResponse.getSearchResponse();
    assertEquals(NUM_DOCS, recall.getHitsCount());
    assertEquals(NUM_DOCS, recall.getTotalHits().getValue());

    List<Integer> allDocIds = docIds(recall);
    List<Integer> toReturn = allDocIds.subList(0, 3);
    // The client logs more documents than it returns.
    List<Integer> toLog = allDocIds.subList(0, 5);
    stream.onNext(reducedMessage(toReturn, toLog));

    StreamSearchResponse fetchResponse = recorder.awaitResponse();
    recorder.awaitClose();
    assertEquals(StreamSearchResponse.Phase.FETCH_AND_LOG, fetchResponse.getPhase());
    SearchResponse fetch = fetchResponse.getSearchResponse();

    // Only the requested documents come back, ordered by this shard's own ranking.
    assertEquals(toReturn, docIds(fetch));
    // The shard's own total hits are preserved for the client to merge.
    assertEquals(NUM_DOCS, fetch.getTotalHits().getValue());
    for (SearchResponse.Hit hit : fetch.getHitsList()) {
      assertTrue(hit.containsFields("doc_id"));
      assertTrue(hit.containsFields("vendor_name"));
    }
  }

  @Test
  public void testRecallResponseHasRankingButNoFields() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(basicRequest(NUM_DOCS).build()));
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();

    // The point of splitting the phases: no field data is read before the client has decided
    // which documents matter. Requested retrieveFields are deliberately not populated yet.
    for (SearchResponse.Hit hit : recall.getHitsList()) {
      assertTrue(hit.getLuceneDocId() >= 0);
      assertTrue(hit.getScore() > 0);
      assertTrue("Recall phase should not fetch fields", hit.getFieldsMap().isEmpty());
    }
    // Search state is returned so the client can see which commit answered the request.
    assertTrue(recall.getSearchState().getSearcherVersion() > 0);

    stream.onCompleted();
    recorder.awaitClose();
  }

  @Test
  public void testHitsLoggerReceivesOnlyClientSelectedDocs() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(loggingRequest(NUM_DOCS, NUM_DOCS).build()));
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();
    assertEquals(NUM_DOCS, recall.getHitsCount());

    // Nothing may be logged before the client has merged all shards.
    assertTrue("Recall phase must not log hits", logCalls.isEmpty());

    List<Integer> allDocIds = docIds(recall);
    // A selection that is neither this shard's leading hits nor listed in its order, sent in the
    // order a global merge happened to produce.
    List<Integer> toLog =
        List.of(allDocIds.get(4), allDocIds.get(0), allDocIds.get(7), allDocIds.get(2));
    List<Integer> toReturn = List.of(allDocIds.get(4), allDocIds.get(0));
    stream.onNext(reducedMessage(toReturn, toLog));

    SearchResponse fetch = recorder.awaitResponse().getSearchResponse();
    recorder.awaitClose();

    assertEquals(1, logCalls.size());
    // Exactly the client's documents, ordered by this shard's ranking rather than by the order the
    // ids arrived in: the sets are unordered, and a global merge keeps each shard's own order.
    assertEquals(List.of("1", "3", "5", "8"), logCalls.get(0));
    assertEquals(List.of("1", "5"), docIdFields(fetch));
    assertTrue(fetch.getDiagnostics().getLoggingHitsTimeMs() >= 0);
  }

  @Test
  public void testLogListIsNotTruncatedToHitsToLog() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    // The unary search would log at most 2 hits. Here the client has already decided which
    // documents to log, so its list wins.
    stream.onNext(recallMessage(loggingRequest(NUM_DOCS, 2).build()));
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();

    List<Integer> allDocIds = docIds(recall);
    stream.onNext(reducedMessage(allDocIds.subList(0, 2), allDocIds.subList(0, 5)));
    recorder.awaitResponse();
    recorder.awaitClose();

    assertEquals(1, logCalls.size());
    assertEquals(List.of("1", "2", "3", "4", "5"), logCalls.get(0));
  }

  @Test
  public void testReturnOnlyDocsAreNotLogged() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(loggingRequest(NUM_DOCS, NUM_DOCS).build()));
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();

    List<Integer> allDocIds = docIds(recall);
    // Disjoint lists: documents to return that the client did not ask to have logged are
    // fetched and returned, but stay out of the logger.
    stream.onNext(reducedMessage(allDocIds.subList(0, 2), allDocIds.subList(5, 7)));
    SearchResponse fetch = recorder.awaitResponse().getSearchResponse();
    recorder.awaitClose();

    assertEquals(1, logCalls.size());
    assertEquals(List.of("6", "7"), logCalls.get(0));
    assertEquals(List.of("1", "2"), docIdFields(fetch));
  }

  @Test
  public void testNothingIsLoggedIfClientNeverFetches() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(loggingRequest(NUM_DOCS, NUM_DOCS).build()));
    recorder.awaitResponse();
    stream.onCompleted();
    recorder.awaitClose();

    assertTrue(logCalls.isEmpty());
  }

  @Test
  public void testSortedQueryReturnsSortedFieldsForMerge() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    SearchRequest request =
        basicRequest(NUM_DOCS)
            .setQuerySort(
                QuerySortField.newBuilder()
                    .setFields(
                        SortFields.newBuilder()
                            .addSortedFields(
                                SortType.newBuilder().setFieldName("long_field").setReverse(true)))
                    .build())
            .build();
    stream.onNext(recallMessage(request));
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();

    // A sorted query has no score to merge on, so the sort values have to be on the wire.
    assertEquals(NUM_DOCS, recall.getHitsCount());
    List<Long> sortValues =
        recall.getHitsList().stream()
            .map(h -> h.getSortedFieldsMap().get("long_field").getFieldValue(0).getLongValue())
            .collect(Collectors.toList());
    assertEquals(List.of(100L, 90L, 80L, 70L, 60L, 50L, 40L, 30L, 20L, 10L), sortValues);

    stream.onNext(reducedMessage(docIds(recall).subList(0, 2), List.of()));
    SearchResponse fetch = recorder.awaitResponse().getSearchResponse();
    recorder.awaitClose();
    assertEquals(List.of("10", "9"), docIdFields(fetch));
  }

  @Test
  public void testFacetsReturnedWithRecallResponse() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(FACET_INDEX)
            .setTopHits(NUM_DOCS)
            .setQuery(Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder().build()))
            .addRetrieveFields(FACET_FIELD)
            .addFacets(Facet.newBuilder().setDim(FACET_FIELD).setTopN(10).build())
            .build();
    stream.onNext(recallMessage(request));

    // Aggregations are computed during the first pass and returned with it, so the client can
    // merge them without waiting for the fetch phase.
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();
    assertEquals(1, recall.getFacetResultCount());
    assertEquals(NUM_DOCS, (long) recall.getFacetResult(0).getValue());

    stream.onNext(reducedMessage(docIds(recall).subList(0, 2), List.of()));
    SearchResponse fetch = recorder.awaitResponse().getSearchResponse();
    recorder.awaitClose();
    assertEquals(2, fetch.getHitsCount());
    // Not repeated in the fetch response, the client already has them.
    assertEquals(0, fetch.getFacetResultCount());
  }

  @Test
  public void testSecondSearchRequestReplacesFirst() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(basicRequest(NUM_DOCS).build()));
    assertEquals(NUM_DOCS, recorder.awaitResponse().getSearchResponse().getHitsCount());

    // Re-query on the same stream, e.g. after the client widens the request.
    stream.onNext(recallMessage(basicRequest(3).build()));
    SearchResponse secondRecall = recorder.awaitResponse().getSearchResponse();
    assertEquals(3, secondRecall.getHitsCount());

    stream.onNext(reducedMessage(docIds(secondRecall), List.of()));
    SearchResponse fetch = recorder.awaitResponse().getSearchResponse();
    recorder.awaitClose();
    assertEquals(3, fetch.getHitsCount());
  }

  @Test
  public void testReducedHitListWithoutSearchRequestFails() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(reducedMessage(List.of(0), List.of()));

    assertEquals(Status.FAILED_PRECONDITION.getCode(), recorder.awaitError().getStatus().getCode());
    assertEquals(0, recorder.responseCount());
  }

  @Test
  public void testUnknownDocIdIsRejected() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(basicRequest(2).build()));
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();

    // A doc id this shard never returned points at a client bug, so it must not be quietly
    // dropped.
    stream.onNext(reducedMessage(List.of(docIds(recall).get(0), 9999), List.of()));

    StatusRuntimeException error = recorder.awaitError();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), error.getStatus().getCode());
    assertTrue(error.getStatus().getDescription().contains("9999"));
  }

  @Test
  public void testDuplicateDocIdsAreRejected() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(basicRequest(3).build()));
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();
    int docId = docIds(recall).get(0);

    // Deduplicating would return fewer documents than the client asked for with nothing to
    // indicate any were dropped, so a repeated id is an error like an unknown one.
    stream.onNext(reducedMessage(List.of(docId, docId), List.of()));

    StatusRuntimeException error = recorder.awaitError();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), error.getStatus().getCode());
    assertTrue(error.getStatus().getDescription().contains("luceneDocIdsToReturn"));
  }

  @Test
  public void testDuplicateLogDocIdsAreRejected() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(loggingRequest(3, 3).build()));
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();
    int docId = docIds(recall).get(0);

    stream.onNext(reducedMessage(List.of(docId), List.of(docId, docId)));

    StatusRuntimeException error = recorder.awaitError();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), error.getStatus().getCode());
    assertTrue(error.getStatus().getDescription().contains("luceneDocIdsToLog"));
    assertTrue(logCalls.isEmpty());
  }

  @Test
  public void testStartHitIsRejected() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    // Paging belongs to the client: a per shard offset would drop each shard's own leading
    // hits before the global merge, so the merged page would not be the true global page.
    stream.onNext(recallMessage(basicRequest(3).setStartHit(2).build()));

    StatusRuntimeException error = recorder.awaitError();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), error.getStatus().getCode());
    assertTrue(error.getStatus().getDescription().contains("startHit"));
    assertEquals(0, recorder.responseCount());
  }

  @Test
  public void testHitsToLogZeroLogsNothing() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    // hitsToLog = 0 is the disable switch on the unary path, so bypassing truncation on the
    // streaming path must not turn logging back on.
    stream.onNext(recallMessage(loggingRequest(NUM_DOCS, 0).build()));
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();

    List<Integer> ids = docIds(recall).subList(0, 3);
    stream.onNext(reducedMessage(ids, ids));
    SearchResponse fetch = recorder.awaitResponse().getSearchResponse();
    recorder.awaitClose();

    assertEquals(3, fetch.getHitsCount());
    assertTrue(logCalls.isEmpty());
  }

  @Test
  public void testIdleTimeoutIsResetByEachMessage() throws Exception {
    long idleTimeoutMs = 500;
    SearchStreamHandler handler = newHandler(4, idleTimeoutMs);
    try {
      ResponseRecorder recorder = new ResponseRecorder();
      StreamObserver<StreamSearchRequest> stream = handler.handle(recorder);

      // Each phase waits less than the timeout, but the two together exceed it. The stream must
      // survive, and a timeout that already fired must not close a stream that just responded.
      Thread.sleep(idleTimeoutMs / 2);
      stream.onNext(recallMessage(basicRequest(3).build()));
      SearchResponse recall = recorder.awaitResponse().getSearchResponse();

      Thread.sleep(idleTimeoutMs / 2);
      stream.onNext(reducedMessage(docIds(recall), List.of()));
      SearchResponse fetch = recorder.awaitResponse().getSearchResponse();
      recorder.awaitClose();

      assertEquals(3, fetch.getHitsCount());
      assertNull("Stream should have completed, not timed out", recorder.error.get());
      assertEquals(0, handler.getActiveStreams());
    } finally {
      handler.shutdown();
    }
  }

  @Test
  public void testUnsetStreamPhaseIsRejected() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(StreamSearchRequest.newBuilder().build());

    assertEquals(Status.INVALID_ARGUMENT.getCode(), recorder.awaitError().getStatus().getCode());
  }

  @Test
  public void testUnknownIndexIsNotFound() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(basicRequest(1).setIndexName("no_such_index").build()));

    // The status code must survive the stream's error handling instead of collapsing to INTERNAL.
    assertEquals(Status.NOT_FOUND.getCode(), recorder.awaitError().getStatus().getCode());
  }

  @Test
  public void testSearchStreamDiagnostics() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(basicRequest(5).build()));
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();
    assertTrue(recall.getDiagnostics().getFirstPassSearchTimeMs() > 0);
    assertEquals(0, recall.getDiagnostics().getGetFieldsTimeMs(), 0.0);

    stream.onNext(reducedMessage(docIds(recall), List.of()));
    SearchResponse fetch = recorder.awaitResponse().getSearchResponse();
    recorder.awaitClose();
    assertTrue(fetch.getDiagnostics().getGetFieldsTimeMs() >= 0);
    // Timings from the first pass are carried through, so one response has the whole picture.
    assertTrue(fetch.getDiagnostics().getFirstPassSearchTimeMs() > 0);
  }

  @Test
  public void testEmptyReducedHitListReturnsNoHits() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(basicRequest(5).build()));
    recorder.awaitResponse();

    // Both sets are present but empty, which selects nothing. An absent set would instead mean
    // "keep this shard's recall result as it is".
    stream.onNext(reducedMessage(List.of(), List.of()));
    SearchResponse fetch = recorder.awaitResponse().getSearchResponse();
    recorder.awaitClose();
    assertEquals(0, fetch.getHitsCount());
  }

  @Test
  public void testUnsetDocIdSetsKeepRecallResult() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(loggingRequest(5, 2).build()));
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();
    assertEquals(5, recall.getHitsCount());

    // Neither set is present, so this shard falls back to what the unary search RPC would have
    // done: fetch and return every recalled hit, and log the top hitsToLog of its own ranking.
    stream.onNext(reducedMessage(ReducedHitList.newBuilder()));
    SearchResponse fetch = recorder.awaitResponse().getSearchResponse();
    recorder.awaitClose();

    assertEquals(docIds(recall), docIds(fetch));
    assertEquals(1, logCalls.size());
    assertEquals(List.of("1", "2"), logCalls.get(0));
  }

  @Test
  public void testUnsetLogSetLogsThisShardsTopHits() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(loggingRequest(NUM_DOCS, 3).build()));
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();

    // The client selected the documents to return but left the logging decision to the shard.
    List<Integer> toReturn = docIds(recall).subList(5, 8);
    stream.onNext(
        reducedMessage(ReducedHitList.newBuilder().setLuceneDocIdsToReturn(docIdSet(toReturn))));
    SearchResponse fetch = recorder.awaitResponse().getSearchResponse();
    recorder.awaitClose();

    assertEquals(List.of("6", "7", "8"), docIdFields(fetch));
    assertEquals(1, logCalls.size());
    assertEquals(List.of("1", "2", "3"), logCalls.get(0));
  }

  @Test
  public void testUnsetReturnSetReturnsEveryRecalledHit() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(loggingRequest(4, 4).build()));
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();

    // Only the logging decision was reduced; the client still wants all of this shard's hits.
    stream.onNext(
        reducedMessage(
            ReducedHitList.newBuilder()
                .setLuceneDocIdsToLog(docIdSet(docIds(recall).subList(2, 4)))));
    SearchResponse fetch = recorder.awaitResponse().getSearchResponse();
    recorder.awaitClose();

    assertEquals(List.of("1", "2", "3", "4"), docIdFields(fetch));
    assertEquals(1, logCalls.size());
    assertEquals(List.of("3", "4"), logCalls.get(0));
  }

  @Test
  public void testRecallRequestFieldsAreFilledOnRecallResponse() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    // The client needs the primary key to deduplicate across shards, but nothing else yet.
    stream.onNext(recallMessage(basicRequest(3).build(), List.of("doc_id")));
    SearchResponse recall = recorder.awaitResponse().getSearchResponse();

    assertEquals(List.of("1", "2", "3"), docIdFields(recall));
    for (SearchResponse.Hit hit : recall.getHitsList()) {
      assertEquals(Set.of("doc_id"), hit.getFieldsMap().keySet());
    }

    // The request retrieveFields still only show up on the fetch response.
    stream.onNext(reducedMessage(docIds(recall).subList(0, 2), List.of()));
    SearchResponse fetch = recorder.awaitResponse().getSearchResponse();
    recorder.awaitClose();
    for (SearchResponse.Hit hit : fetch.getHitsList()) {
      assertEquals(Set.of("doc_id", "vendor_name"), hit.getFieldsMap().keySet());
    }
  }

  @Test
  public void testUnknownRecallRequestFieldIsRejected() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(basicRequest(3).build(), List.of("not_a_field")));

    StatusRuntimeException error = recorder.awaitError();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), error.getStatus().getCode());
    assertTrue(error.getStatus().getDescription().contains("not_a_field"));
  }

  @Test
  public void testClientCompletesWithoutFetch() throws Exception {
    ResponseRecorder recorder = new ResponseRecorder();
    StreamObserver<StreamSearchRequest> stream = openStream(recorder);

    stream.onNext(recallMessage(basicRequest(5).build()));
    recorder.awaitResponse();

    stream.onCompleted();
    recorder.awaitClose();
    assertEquals(0, recorder.responseCount());
  }

  // Tests below drive a handler instance directly, so that the resource limits and the searcher
  // lifecycle can be exercised without going through the shared test server.

  @Test
  public void testConcurrencyLimitIsEnforcedAndPermitsAreReturned() throws Exception {
    SearchStreamHandler handler = newHandler(1, SearchStreamHandler.DEFAULT_IDLE_TIMEOUT_MS);
    try {
      ResponseRecorder firstRecorder = new ResponseRecorder();
      StreamObserver<StreamSearchRequest> first = handler.handle(firstRecorder);
      first.onNext(recallMessage(basicRequest(5).build()));
      firstRecorder.awaitResponse();
      assertEquals(1, handler.getActiveStreams());

      ResponseRecorder secondRecorder = new ResponseRecorder();
      handler.handle(secondRecorder);
      assertEquals(
          Status.RESOURCE_EXHAUSTED.getCode(), secondRecorder.awaitError().getStatus().getCode());

      // Finishing the first stream frees the permit for the next client.
      first.onCompleted();
      firstRecorder.awaitClose();
      assertEquals(0, handler.getActiveStreams());

      ResponseRecorder thirdRecorder = new ResponseRecorder();
      StreamObserver<StreamSearchRequest> third = handler.handle(thirdRecorder);
      third.onNext(recallMessage(basicRequest(5).build()));
      thirdRecorder.awaitResponse();
      third.onCompleted();
      thirdRecorder.awaitClose();
      assertEquals(0, handler.getActiveStreams());
    } finally {
      handler.shutdown();
    }
  }

  @Test
  public void testIdleStreamTimesOutBeforeSendingAnything() throws Exception {
    SearchStreamHandler handler = newHandler(4, 150);
    try {
      ResponseRecorder recorder = new ResponseRecorder();
      handler.handle(recorder);

      // A client that opens a stream and never speaks must not hold its permit forever.
      assertEquals(Status.DEADLINE_EXCEEDED.getCode(), recorder.awaitError().getStatus().getCode());
      assertEquals(0, handler.getActiveStreams());
    } finally {
      handler.shutdown();
    }
  }

  @Test
  public void testIdleStreamTimesOutWaitingForReducedHitList() throws Exception {
    SearchStreamHandler handler = newHandler(4, 150);
    try {
      int refCountBefore = readerRefCount(DEFAULT_TEST_INDEX);
      ResponseRecorder recorder = new ResponseRecorder();
      StreamObserver<StreamSearchRequest> stream = handler.handle(recorder);
      stream.onNext(recallMessage(basicRequest(5).build()));
      recorder.awaitResponse();

      assertEquals(Status.DEADLINE_EXCEEDED.getCode(), recorder.awaitError().getStatus().getCode());
      assertEquals(0, handler.getActiveStreams());
      // The searcher the abandoned stream was holding is released, not leaked.
      assertEquals(refCountBefore, readerRefCount(DEFAULT_TEST_INDEX));
    } finally {
      handler.shutdown();
    }
  }

  @Test
  public void testSearcherIsReleasedOnEveryTerminalPath() throws Exception {
    SearchStreamHandler handler = newHandler(4, SearchStreamHandler.DEFAULT_IDLE_TIMEOUT_MS);
    try {
      int refCountBefore = readerRefCount(DEFAULT_TEST_INDEX);

      // Full two phase flow.
      ResponseRecorder completed = new ResponseRecorder();
      StreamObserver<StreamSearchRequest> stream = handler.handle(completed);
      stream.onNext(recallMessage(basicRequest(5).build()));
      SearchResponse recall = completed.awaitResponse().getSearchResponse();
      stream.onNext(reducedMessage(docIds(recall).subList(0, 2), List.of()));
      completed.awaitResponse();
      completed.awaitClose();
      assertEquals(refCountBefore, readerRefCount(DEFAULT_TEST_INDEX));

      // Client hangs up after the recall phase.
      ResponseRecorder abandoned = new ResponseRecorder();
      StreamObserver<StreamSearchRequest> abandonedStream = handler.handle(abandoned);
      abandonedStream.onNext(recallMessage(basicRequest(5).build()));
      abandoned.awaitResponse();
      abandonedStream.onCompleted();
      abandoned.awaitClose();
      assertEquals(refCountBefore, readerRefCount(DEFAULT_TEST_INDEX));

      // Client cancels the stream.
      ResponseRecorder cancelled = new ResponseRecorder();
      StreamObserver<StreamSearchRequest> cancelledStream = handler.handle(cancelled);
      cancelledStream.onNext(recallMessage(basicRequest(5).build()));
      cancelled.awaitResponse();
      cancelledStream.onError(new RuntimeException("client went away"));
      assertEquals(refCountBefore, readerRefCount(DEFAULT_TEST_INDEX));

      // Re-query, which releases the first searcher and acquires another.
      ResponseRecorder requeried = new ResponseRecorder();
      StreamObserver<StreamSearchRequest> requeriedStream = handler.handle(requeried);
      requeriedStream.onNext(recallMessage(basicRequest(5).build()));
      requeried.awaitResponse();
      requeriedStream.onNext(recallMessage(basicRequest(3).build()));
      requeried.awaitResponse();
      requeriedStream.onCompleted();
      requeried.awaitClose();
      assertEquals(refCountBefore, readerRefCount(DEFAULT_TEST_INDEX));

      // Server side failure.
      ResponseRecorder failed = new ResponseRecorder();
      StreamObserver<StreamSearchRequest> failedStream = handler.handle(failed);
      failedStream.onNext(recallMessage(basicRequest(5).build()));
      SearchResponse failedRecall = failed.awaitResponse().getSearchResponse();
      failedStream.onNext(reducedMessage(List.of(9999), List.of()));
      assertEquals(Status.INVALID_ARGUMENT.getCode(), failed.awaitError().getStatus().getCode());
      assertFalse(failedRecall.getHitsList().isEmpty());
      assertEquals(refCountBefore, readerRefCount(DEFAULT_TEST_INDEX));

      assertEquals(0, handler.getActiveStreams());
    } finally {
      handler.shutdown();
    }
  }

  private SearchStreamHandler newHandler(int maxConcurrentStreams, long idleTimeoutMs) {
    return new SearchStreamHandler(getGlobalState(), maxConcurrentStreams, idleTimeoutMs);
  }

  /**
   * Reference count of the index reader, as seen while holding one reference of our own. A stream
   * that fails to release its searcher shows up as a higher count.
   */
  private int readerRefCount(String indexName) throws IOException {
    ShardState shardState = getGlobalState().getIndex(indexName).getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy searcher = shardState.acquire();
    try {
      return searcher.searcher().getIndexReader().getRefCount();
    } finally {
      shardState.release(searcher);
    }
  }

  private static SearchRequest.Builder loggingRequest(int topHits, int hitsToLog) {
    return basicRequest(topHits)
        .setLoggingHits(
            LoggingHits.newBuilder().setName("test_stream_logger").setHitsToLog(hitsToLog).build());
  }
}
