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

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.grpc.DeadlineUtils;
import com.yelp.nrtsearch.server.grpc.ProfileResult;
import com.yelp.nrtsearch.server.grpc.ReducedHitList;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.SearchState;
import com.yelp.nrtsearch.server.grpc.StreamSearchRequest;
import com.yelp.nrtsearch.server.grpc.StreamSearchResponse;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.innerhit.InnerHitFetchTask;
import com.yelp.nrtsearch.server.logging.HitsLoggerFetchTask;
import com.yelp.nrtsearch.server.monitoring.SearchResponseCollector;
import com.yelp.nrtsearch.server.rescore.RescoreTask;
import com.yelp.nrtsearch.server.search.FetchTasks;
import com.yelp.nrtsearch.server.search.SearchContext;
import com.yelp.nrtsearch.server.search.SearchRequestProcessor;
import com.yelp.nrtsearch.server.search.SearcherResult;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for the bidirectional streaming searchStream RPC, which implements the shard side of a
 * query-then-fetch (QTF) search.
 *
 * <p>The client sends a {@link SearchRequest}; the server executes recall and rescoring, then
 * responds with hit ids and ranking info but no hit fields. The client (a coordinator) merges those
 * responses across all shards into a single global ranking and sends back a {@link ReducedHitList}
 * naming the documents this shard should fetch and log. Only then does the server fetch fields and
 * invoke the {@link HitsLoggerFetchTask}, so a document is logged once globally rather than once
 * per shard.
 *
 * <p>The Lucene searcher acquired for the first message is held open until the stream ends, so both
 * phases see the same index commit and lucene doc ids remain valid as document references.
 */
public class SearchStreamHandler {
  private static final Logger logger = LoggerFactory.getLogger(SearchStreamHandler.class);

  /** Maximum number of concurrent streams, each of which pins an index commit while open. */
  static final int DEFAULT_MAX_CONCURRENT_STREAMS = 100;

  /** How long a stream may sit idle, waiting on the client, before the server closes it. */
  static final long DEFAULT_IDLE_TIMEOUT_MS = 60_000;

  private final GlobalState globalState;
  private final SearchHandler searchHandler;
  private final int maxConcurrentStreams;
  private final long idleTimeoutMs;
  private final Semaphore concurrencyLimiter;
  private final ScheduledExecutorService timeoutScheduler;

  public SearchStreamHandler(GlobalState globalState, SearchHandler searchHandler) {
    this(globalState, searchHandler, DEFAULT_MAX_CONCURRENT_STREAMS, DEFAULT_IDLE_TIMEOUT_MS);
  }

  @VisibleForTesting
  SearchStreamHandler(
      GlobalState globalState,
      SearchHandler searchHandler,
      int maxConcurrentStreams,
      long idleTimeoutMs) {
    this.globalState = globalState;
    this.searchHandler = searchHandler;
    this.maxConcurrentStreams = maxConcurrentStreams;
    this.idleTimeoutMs = idleTimeoutMs;
    this.concurrencyLimiter = new Semaphore(maxConcurrentStreams);
    AtomicInteger threadId = new AtomicInteger();
    // Idle timeouts only need to release a searcher and write a status, so a small pool is
    // enough. A slow release can still delay other sessions' timeouts, which delays cleanup but
    // does not affect any in flight request.
    this.timeoutScheduler =
        Executors.newScheduledThreadPool(
            2,
            r -> {
              Thread t = new Thread(r, "search-stream-timeout-" + threadId.getAndIncrement());
              t.setDaemon(true);
              return t;
            });
  }

  public StreamObserver<StreamSearchRequest> handle(
      StreamObserver<StreamSearchResponse> responseObserver) {
    if (!concurrencyLimiter.tryAcquire()) {
      responseObserver.onError(
          Status.RESOURCE_EXHAUSTED
              .withDescription(
                  "Maximum concurrent search streams exceeded (" + maxConcurrentStreams + ")")
              .asRuntimeException());
      return new NoOpStreamObserver<>();
    }
    SearchStreamSession session = new SearchStreamSession(responseObserver);
    // Start the idle timer as soon as the permit is taken, so that a client which opens a stream
    // and then goes silent cannot hold the permit forever. Safe to do here since grpc cannot
    // deliver a message until this observer is returned.
    session.startIdleTimeout();
    return session;
  }

  /** Number of streams currently holding a permit. */
  @VisibleForTesting
  int getActiveStreams() {
    return maxConcurrentStreams - concurrencyLimiter.availablePermits();
  }

  /** Stop the idle timeout scheduler. Any open sessions are left to be closed by grpc. */
  public void shutdown() {
    timeoutScheduler.shutdownNow();
  }

  /** The per-stream session, holding the searcher and search state across round-trips. */
  private class SearchStreamSession implements StreamObserver<StreamSearchRequest> {
    private final StreamObserver<StreamSearchResponse> responseObserver;

    private SearcherTaxonomyManager.SearcherAndTaxonomy searcher;
    private ShardState shardState;
    private IndexState indexState;
    private SearchContext searchContext;
    private SearchResponse.Diagnostics.Builder diagnostics;
    private TopDocs currentHits;
    private ScheduledFuture<?> idleTimeoutFuture;
    private boolean closed = false;

    SearchStreamSession(StreamObserver<StreamSearchResponse> responseObserver) {
      this.responseObserver = responseObserver;
    }

    @Override
    public synchronized void onNext(StreamSearchRequest request) {
      if (closed) {
        return;
      }
      cancelIdleTimeout();
      try {
        switch (request.getStreamPhaseCase()) {
          case SEARCHREQUEST -> handleSearchRequest(request.getSearchRequest());
          case REDUCEDHITLIST -> handleReducedHitList(request.getReducedHitList());
          default ->
              closeWithError(
                  Status.INVALID_ARGUMENT
                      .withDescription(
                          "StreamSearchRequest must set either searchRequest or reducedHitList")
                      .asRuntimeException());
        }
      } catch (StatusRuntimeException e) {
        logger.warn("Error processing search stream request", e);
        closeWithError(e);
      } catch (Exception e) {
        logger.warn("Error processing search stream request", e);
        closeWithError(
            Status.INTERNAL
                .withDescription("Error processing search stream request: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
      }
    }

    @Override
    public synchronized void onError(Throwable t) {
      if (closed) {
        return;
      }
      logger.warn("Search stream client error", t);
      releaseResources();
    }

    @Override
    public synchronized void onCompleted() {
      if (closed) {
        return;
      }
      releaseResources();
      responseObserver.onCompleted();
    }

    /**
     * Execute recall and rescoring for the given request and respond with hit ids and ranking info.
     * Deliberately does not fetch hit fields: field fetch is what triggers the {@link
     * HitsLoggerFetchTask}, and this shard does not yet know which documents survive the global
     * merge.
     */
    private void handleSearchRequest(SearchRequest searchRequest) throws Exception {
      // this request may have been waiting in the grpc queue too long
      DeadlineUtils.checkDeadline("SearchStreamHandler: start", "SEARCH");

      // A second search request on the same stream replaces the first, e.g. to re-query with
      // different parameters. Drop the previous searcher and its state first.
      if (searcher != null) {
        releaseSearcher();
        searchContext = null;
        currentHits = null;
      }

      setResponseCompression(searchRequest.getResponseCompression(), responseObserver);

      indexState = globalState.getIndex(searchRequest.getIndexName());
      if (indexState == null) {
        throw Status.NOT_FOUND
            .withDescription("Index " + searchRequest.getIndexName() + " not found")
            .asRuntimeException();
      }
      shardState = indexState.getShard(0);
      indexState.verifyStarted();

      diagnostics = SearchResponse.Diagnostics.newBuilder();
      diagnostics.setInitialDeadlineMs(DeadlineUtils.getDeadlineRemainingMs());

      ExecutorService searchExecutor = globalState.getSearchExecutor();
      searcher =
          SearchHandler.getSearcherAndTaxonomy(
              searchRequest, indexState, shardState, diagnostics, searchExecutor);

      ProfileResult.Builder profileResultBuilder = null;
      if (searchRequest.getProfile()) {
        profileResultBuilder = ProfileResult.newBuilder();
      }

      searchContext =
          SearchRequestProcessor.buildContextForRequest(
              searchRequest,
              indexState,
              shardState,
              searcher,
              diagnostics,
              profileResultBuilder,
              false);
      SearchResponse.Builder responseBuilder = searchContext.getResponseBuilder();

      long searchStartTime = System.nanoTime();

      TopDocs hits;
      if (searchContext.getMultiRetrieverContext() != null) {
        SearchHandler.MultiRetrieverResult multiRetrieverResult =
            searchHandler.executeMultiRetriever(
                searchContext, searcher.searcher(), diagnostics, profileResultBuilder);
        hits = multiRetrieverResult.topDocs();

        DeadlineUtils.checkDeadline(
            "SearchStreamHandler: post multi-retriever recall", diagnostics, "SEARCH");

        SearchHandler.populateRetrieverScores(hits, searchContext.getSharedDocContext());

        if (!searchRequest.getFacetsList().isEmpty()) {
          SearcherResult searcherResult =
              searchHandler.runDrillSidewaysSearch(
                  searcher,
                  indexState,
                  shardState,
                  searchContext,
                  searchRequest,
                  diagnostics,
                  hits);
          responseBuilder.putAllCollectorResults(searcherResult.getCollectorResults());
          hits = new TopDocs(searcherResult.getTopDocs().totalHits, hits.scoreDocs);
        } else if (searchRequest.getCollectorsCount() > 0) {
          SearcherResult searcherResult =
              SearchHandler.executeSearch(searcher.searcher(), searchContext);
          responseBuilder.putAllCollectorResults(searcherResult.getCollectorResults());
          hits = new TopDocs(searcherResult.getTopDocs().totalHits, hits.scoreDocs);
        }

        responseBuilder.setHitTimeout(
            multiRetrieverResult.hadTimeout() || searchContext.getCollector().hadTimeout());
        responseBuilder.setTerminatedEarly(
            multiRetrieverResult.terminatedEarly()
                || searchContext.getCollector().terminatedEarly());

        if (profileResultBuilder != null
            && (!searchRequest.getFacetsList().isEmpty()
                || searchRequest.getCollectorsCount() > 0)) {
          searchContext
              .getCollector()
              .maybeAddProfiling(
                  profileResultBuilder
                      .getMultiRetrieverProfileResultBuilder()
                      .getAggregationProfileResultBuilder());
        }
      } else {
        SearcherResult searcherResult;
        if (!searchRequest.getFacetsList().isEmpty()) {
          searcherResult =
              searchHandler.runDrillSidewaysSearch(
                  searcher,
                  indexState,
                  shardState,
                  searchContext,
                  searchRequest,
                  diagnostics,
                  null);
        } else {
          searcherResult = SearchHandler.executeSearch(searcher.searcher(), searchContext);
        }
        hits = searcherResult.getTopDocs();
        responseBuilder.putAllCollectorResults(searcherResult.getCollectorResults());
        responseBuilder.setHitTimeout(searchContext.getCollector().hadTimeout());
        responseBuilder.setTerminatedEarly(searchContext.getCollector().terminatedEarly());
      }

      diagnostics.setFirstPassSearchTimeMs(((System.nanoTime() - searchStartTime) / 1000000.0));

      DeadlineUtils.checkDeadline("SearchStreamHandler: post recall", diagnostics, "SEARCH");

      if (profileResultBuilder != null && searchContext.getMultiRetrieverContext() == null) {
        searchContext.getCollector().maybeAddProfiling(profileResultBuilder);
      }

      long rescoreStartTime = System.nanoTime();
      if (!searchContext.getRescorers().isEmpty()) {
        for (RescoreTask rescorer : searchContext.getRescorers()) {
          long startNS = System.nanoTime();
          hits = rescorer.rescore(hits, searchContext);
          long endNS = System.nanoTime();
          diagnostics.putRescorersTimeMs(rescorer.getName(), (endNS - startNS) / 1000000.0);
          DeadlineUtils.checkDeadline(
              "SearchStreamHandler: post " + rescorer.getName(), diagnostics, "SEARCH");
        }
        diagnostics.setRescoreTimeMs(((System.nanoTime() - rescoreStartTime) / 1000000.0));
      }

      // Same window the unary search would keep: enough hits for the requested page plus any
      // extras needed for logging. Using the collection window instead would inflate the
      // response to the largest rescorer window on every shard.
      hits =
          SearchHandler.getHitsFromOffset(
              hits,
              searchContext.getStartHit(),
              Math.max(
                  searchContext.getTopHits(),
                  searchContext.getHitsToLog() + searchContext.getStartHit()));
      currentHits = hits;

      // Populate hit ids and ranking info (score, or sorted field values for a sorted query) so
      // the coordinator has what it needs to merge. Hit fields are left for the fetch phase.
      SearchHandler.setResponseHits(searchContext, hits);

      SearchState.Builder searchState = SearchState.newBuilder();
      searchState.setTimestamp(searchContext.getTimestampSec());
      searchState.setSearcherVersion(
          ((DirectoryReader) searcher.searcher().getIndexReader()).getVersion());
      if (hits.scoreDocs.length != 0) {
        ScoreDoc lastHit = hits.scoreDocs[hits.scoreDocs.length - 1];
        searchState.setLastDocId(lastHit.doc);
        searchContext.getCollector().fillLastHit(searchState, lastHit);
      }
      responseBuilder.setSearchState(searchState);

      responseBuilder.setDiagnostics(diagnostics);
      if (profileResultBuilder != null) {
        responseBuilder.setProfileResult(profileResultBuilder);
      }

      addToWarmer(searchRequest);

      DeadlineUtils.checkDeadline("SearchStreamHandler: recall response", diagnostics, "SEARCH");

      responseObserver.onNext(
          StreamSearchResponse.newBuilder()
              .setPhase(StreamSearchResponse.Phase.RESCORE)
              .setSearchResponse(responseBuilder)
              .build());
      scheduleIdleTimeout();
    }

    /**
     * Fetch fields for, and log, exactly the documents the coordinator selected after its global
     * merge, then respond with the documents it asked to have returned.
     */
    private void handleReducedHitList(ReducedHitList reducedHitList) throws Exception {
      if (searchContext == null || currentHits == null) {
        closeWithError(
            Status.FAILED_PRECONDITION
                .withDescription("Must send searchRequest before reducedHitList")
                .asRuntimeException());
        return;
      }

      DeadlineUtils.checkDeadline("SearchStreamHandler: reduced hit list", diagnostics, "SEARCH");
      long fetchStartTime = System.nanoTime();

      Map<Integer, ScoreDoc> firstPassHits = new LinkedHashMap<>();
      for (ScoreDoc scoreDoc : currentHits.scoreDocs) {
        firstPassHits.put(scoreDoc.doc, scoreDoc);
      }

      List<Integer> logIds = distinctIds(reducedHitList.getLuceneDocIdsToLogList());
      List<Integer> returnIds = distinctIds(reducedHitList.getLuceneDocIdsToReturnList());
      verifyKnownDocIds(firstPassHits.keySet(), logIds, returnIds);

      // Fetch the union of both lists, ordered by the log list, which carries the coordinator's
      // globally merged ordering. Return-only ids are appended.
      LinkedHashSet<Integer> fetchIds = new LinkedHashSet<>(logIds);
      fetchIds.addAll(returnIds);
      ScoreDoc[] fetchScoreDocs =
          fetchIds.stream().map(firstPassHits::get).toArray(ScoreDoc[]::new);

      // Reuse the first pass SearchContext. Rebuilding it from the request would discard the
      // per-document data the rescorers wrote to the shared doc context (which the HitsLogger
      // reads), re-execute any knn query, and construct a second HitsLogger.
      SearchResponse.Builder responseBuilder = searchContext.getResponseBuilder();
      responseBuilder.clearHits();
      // Aggregations were already sent with the recall response; no need to repeat them.
      responseBuilder.clearFacetResult();
      responseBuilder.clearCollectorResults();
      SearchHandler.setResponseHits(
          searchContext, new TopDocs(currentHits.totalHits, fetchScoreDocs));

      // Suppress the fetch task's own logging. It would log the first hitsToLog documents of the
      // fetched set, ordered by this shard's local ranking, which the coordinator's global
      // ordering has already superseded. The logger is invoked explicitly below instead.
      FetchTasks fetchTasks = searchContext.getFetchTasks();
      HitsLoggerFetchTask hitsLoggerFetchTask = fetchTasks.getHitsLoggerFetchTask();
      fetchTasks.setHitsLoggerFetchTask(null);
      try {
        searchHandler.fetchFields(searchContext);
      } finally {
        fetchTasks.setHitsLoggerFetchTask(hitsLoggerFetchTask);
      }
      diagnostics.setGetFieldsTimeMs(((System.nanoTime() - fetchStartTime) / 1000000.0));

      Map<Integer, SearchResponse.Hit.Builder> fetchedHits = new HashMap<>();
      for (SearchResponse.Hit.Builder hitBuilder : responseBuilder.getHitsBuilderList()) {
        fetchedHits.put(hitBuilder.getLuceneDocId(), hitBuilder);
      }

      if (hitsLoggerFetchTask != null) {
        List<SearchResponse.Hit.Builder> hitsToLog = new ArrayList<>(logIds.size());
        for (int docId : logIds) {
          hitsToLog.add(fetchedHits.get(docId));
        }
        hitsLoggerFetchTask.logHits(searchContext, hitsToLog);
        diagnostics.setLoggingHitsTimeMs(hitsLoggerFetchTask.getTimeTakenMs());
      }
      if (fetchTasks.getHighlightFetchTask() != null) {
        diagnostics.setHighlightTimeMs(fetchTasks.getHighlightFetchTask().getTimeTakenMs());
      }
      if (fetchTasks.getInnerHitFetchTaskList() != null) {
        diagnostics.putAllInnerHitsDiagnostics(
            fetchTasks.getInnerHitFetchTaskList().stream()
                .collect(
                    Collectors.toMap(
                        task -> task.getInnerHitContext().getInnerHitName(),
                        InnerHitFetchTask::getDiagnostic)));
      }

      // Only the documents the coordinator asked to have returned go over the wire, in the order
      // it listed them. Documents that were fetched solely to be logged are dropped here.
      List<SearchResponse.Hit> hitsToReturn = new ArrayList<>(returnIds.size());
      for (int docId : returnIds) {
        hitsToReturn.add(fetchedHits.get(docId).build());
      }
      responseBuilder.clearHits();
      responseBuilder.addAllHits(hitsToReturn);
      responseBuilder.setDiagnostics(diagnostics);

      DeadlineUtils.checkDeadline("SearchStreamHandler: fetch response", diagnostics, "SEARCH");

      SearchResponse searchResponse = responseBuilder.build();
      SearchResponseCollector.updateSearchResponseMetrics(
          searchResponse, indexState.getName(), indexState.getVerboseMetrics());

      responseObserver.onNext(
          StreamSearchResponse.newBuilder()
              .setPhase(StreamSearchResponse.Phase.FETCH)
              .setSearchResponse(searchResponse)
              .build());
      responseObserver.onCompleted();
      releaseResources();
    }

    /**
     * Reject any doc id this shard did not return during the recall phase, so that a coordinator
     * bug surfaces as an error rather than as silently missing hits or log records.
     */
    private void verifyKnownDocIds(
        Set<Integer> knownDocIds, List<Integer> logIds, List<Integer> returnIds) {
      List<Integer> unknown = new ArrayList<>();
      for (int docId : logIds) {
        if (!knownDocIds.contains(docId)) {
          unknown.add(docId);
        }
      }
      for (int docId : returnIds) {
        if (!knownDocIds.contains(docId) && !unknown.contains(docId)) {
          unknown.add(docId);
        }
      }
      if (!unknown.isEmpty()) {
        List<Integer> sample = unknown.subList(0, Math.min(10, unknown.size()));
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "ReducedHitList contains "
                    + unknown.size()
                    + " lucene doc id(s) not returned by this shard, e.g. "
                    + sample)
            .asRuntimeException();
      }
    }

    private void addToWarmer(SearchRequest searchRequest) {
      try {
        if (indexState.getWarmer() != null) {
          indexState.getWarmer().addSearchRequest(searchRequest);
        }
      } catch (Exception e) {
        logger.error("Unable to add warming query", e);
      }
    }

    synchronized void startIdleTimeout() {
      scheduleIdleTimeout();
    }

    private void scheduleIdleTimeout() {
      idleTimeoutFuture =
          timeoutScheduler.schedule(
              () -> {
                synchronized (SearchStreamSession.this) {
                  if (!closed) {
                    logger.info("Search stream idle timeout reached, closing session");
                    closeWithError(
                        Status.DEADLINE_EXCEEDED
                            .withDescription(
                                "Search stream idle for more than " + idleTimeoutMs + "ms")
                            .asRuntimeException());
                  }
                }
              },
              idleTimeoutMs,
              TimeUnit.MILLISECONDS);
    }

    private void cancelIdleTimeout() {
      if (idleTimeoutFuture != null) {
        idleTimeoutFuture.cancel(false);
        idleTimeoutFuture = null;
      }
    }

    private void closeWithError(Throwable error) {
      if (closed) {
        return;
      }
      try {
        responseObserver.onError(error);
      } catch (Exception e) {
        logger.debug("Failed to send error to search stream client", e);
      }
      releaseResources();
    }

    private void releaseSearcher() {
      if (searcher != null && shardState != null) {
        try {
          shardState.release(searcher);
        } catch (IOException e) {
          logger.warn("Failed to release searcher reference previously acquired by acquire()", e);
        }
      }
      searcher = null;
    }

    /**
     * Release everything this session holds. Idempotent, and called from every terminal path:
     * normal completion, client error or cancellation, server error, and idle timeout.
     */
    private void releaseResources() {
      if (closed) {
        return;
      }
      closed = true;
      cancelIdleTimeout();
      releaseSearcher();
      searchContext = null;
      currentHits = null;
      concurrencyLimiter.release();
    }
  }

  private static List<Integer> distinctIds(List<Integer> docIds) {
    return docIds.stream().distinct().collect(Collectors.toList());
  }

  /**
   * Set response compression on the stream. Mirrors {@link Handler#setResponseCompression(String,
   * StreamObserver)}, which is not reachable from here since this handler is not a {@link Handler}.
   */
  private static void setResponseCompression(
      String compressionType, StreamObserver<?> responseObserver) {
    if (!compressionType.isEmpty()) {
      try {
        ((ServerCallStreamObserver<?>) responseObserver).setCompression(compressionType);
      } catch (Exception e) {
        logger.warn("Unable to set response compression to type '" + compressionType + "' : " + e);
      }
    }
  }

  private static class NoOpStreamObserver<T> implements StreamObserver<T> {
    @Override
    public void onNext(T value) {}

    @Override
    public void onError(Throwable t) {}

    @Override
    public void onCompleted() {}
  }
}
