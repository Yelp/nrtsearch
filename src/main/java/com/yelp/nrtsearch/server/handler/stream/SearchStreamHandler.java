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
package com.yelp.nrtsearch.server.handler.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.grpc.DeadlineUtils;
import com.yelp.nrtsearch.server.grpc.LuceneDocIdSet;
import com.yelp.nrtsearch.server.grpc.ProfileResult;
import com.yelp.nrtsearch.server.grpc.RankingRequest;
import com.yelp.nrtsearch.server.grpc.ReducedHitList;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.SearchState;
import com.yelp.nrtsearch.server.grpc.StreamSearchRequest;
import com.yelp.nrtsearch.server.grpc.StreamSearchResponse;
import com.yelp.nrtsearch.server.handler.SearchExecutionUtils;
import com.yelp.nrtsearch.server.handler.SearchHandler;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.innerhit.InnerHitFetchTask;
import com.yelp.nrtsearch.server.logging.HitsLoggerFetchTask;
import com.yelp.nrtsearch.server.monitoring.SearchResponseCollector;
import com.yelp.nrtsearch.server.rescore.RescoreTask;
import com.yelp.nrtsearch.server.search.FetchTasks;
import com.yelp.nrtsearch.server.search.FieldFetchContext;
import com.yelp.nrtsearch.server.search.SearchContext;
import com.yelp.nrtsearch.server.search.SearchRequestProcessor;
import com.yelp.nrtsearch.server.search.SearcherResult;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for the bidirectional streaming searchStream RPC, which implements the shard side of a
 * query-then-fetch (QTF) search.
 *
 * <p>The client sends a {@link RankingRequest}; the server executes the ranking phase, search and
 * rescoring, then responds with hit ids, ranking info and any intermediate fields the client asked
 * for, but no {@link SearchRequest#getRetrieveFieldsList() retrieveFields}. The client merges those
 * responses across all shards into a single global ranking and sends back a {@link ReducedHitList}
 * naming the documents this shard should fetch and log. Only then does the server fetch fields and
 * invoke the {@link HitsLoggerFetchTask}, so a document is logged once globally rather than once
 * per shard.
 *
 * <p>The Lucene searcher acquired for the first message is held open until the stream ends, so both
 * phases see the same index commit and lucene doc ids remain valid as document references.
 */
public class SearchStreamHandler
    extends BidiStreamHandler<StreamSearchRequest, StreamSearchResponse, SearchStreamState> {
  private static final Logger logger = LoggerFactory.getLogger(SearchStreamHandler.class);

  /** Maximum number of concurrent streams, each of which pins an index commit while open. */
  static final int DEFAULT_MAX_CONCURRENT_STREAMS = 100;

  /** How long a stream may sit idle, waiting on the client, before the server closes it. */
  static final long DEFAULT_IDLE_TIMEOUT_MS = 60_000;

  private static final int TIMEOUT_THREADS = 2;

  private final GlobalState globalState;

  public SearchStreamHandler(GlobalState globalState) {
    this(globalState, DEFAULT_MAX_CONCURRENT_STREAMS, DEFAULT_IDLE_TIMEOUT_MS);
  }

  @VisibleForTesting
  SearchStreamHandler(GlobalState globalState, int maxConcurrentStreams, long idleTimeoutMs) {
    super("search-stream", maxConcurrentStreams, idleTimeoutMs, TIMEOUT_THREADS);
    this.globalState = globalState;
  }

  @Override
  protected SearchStreamSession newSession(StreamObserver<StreamSearchResponse> responseObserver) {
    return new SearchStreamSession(responseObserver);
  }

  @Override
  protected SearchStreamState newSessionState() {
    return new SearchStreamState();
  }

  /**
   * The per-stream session. Holds no search state of its own: what the ranking phase leaves for the
   * fetch phase lives in {@link SearchStreamState}, reached through {@link Session#sessionState()}.
   */
  private class SearchStreamSession extends Session {
    SearchStreamSession(StreamObserver<StreamSearchResponse> responseObserver) {
      super(responseObserver);
    }

    @Override
    protected synchronized void handleNext(StreamSearchRequest request) {
      if (closed) {
        return;
      }
      cancelIdleTimeout();
      try {
        switch (request.getStreamPhaseCase()) {
          case RANKINGREQUEST -> {
            handleRankingRequest(request.getRankingRequest());
            // More messages are expected, so re-arm the timer. Doing it here rather than inside the
            // phase keeps the wait for the client the only thing the timeout measures: the response
            // is already on the wire, so the ranking phase's own duration is not charged to it.
            scheduleIdleTimeout();
          }
          case REDUCEDHITLIST -> {
            handleReducedHitList(request.getReducedHitList());
            // The fetch phase is terminal, so there is nothing left to wait for. Release before
            // closing, for the same reason as closeWithError.
            releaseResources();
            responseObserver.onCompleted();
          }
          default ->
              throw Status.INVALID_ARGUMENT
                  .withDescription(
                      "StreamSearchRequest must set either rankingRequest or reducedHitList")
                  .asRuntimeException();
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
    protected synchronized void handleError(Throwable t) {
      if (closed) {
        return;
      }
      logger.warn("Search stream client error", t);
      releaseResources();
    }

    @Override
    protected synchronized void handleCompleted() {
      if (closed) {
        return;
      }
      releaseResources();
      responseObserver.onCompleted();
    }

    /**
     * Execute the ranking phase, search and rescoring, for the given request and respond with hit
     * ids, ranking info and any {@link RankingRequest#getIntermediateRetrieveFieldsList()}.
     * Deliberately does not fetch the request {@link SearchRequest#getRetrieveFieldsList()}: this
     * shard does not yet know which documents survive the global merge, so fetching them would do
     * the work this RPC exists to avoid, and running the {@link HitsLoggerFetchTask} here would log
     * each shard's local ranking.
     */
    private void handleRankingRequest(RankingRequest rankingRequest) throws Exception {
      SearchStreamState state = sessionState();
      // this request may have been waiting in the grpc queue too long
      DeadlineUtils.checkDeadline("SearchStreamHandler: start", "SEARCH");

      SearchRequest searchRequest = rankingRequest.getSearchRequest();

      // Paging is the client's job in query-then-fetch: it applies the offset to the merged global
      // ranking. Honoring startHit here would make each shard discard its own leading hits before
      // the merge, so the merged page would not be the true global page.
      if (searchRequest.getStartHit() != 0) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "startHit must be 0 for searchStream; apply the offset to the merged ranking "
                    + "instead, since a per shard offset does not compose across shards")
            .asRuntimeException();
      }

      // A second ranking request on the same stream replaces the first, e.g. to re-query with
      // different parameters. Drop the previous searcher and its state first.
      if (state.searchContext != null) {
        releaseSearcher();
        state.searchContext = null;
        state.currentHits = null;
      }

      setResponseCompression(searchRequest.getResponseCompression());

      IndexState indexState = globalState.getIndex(searchRequest.getIndexName());
      if (indexState == null) {
        throw Status.NOT_FOUND
            .withDescription("Index " + searchRequest.getIndexName() + " not found")
            .asRuntimeException();
      }
      ShardState shardState = indexState.getShard(0);
      try {
        indexState.verifyStarted();
      } catch (IllegalStateException e) {
        // Would otherwise reach the client as INTERNAL, which reads as a server bug rather than an
        // index that has not been started.
        throw Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException();
      }

      SearchResponse.Diagnostics.Builder diagnostics = SearchResponse.Diagnostics.newBuilder();
      diagnostics.setInitialDeadlineMs(DeadlineUtils.getDeadlineRemainingMs());

      ExecutorService searchExecutor = globalState.getSearchExecutor();
      SearcherAndTaxonomy searcher =
          SearchHandler.getSearcherAndTaxonomy(
              searchRequest, indexState, shardState, diagnostics, searchExecutor);

      ProfileResult.Builder profileResultBuilder = null;
      if (searchRequest.getProfile()) {
        profileResultBuilder = ProfileResult.newBuilder();
      }

      state.requestHitsToLog = searchRequest.getLoggingHits().getHitsToLog();
      try {
        // Build with an unbounded logging limit: the documents to log are chosen by the client's
        // global merge, so this shard's local hitsToLog must not truncate them. The window of hits
        // this shard ranks and returns is still sized from the request value, below.
        state.searchContext =
            SearchRequestProcessor.buildContextForRequest(
                searchRequest,
                indexState,
                shardState,
                searcher,
                diagnostics,
                profileResultBuilder,
                false,
                true);
      } catch (Throwable t) {
        // Nothing holds the searcher yet, so release it here rather than leaking the index commit
        // until the stream ends.
        SearchStreamHandler.releaseSearcher(searcher, shardState);
        throw t;
      }
      SearchResponse.Builder responseBuilder = state.searchContext.getResponseBuilder();

      long searchStartTime = System.nanoTime();

      TopDocs hits;
      if (state.searchContext.getMultiRetrieverContext() != null) {
        SearchExecutionUtils.MultiRetrieverResult multiRetrieverResult =
            SearchExecutionUtils.executeMultiRetriever(
                state.searchContext,
                searcher.searcher(),
                searchExecutor,
                diagnostics,
                profileResultBuilder,
                state.requestHitsToLog);
        hits = multiRetrieverResult.topDocs();

        DeadlineUtils.checkDeadline(
            "SearchStreamHandler: post multi-retriever ranking", diagnostics, "SEARCH");

        SearchHandler.populateRetrieverScores(hits, state.searchContext.getSharedDocContext());

        if (!searchRequest.getFacetsList().isEmpty()) {
          SearcherResult searcherResult =
              SearchExecutionUtils.runDrillSidewaysSearch(
                  searcher,
                  indexState,
                  shardState,
                  state.searchContext,
                  searchRequest,
                  diagnostics,
                  hits);
          responseBuilder.putAllCollectorResults(searcherResult.getCollectorResults());
          hits = new TopDocs(searcherResult.getTopDocs().totalHits, hits.scoreDocs);
        } else if (searchRequest.getCollectorsCount() > 0) {
          SearcherResult searcherResult =
              SearchExecutionUtils.executeSearch(searcher.searcher(), state.searchContext);
          responseBuilder.putAllCollectorResults(searcherResult.getCollectorResults());
          hits = new TopDocs(searcherResult.getTopDocs().totalHits, hits.scoreDocs);
        }

        responseBuilder.setHitTimeout(
            multiRetrieverResult.hadTimeout() || state.searchContext.getCollector().hadTimeout());
        responseBuilder.setTerminatedEarly(
            multiRetrieverResult.terminatedEarly()
                || state.searchContext.getCollector().terminatedEarly());

        if (profileResultBuilder != null
            && (!searchRequest.getFacetsList().isEmpty()
                || searchRequest.getCollectorsCount() > 0)) {
          state
              .searchContext
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
              SearchExecutionUtils.runDrillSidewaysSearch(
                  searcher,
                  indexState,
                  shardState,
                  state.searchContext,
                  searchRequest,
                  diagnostics,
                  null);
        } else {
          searcherResult =
              SearchExecutionUtils.executeSearch(searcher.searcher(), state.searchContext);
        }
        hits = searcherResult.getTopDocs();
        responseBuilder.putAllCollectorResults(searcherResult.getCollectorResults());
        responseBuilder.setHitTimeout(state.searchContext.getCollector().hadTimeout());
        responseBuilder.setTerminatedEarly(state.searchContext.getCollector().terminatedEarly());
      }

      diagnostics.setFirstPassSearchTimeMs(((System.nanoTime() - searchStartTime) / 1000000.0));

      DeadlineUtils.checkDeadline("SearchStreamHandler: post ranking", diagnostics, "SEARCH");

      if (profileResultBuilder != null && state.searchContext.getMultiRetrieverContext() == null) {
        state.searchContext.getCollector().maybeAddProfiling(profileResultBuilder);
      }

      long rescoreStartTime = System.nanoTime();
      if (!state.searchContext.getRescorers().isEmpty()) {
        for (RescoreTask rescorer : state.searchContext.getRescorers()) {
          long startNS = System.nanoTime();
          hits = rescorer.rescore(hits, state.searchContext);
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
              state.searchContext.getStartHit(),
              Math.max(
                  state.searchContext.getTopHits(),
                  state.requestHitsToLog + state.searchContext.getStartHit()));
      state.currentHits = hits;

      // Populate hit ids and ranking info (score, or sorted field values for a sorted query) so
      // the client has what it needs to merge. Request retrieveFields are left for the fetch phase.
      SearchExecutionUtils.setResponseHits(state.searchContext, hits);
      fillIntermediateFields(rankingRequest.getIntermediateRetrieveFieldsList());

      SearchState.Builder searchState = SearchState.newBuilder();
      searchState.setTimestamp(state.searchContext.getTimestampSec());
      searchState.setSearcherVersion(
          ((DirectoryReader) searcher.searcher().getIndexReader()).getVersion());
      if (hits.scoreDocs.length != 0) {
        ScoreDoc lastHit = hits.scoreDocs[hits.scoreDocs.length - 1];
        searchState.setLastDocId(lastHit.doc);
        state.searchContext.getCollector().fillLastHit(searchState, lastHit);
      }
      responseBuilder.setSearchState(searchState);

      responseBuilder.setDiagnostics(diagnostics);
      if (profileResultBuilder != null) {
        responseBuilder.setProfileResult(profileResultBuilder);
      }

      addToWarmer(searchRequest);

      DeadlineUtils.checkDeadline("SearchStreamHandler: ranking response", diagnostics, "SEARCH");

      responseObserver.onNext(
          StreamSearchResponse.newBuilder()
              .setPhase(StreamSearchResponse.Phase.RESCORE)
              .setSearchResponse(responseBuilder)
              .build());
    }

    /**
     * Fill the fields the client needs on the ranking phase response, typically just the primary
     * key it deduplicates on. Uses a fetch context of its own so that only these fields are filled,
     * and so that no query fetch task runs: those belong to the fetch phase, which sees the final
     * field set and the globally selected documents.
     */
    private void fillIntermediateFields(List<String> intermediateRetrieveFields) {
      SearchStreamState state = sessionState();
      if (intermediateRetrieveFields.isEmpty()) {
        return;
      }
      Map<String, FieldDef> fields;
      try {
        fields =
            SearchRequestProcessor.getRetrieveFields(
                intermediateRetrieveFields, state.searchContext.getQueryFields());
      } catch (IllegalArgumentException e) {
        throw Status.INVALID_ARGUMENT
            .withDescription("RankingRequest intermediateRetrieveFields: " + e.getMessage())
            .asRuntimeException();
      }
      List<SearchResponse.Hit.Builder> hitBuilders =
          new ArrayList<>(state.searchContext.getResponseBuilder().getHitsBuilderList());
      if (hitBuilders.isEmpty()) {
        return;
      }
      // FillDocsTask groups hits by lucene segment, so it needs them in doc id order
      hitBuilders.sort(Comparator.comparingInt(SearchResponse.Hit.Builder::getLuceneDocId));
      new SearchHandler.FillDocsTask(
              new IntermediateFieldFetchContext(state.searchContext, fields), hitBuilders)
          .run();
    }

    /**
     * Fetch fields for, and log, exactly the documents the client selected after its global merge,
     * then respond with the documents it asked to have returned.
     */
    private void handleReducedHitList(ReducedHitList reducedHitList) throws Exception {
      SearchStreamState state = sessionState();
      if (state.searchContext == null || state.currentHits == null) {
        throw Status.FAILED_PRECONDITION
            .withDescription("Must send rankingRequest before reducedHitList")
            .asRuntimeException();
      }

      SearchResponse.Builder responseBuilder = state.searchContext.getResponseBuilder();
      SearchResponse.Diagnostics.Builder diagnostics = responseBuilder.getDiagnosticsBuilder();
      DeadlineUtils.checkDeadline("SearchStreamHandler: reduced hit list", diagnostics, "SEARCH");
      long fetchStartTime = System.nanoTime();

      Set<Integer> rankedDocIds = new LinkedHashSet<>(state.currentHits.scoreDocs.length);
      for (ScoreDoc scoreDoc : state.currentHits.scoreDocs) {
        rankedDocIds.add(scoreDoc.doc);
      }

      // An unset set keeps this shard's ranking phase result as it is; an empty one selects
      // nothing.
      Set<Integer> returnIds =
          reducedHitList.hasLuceneDocIdsToReturn()
              ? toDocIdSet(reducedHitList.getLuceneDocIdsToReturn(), "luceneDocIdsToReturn")
              : rankedDocIds;
      Set<Integer> logIds;
      if (reducedHitList.hasLuceneDocIdsToLog()) {
        logIds = toDocIdSet(reducedHitList.getLuceneDocIdsToLog(), "luceneDocIdsToLog");
      } else {
        // Fall back to what the unary search RPC would have logged: the top hitsToLog documents of
        // this shard's own ranking.
        logIds = new LinkedHashSet<>();
        for (ScoreDoc scoreDoc : state.currentHits.scoreDocs) {
          if (logIds.size() >= state.requestHitsToLog) {
            break;
          }
          logIds.add(scoreDoc.doc);
        }
      }
      verifyKnownDocIds(rankedDocIds, logIds, returnIds);

      // Fetch the union of both sets. The client sends sets rather than lists, so order comes from
      // this shard's own ranking, which a global merge preserves within a shard: walk the ranking
      // and keep whatever the client selected, rather than iterating the union itself.
      Set<Integer> fetchIds = Sets.union(logIds, returnIds);
      List<ScoreDoc> fetchScoreDocs = new ArrayList<>(fetchIds.size());
      List<Integer> orderedLogIds = new ArrayList<>(logIds.size());
      List<Integer> orderedReturnIds = new ArrayList<>(returnIds.size());
      for (ScoreDoc scoreDoc : state.currentHits.scoreDocs) {
        if (!fetchIds.contains(scoreDoc.doc)) {
          continue;
        }
        if (logIds.contains(scoreDoc.doc)) {
          orderedLogIds.add(scoreDoc.doc);
        }
        if (returnIds.contains(scoreDoc.doc)) {
          orderedReturnIds.add(scoreDoc.doc);
        }
        fetchScoreDocs.add(scoreDoc);
      }

      // Reuse the ranking phase SearchContext. Rebuilding it from the request would discard the
      // per-document data the rescorers wrote to the shared doc context (which the HitsLogger
      // reads), re-execute any knn query, and construct a second HitsLogger.
      responseBuilder.clearHits();
      // Aggregations were already sent with the ranking response; no need to repeat them.
      responseBuilder.clearFacetResult();
      responseBuilder.clearCollectorResults();
      SearchExecutionUtils.setResponseHits(
          state.searchContext,
          new TopDocs(state.currentHits.totalHits, fetchScoreDocs.toArray(new ScoreDoc[0])));

      // Fetch with the fetch task's own logging pass skipped: it would log the leading documents of
      // the whole fetched set, which also holds the documents fetched only to be logged. The logger
      // is invoked explicitly below, with exactly the set the client selected.
      FetchTasks fetchTasks = state.searchContext.getFetchTasks();
      SearchExecutionUtils.fetchFields(state.searchContext, true);
      diagnostics.setGetFieldsTimeMs(((System.nanoTime() - fetchStartTime) / 1000000.0));

      Map<Integer, SearchResponse.Hit.Builder> fetchedHits = new HashMap<>();
      for (SearchResponse.Hit.Builder hitBuilder : responseBuilder.getHitsBuilderList()) {
        fetchedHits.put(hitBuilder.getLuceneDocId(), hitBuilder);
      }

      HitsLoggerFetchTask hitsLoggerFetchTask = fetchTasks.getHitsLoggerFetchTask();
      if (hitsLoggerFetchTask != null
          // Call the logger with an empty list only when the search had no hits at all, matching
          // the unary flow: a plugin may want to record that. A client that selects nothing out of
          // a non empty ranking result is asking for nothing to be logged.
          && (!orderedLogIds.isEmpty() || state.currentHits.scoreDocs.length == 0)) {
        List<SearchResponse.Hit.Builder> hitsToLog = new ArrayList<>(orderedLogIds.size());
        for (int docId : orderedLogIds) {
          hitsToLog.add(fetchedHits.get(docId));
        }
        hitsLoggerFetchTask.processAllHits(state.searchContext, hitsToLog);
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

      // Only the documents the client asked to have returned go over the wire. Documents that were
      // fetched solely to be logged are dropped here.
      List<SearchResponse.Hit> hitsToReturn = new ArrayList<>(orderedReturnIds.size());
      for (int docId : orderedReturnIds) {
        hitsToReturn.add(fetchedHits.get(docId).build());
      }
      responseBuilder.clearHits();
      responseBuilder.addAllHits(hitsToReturn);

      DeadlineUtils.checkDeadline("SearchStreamHandler: fetch response", diagnostics, "SEARCH");

      SearchResponse searchResponse = responseBuilder.build();
      recordMetrics(searchResponse);

      responseObserver.onNext(
          StreamSearchResponse.newBuilder()
              .setPhase(StreamSearchResponse.Phase.FETCH_AND_LOG)
              .setSearchResponse(searchResponse)
              .build());
    }

    /**
     * Reject any doc id this shard did not return during the ranking phase, so that a client bug
     * surfaces as an error rather than as silently missing hits or log records.
     */
    private void verifyKnownDocIds(
        Set<Integer> knownDocIds, Set<Integer> logIds, Set<Integer> returnIds) {
      Set<Integer> unknown = Sets.difference(Sets.union(logIds, returnIds), knownDocIds);
      if (!unknown.isEmpty()) {
        List<Integer> sample = unknown.stream().limit(10).toList();
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
      SearchStreamState state = sessionState();
      try {
        IndexState indexState = state.searchContext.getIndexState();
        if (indexState.getWarmer() != null) {
          indexState.getWarmer().addSearchRequest(searchRequest);
        }
      } catch (Exception e) {
        logger.error("Unable to add warming query", e);
      }
    }

    private void releaseSearcher() {
      SearchStreamState state = sessionState();
      if (state.searchContext != null) {
        SearchStreamHandler.releaseSearcher(
            state.searchContext.getSearcherAndTaxonomy(), state.searchContext.getShardState());
      }
    }

    /**
     * Record response metrics for this stream, at most once. Called on the fetch response, which
     * carries the richest diagnostics, and again from {@link #releaseResources()} so that a stream
     * which ran a search but never reached fetch is still counted.
     */
    private void recordMetrics(SearchResponse searchResponse) {
      SearchStreamState state = sessionState();
      if (state.metricsRecorded || state.searchContext == null) {
        return;
      }
      state.metricsRecorded = true;
      IndexState indexState = state.searchContext.getIndexState();
      try {
        SearchResponseCollector.updateSearchResponseMetrics(
            searchResponse, indexState.getName(), indexState.getVerboseMetrics());
      } catch (Exception e) {
        logger.warn("Failed to record search stream response metrics", e);
      }
    }

    /**
     * Release everything this session holds. Idempotent, and called from every terminal path:
     * normal completion, client error or cancellation, server error, and idle timeout.
     */
    @Override
    protected synchronized void releaseResources() {
      if (closed) {
        return;
      }
      closed = true;
      SearchStreamState state = sessionState();
      // A stream that ranked but never fetched (idle timeout, client cancel, or a client that
      // dropped this shard) still did the search work, and those are exactly the cases worth
      // seeing on a dashboard. Report what the ranking phase produced.
      if (!state.metricsRecorded && state.searchContext != null) {
        recordMetrics(state.searchContext.getResponseBuilder().build());
      }
      cancelIdleTimeout();
      releaseSearcher();
      state.searchContext = null;
      state.currentHits = null;
      releasePermit();
    }
  }

  /**
   * Fetch context used to fill the intermediate fields of a ranking response. Carries its own field
   * set and an empty {@link FetchTasks}, leaving the query fetch tasks for the fetch phase.
   */
  private record IntermediateFieldFetchContext(
      SearchContext searchContext, Map<String, FieldDef> retrieveFields)
      implements FieldFetchContext {
    private static final FetchTasks NO_FETCH_TASKS = new FetchTasks(List.of());

    @Override
    public SearcherAndTaxonomy getSearcherAndTaxonomy() {
      return searchContext.getSearcherAndTaxonomy();
    }

    @Override
    public Map<String, FieldDef> getRetrieveFields() {
      return retrieveFields;
    }

    @Override
    public FetchTasks getFetchTasks() {
      return NO_FETCH_TASKS;
    }

    @Override
    public SearchContext getSearchContext() {
      return searchContext;
    }

    @Override
    public boolean isExplain() {
      return false;
    }
  }

  /**
   * Convert a client provided doc id set to a java set, rejecting a repeated id. Silently
   * deduplicating would hide a client bug that is likely to have dropped ids elsewhere too, and the
   * repeat itself has no meaning: the shard fetches and logs each document once.
   */
  private static Set<Integer> toDocIdSet(LuceneDocIdSet docIdSet, String fieldName) {
    List<Integer> docIds = docIdSet.getLuceneDocIdsList();
    Set<Integer> result = new LinkedHashSet<>(docIds.size());
    for (int docId : docIds) {
      if (!result.add(docId)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(fieldName + " contains duplicate lucene doc id " + docId)
            .asRuntimeException();
      }
    }
    return result;
  }

  /** Release a searcher reference previously acquired by {@code acquire()}. */
  private static void releaseSearcher(SearcherAndTaxonomy searcher, ShardState shardState) {
    if (searcher != null && shardState != null) {
      try {
        shardState.release(searcher);
      } catch (IOException e) {
        logger.warn("Failed to release searcher reference previously acquired by acquire()", e);
      }
    }
  }
}
