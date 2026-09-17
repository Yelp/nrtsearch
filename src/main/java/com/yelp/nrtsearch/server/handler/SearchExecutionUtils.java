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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.yelp.nrtsearch.server.facet.DrillSidewaysImpl;
import com.yelp.nrtsearch.server.facet.FacetTopDocs;
import com.yelp.nrtsearch.server.grpc.DeadlineUtils;
import com.yelp.nrtsearch.server.grpc.FacetResult;
import com.yelp.nrtsearch.server.grpc.ProfileResult;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.TotalHits;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.search.FetchTasks;
import com.yelp.nrtsearch.server.search.SearchContext;
import com.yelp.nrtsearch.server.search.SearchCutoffWrapper.CollectionTimeoutException;
import com.yelp.nrtsearch.server.search.SearcherResult;
import com.yelp.nrtsearch.server.search.collectors.DocCollector;
import com.yelp.nrtsearch.server.search.multiretriever.MultiRetrieverContext;
import com.yelp.nrtsearch.server.search.multiretriever.RetrieverContext;
import com.yelp.nrtsearch.server.search.multiretriever.blender.score.BlendedScoreDoc;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/**
 * Steps of a search request that are shared by the unary {@link SearchHandler} and the streaming
 * {@link SearchStreamHandler}: running the query, running facets, blending multi-retriever results,
 * writing hits to the response, and fetching field values.
 *
 * <p>These operate on a {@link SearchContext} and hold no state of their own, so a handler can run
 * them without holding a reference to another handler.
 */
public class SearchExecutionUtils {
  private static final ExecutorService DIRECT_EXECUTOR = MoreExecutors.newDirectExecutorService();

  private SearchExecutionUtils() {}

  /** Return value of {@link #executeMultiRetriever}. */
  public record MultiRetrieverResult(
      TopDocs topDocs, boolean hadTimeout, boolean terminatedEarly) {}

  /**
   * Runs {@link org.apache.lucene.search.IndexSearcher#search} against the search context query and
   * collector, unwrapping any {@link CollectionTimeoutException} from the call stack.
   */
  public static SearcherResult executeSearch(IndexSearcher searcher, SearchContext searchContext)
      throws IOException {
    try {
      return searcher.search(
          searchContext.getQuery(), searchContext.getCollector().getWrappedManager());
    } catch (RuntimeException e) {
      CollectionTimeoutException timeoutException = findTimeoutException(e);
      if (timeoutException != null) {
        throw new CollectionTimeoutException(timeoutException.getMessage(), e);
      }
      throw e;
    }
  }

  /**
   * Builds a {@link DrillSidewaysImpl}, executes the search, writes facet results to the response
   * builder, and returns the {@link SearcherResult} from the drill sideways pass.
   *
   * @param topDocsForSample top docs to use for {@link FacetTopDocs#facetTopDocsSample}. Pass the
   *     pre-blended hits for multi-retriever queries (where ranking is already determined), or
   *     {@code null} to use the top docs produced by this search (single-retriever path).
   */
  public static SearcherResult runDrillSidewaysSearch(
      SearcherTaxonomyManager.SearcherAndTaxonomy s,
      IndexState indexState,
      ShardState shardState,
      SearchContext searchContext,
      SearchRequest searchRequest,
      SearchResponse.Diagnostics.Builder diagnostics,
      TopDocs topDocsForSample)
      throws IOException {
    DrillDownQuery ddq = (DrillDownQuery) searchContext.getQuery();
    List<FacetResult> grpcFacetResults = new ArrayList<>();
    // Run the drill sideways search on the direct executor to run subtasks in the
    // current (grpc) thread. If we use the search thread pool for this, it can cause a
    // deadlock trying to execute the dependent parallel search tasks. Since we do not
    // currently add additional drill down definitions, there will only be one drill
    // sideways task per query.
    DrillSideways drillS =
        new DrillSidewaysImpl(
            s.searcher(),
            indexState.getFacetsConfig(),
            s.taxonomyReader(),
            searchRequest.getFacetsList(),
            s,
            indexState,
            shardState,
            searchContext.getQueryFields(),
            grpcFacetResults,
            DIRECT_EXECUTOR,
            diagnostics);
    DrillSideways.ConcurrentDrillSidewaysResult<SearcherResult> drillResult;
    try {
      drillResult = drillS.search(ddq, searchContext.getCollector().getWrappedManager());
    } catch (RuntimeException e) {
      // DrillSideways wraps exceptions in a few layers; unwrap timeouts so the top-level
      // exception type is consistent with the non-facets path.
      CollectionTimeoutException timeoutException = findTimeoutException(e);
      if (timeoutException != null) {
        throw new CollectionTimeoutException(timeoutException.getMessage(), e);
      }
      throw e;
    }
    SearcherResult searcherResult = drillResult.collectorResult;
    searchContext.getResponseBuilder().addAllFacetResult(grpcFacetResults);
    searchContext
        .getResponseBuilder()
        .addAllFacetResult(
            FacetTopDocs.facetTopDocsSample(
                topDocsForSample != null ? topDocsForSample : searcherResult.getTopDocs(),
                searchRequest.getFacetsList(),
                indexState,
                s.searcher(),
                diagnostics));
    return searcherResult;
  }

  /**
   * Execute per-retriever searches in parallel, apply optional per-retriever L1 rescoring, then
   * blend the results into a single ranked TopDocs.
   *
   * @param searchExecutor executor to run the per-retriever searches on
   */
  public static MultiRetrieverResult executeMultiRetriever(
      SearchContext searchContext,
      IndexSearcher searcher,
      ExecutorService searchExecutor,
      SearchResponse.Diagnostics.Builder diagnostics,
      ProfileResult.Builder profileResultBuilder)
      throws InterruptedException {
    return executeMultiRetriever(
        searchContext,
        searcher,
        searchExecutor,
        diagnostics,
        profileResultBuilder,
        searchContext.getHitsToLog());
  }

  /**
   * Execute per-retriever searches in parallel, apply optional per-retriever L1 rescoring, then
   * blend the results into a single ranked TopDocs.
   *
   * @param searchExecutor executor to run the per-retriever searches on
   * @param hitsToLog number of hits to log to account for when sizing the blend window. Normally
   *     {@link SearchContext#getHitsToLog()}, but the query-then-fetch streaming search flow builds
   *     its context with an unbounded logging limit, so it passes the limit from the request
   *     instead.
   */
  public static MultiRetrieverResult executeMultiRetriever(
      SearchContext searchContext,
      IndexSearcher searcher,
      ExecutorService searchExecutor,
      SearchResponse.Diagnostics.Builder diagnostics,
      ProfileResult.Builder profileResultBuilder,
      int hitsToLog)
      throws InterruptedException {
    MultiRetrieverContext multiRetrieverContext = searchContext.getMultiRetrieverContext();
    LinkedHashMap<String, RetrieverContext> retrieverContexts =
        new LinkedHashMap<>(multiRetrieverContext.getRetrieverContextMap());

    record RetrieverResult(
        TopDocs topDocs,
        double searchTimeMs,
        double rescoreTimeMs,
        boolean hadTimeout,
        boolean terminatedEarly) {}

    LinkedHashMap<String, Future<RetrieverResult>> retrieverFutures = new LinkedHashMap<>();
    for (Map.Entry<String, RetrieverContext> entry : retrieverContexts.entrySet()) {
      String name = entry.getKey();
      RetrieverContext retrieverContext = entry.getValue();
      retrieverFutures.put(
          name,
          searchExecutor.submit(
              () -> {
                DocCollector docCollector = retrieverContext.getDocCollector();
                long searchStart = System.nanoTime();
                SearcherResult result =
                    searcher.search(retrieverContext.getQuery(), docCollector.getWrappedManager());
                TopDocs topDocs = result.getTopDocs();
                double searchTimeMs = (System.nanoTime() - searchStart) / 1_000_000.0;

                double rescoreTimeMs = 0;
                if (retrieverContext.getRescoreTask() != null) {
                  long rescoreStart = System.nanoTime();
                  topDocs = retrieverContext.getRescoreTask().rescore(topDocs, searchContext);
                  rescoreTimeMs = (System.nanoTime() - rescoreStart) / 1_000_000.0;
                  topDocs =
                      SearchHandler.getHitsFromOffset(topDocs, 0, retrieverContext.getTopHits());
                }
                return new RetrieverResult(
                    topDocs,
                    searchTimeMs,
                    rescoreTimeMs,
                    docCollector.hadTimeout(),
                    docCollector.terminatedEarly());
              }));
    }

    // Compute the blend window to cover rescorer windows and hits to log.
    // L2 rescorer then trims to the final topHits window.
    int blendTopHits =
        DocCollector.computeNumHitsToCollect(
            searchContext.getStartHit(),
            searchContext.getTopHits(),
            hitsToLog,
            searchContext.getRescorers());

    LinkedHashMap<String, RetrieverResult> retrieverResults = new LinkedHashMap<>();
    boolean anyRetrieverHadTimeout = false;
    boolean anyRetrieverTerminatedEarly = false;
    for (Map.Entry<String, Future<RetrieverResult>> entry : retrieverFutures.entrySet()) {
      String name = entry.getKey();
      try {
        RetrieverResult result = entry.getValue().get();
        retrieverResults.put(name, result);
        anyRetrieverHadTimeout |= result.hadTimeout();
        anyRetrieverTerminatedEarly |= result.terminatedEarly();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        throw new RuntimeException("Retriever '" + name + "' failed: " + cause.getMessage(), cause);
      }
    }

    DeadlineUtils.checkDeadline("SearchHandler: post retriever recall", diagnostics, "SEARCH");

    LinkedHashMap<String, TopDocs> retrieverTopDocs = new LinkedHashMap<>();
    retrieverResults.forEach((name, result) -> retrieverTopDocs.put(name, result.topDocs()));

    long blendStartTime = System.nanoTime();
    TopDocs blendedHits =
        multiRetrieverContext
            .getBlenderOperation()
            .blend(retrieverTopDocs, retrieverContexts, 0, blendTopHits);
    double blenderTimeMs = (System.nanoTime() - blendStartTime) / 1_000_000.0;

    // Populate per-retriever diagnostics
    SearchResponse.Diagnostics.MultiRetrieverDiagnostics.Builder multiRetrieverDiagnosticsBuilder =
        diagnostics.getMultiRetrieverDiagnosticsBuilder();
    for (Map.Entry<String, RetrieverResult> entry : retrieverResults.entrySet()) {
      String name = entry.getKey();
      RetrieverResult retrieverResult = entry.getValue();
      RetrieverContext.RetrieverType type = retrieverContexts.get(name).getRetrieverType();
      org.apache.lucene.search.TotalHits luceneTotalHits = retrieverResult.topDocs().totalHits;
      TotalHits totalHits =
          TotalHits.newBuilder()
              .setRelation(TotalHits.Relation.valueOf(luceneTotalHits.relation().name()))
              .setValue(luceneTotalHits.value())
              .build();
      SearchResponse.Diagnostics.RetrieverDiagnostics.Builder retrieverDiagBuilder;
      if (type == RetrieverContext.RetrieverType.KNN) {
        // Preserve vectorDiagnostics already set by SearchRequestProcessor, then add timing
        retrieverDiagBuilder =
            multiRetrieverDiagnosticsBuilder
                .getRetrieverDiagnosticsOrDefault(
                    name, SearchResponse.Diagnostics.RetrieverDiagnostics.getDefaultInstance())
                .toBuilder()
                .setSearchTimeMs(retrieverResult.searchTimeMs());
      } else {
        retrieverDiagBuilder =
            SearchResponse.Diagnostics.RetrieverDiagnostics.newBuilder()
                .setSearchTimeMs(retrieverResult.searchTimeMs());
      }
      if (retrieverResult.rescoreTimeMs() > 0) {
        retrieverDiagBuilder.setRescoreTimeMs(retrieverResult.rescoreTimeMs());
      }
      retrieverDiagBuilder.setTotalHits(totalHits);
      retrieverDiagBuilder.setHitTimeout(retrieverResult.hadTimeout());
      retrieverDiagBuilder.setTerminatedEarly(retrieverResult.terminatedEarly());
      multiRetrieverDiagnosticsBuilder.putRetrieverDiagnostics(name, retrieverDiagBuilder.build());
    }
    multiRetrieverDiagnosticsBuilder.setBlenderTimeMs(blenderTimeMs);

    // Add per-retriever profiling stats
    if (profileResultBuilder != null) {
      ProfileResult.MultiRetrieverProfileResult.Builder multiRetrieverProfileBuilder =
          profileResultBuilder.getMultiRetrieverProfileResultBuilder();
      for (Map.Entry<String, RetrieverContext> entry : retrieverContexts.entrySet()) {
        String name = entry.getKey();
        ProfileResult.Builder retrieverProfile =
            multiRetrieverProfileBuilder
                .getRetrieverProfileResultsOrDefault(name, ProfileResult.getDefaultInstance())
                .toBuilder();
        entry.getValue().getDocCollector().maybeAddProfiling(retrieverProfile);
        multiRetrieverProfileBuilder.putRetrieverProfileResults(name, retrieverProfile.build());
      }
    }

    return new MultiRetrieverResult(
        blendedHits, anyRetrieverHadTimeout, anyRetrieverTerminatedEarly);
  }

  /**
   * Add {@link com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder}s to the context {@link
   * SearchResponse.Builder} for each of the query hits. Populate the builders with the lucene doc
   * id and ranking info.
   *
   * @param context search context
   * @param hits hits from query
   */
  public static void setResponseHits(SearchContext context, TopDocs hits) {
    TotalHits totalHits =
        TotalHits.newBuilder()
            .setRelation(TotalHits.Relation.valueOf(hits.totalHits.relation().name()))
            .setValue(hits.totalHits.value())
            .build();
    context.getResponseBuilder().setTotalHits(totalHits);
    for (int hitIndex = 0; hitIndex < hits.scoreDocs.length; hitIndex++) {
      var hitResponse = context.getResponseBuilder().addHitsBuilder();
      ScoreDoc hit = hits.scoreDocs[hitIndex];
      hitResponse.setLuceneDocId(hit.doc);
      if (context.getMultiRetrieverContext() != null) {
        // Expose per-retriever scores.
        if (!Float.isNaN(hit.score)) {
          hitResponse.setScore(hit.score);
        }
        if (hit instanceof BlendedScoreDoc blendedHit) {
          for (Map.Entry<String, ScoreDoc> entry : blendedHit.getScoreDocs().entrySet()) {
            if (!Float.isNaN(entry.getValue().score)) {
              hitResponse.putRetrieverScores(entry.getKey(), entry.getValue().score);
            }
          }
        }
      } else {
        context.getCollector().fillHitRanking(hitResponse, hit);
      }
    }
  }

  /**
   * Fetch/compute field values for the top hits. This operation may be done in parallel, based on
   * the setting for the fetch thread pool. In addition to filling hit fields, any query {@link
   * FetchTasks.FetchTask}s are executed.
   *
   * @param searchContext search parameters
   * @throws IOException on error reading index data
   * @throws ExecutionException on error when performing parallel fetch
   * @throws InterruptedException if parallel fetch is interrupted
   */
  public static void fetchFields(SearchContext searchContext)
      throws IOException, ExecutionException, InterruptedException {
    fetchFields(searchContext, false);
  }

  /**
   * Fetch/compute field values for the top hits. This operation may be done in parallel, based on
   * the setting for the fetch thread pool. In addition to filling hit fields, any query {@link
   * FetchTasks.FetchTask}s are executed.
   *
   * @param searchContext search parameters
   * @param skipLogging if true, do not run the {@link
   *     com.yelp.nrtsearch.server.logging.HitsLoggerFetchTask} for the fetched hits. Used by the
   *     query-then-fetch streaming search flow, which fetches the union of the documents to return
   *     and the documents to log, and then logs the requested subset of them itself.
   * @throws IOException on error reading index data
   * @throws ExecutionException on error when performing parallel fetch
   * @throws InterruptedException if parallel fetch is interrupted
   */
  public static void fetchFields(SearchContext searchContext, boolean skipLogging)
      throws IOException, ExecutionException, InterruptedException {
    if (searchContext.getResponseBuilder().getHitsBuilderList().isEmpty()) {
      // call log even when there is no hits.
      // HitsLogger implementation should decide what to log or not when there is no hits.
      if (!skipLogging && searchContext.getFetchTasks().getHitsLoggerFetchTask() != null) {
        searchContext
            .getFetchTasks()
            .getHitsLoggerFetchTask()
            .processAllHits(searchContext, Collections.emptyList());
      }
      return;
    }

    // sort hits by lucene doc id
    List<Hit.Builder> hitBuilders =
        new ArrayList<>(searchContext.getResponseBuilder().getHitsBuilderList());
    hitBuilders.sort(Comparator.comparing(Hit.Builder::getLuceneDocId));

    IndexState.ParallelFetchConfig parallelFetchConfig =
        searchContext.getIndexState().getParallelFetchConfig();

    if (parallelFetchConfig.parallelFetchByField()
        && parallelFetchConfig.maxParallelism() > 1
        && searchContext.getRetrieveFields().keySet().size()
            > parallelFetchConfig.parallelFetchChunkSize()) {
      // Fetch fields in parallel

      List<LeafReaderContext> leaves =
          searchContext.getSearcherAndTaxonomy().searcher().getIndexReader().leaves();
      List<LeafReaderContext> hitIdToLeaves = new ArrayList<>();
      for (int hitIndex = 0; hitIndex < hitBuilders.size(); ++hitIndex) {
        var hitResponse = hitBuilders.get(hitIndex);
        LeafReaderContext leaf =
            leaves.get(ReaderUtil.subIndex(hitResponse.getLuceneDocId(), leaves));
        hitIdToLeaves.add(hitIndex, leaf);
      }
      List<String> fields = new ArrayList<>(searchContext.getRetrieveFields().keySet());

      // parallelism is min of maxParallelism and fields.size() / parallelFetchChunkSize
      // round up
      int parallelism =
          Math.min(
              parallelFetchConfig.maxParallelism(),
              (fields.size() + parallelFetchConfig.parallelFetchChunkSize() - 1)
                  / parallelFetchConfig.parallelFetchChunkSize());
      List<List<String>> fieldsChunks =
          Lists.partition(fields, (fields.size() + parallelism - 1) / parallelism);
      List<Future<List<Map<String, CompositeFieldValue>>>> futures = new ArrayList<>();

      // Only parallel by fields here, which should work well for doc values and virtual fields
      // For row based stored fields, we should do it by hit id.
      // Stored fields are not widely used for NRTSearch (not recommended for memory usage)
      for (List<String> fieldsChunk : fieldsChunks) {
        futures.add(
            parallelFetchConfig
                .fetchExecutor()
                .submit(
                    new SearchHandler.FillFieldsTask(
                        searchContext.getSearcherAndTaxonomy().searcher(),
                        hitIdToLeaves,
                        hitBuilders,
                        fieldsChunk,
                        searchContext)));
      }
      for (Future<List<Map<String, CompositeFieldValue>>> future : futures) {
        List<Map<String, CompositeFieldValue>> values = future.get();
        for (int hitIndex = 0; hitIndex < hitBuilders.size(); ++hitIndex) {
          var hitResponse = hitBuilders.get(hitIndex);
          hitResponse.putAllFields(values.get(hitIndex));
        }
      }

      // execute per hit fetch tasks
      for (int hitIndex = 0; hitIndex < hitBuilders.size(); ++hitIndex) {
        var hitResponse = hitBuilders.get(hitIndex);
        LeafReaderContext leaf = hitIdToLeaves.get(hitIndex);
        if (searchContext.isExplain()) {
          hitResponse.setExplain(
              searchContext
                  .getSearcherAndTaxonomy()
                  .searcher()
                  .explain(searchContext.getQuery(), hitResponse.getLuceneDocId())
                  .toString());
        }
        searchContext.getFetchTasks().processHit(searchContext, leaf, hitResponse);
      }
    } else if (!parallelFetchConfig.parallelFetchByField()
        && parallelFetchConfig.maxParallelism() > 1
        && hitBuilders.size() > parallelFetchConfig.parallelFetchChunkSize()) {
      // Fetch docs in parallel

      // parallelism is min of maxParallelism and hitsBuilder.size() / parallelFetchChunkSize
      // round up
      int parallelism =
          Math.min(
              parallelFetchConfig.maxParallelism(),
              (hitBuilders.size() + parallelFetchConfig.parallelFetchChunkSize() - 1)
                  / parallelFetchConfig.parallelFetchChunkSize());
      List<List<Hit.Builder>> docChunks =
          Lists.partition(hitBuilders, (hitBuilders.size() + parallelism - 1) / parallelism);

      // process each document chunk in parallel
      List<Future<?>> futures = new ArrayList<>();
      for (List<Hit.Builder> docChunk : docChunks) {
        futures.add(
            parallelFetchConfig
                .fetchExecutor()
                .submit(
                    new SearchHandler.FillDocsTask(
                        searchContext, docChunk, searchContext.getQuery())));
      }
      for (Future<?> future : futures) {
        future.get();
      }
      // no need to run the per hit fetch tasks here, since they were done in the FillDocsTask
    } else {
      // single threaded fetch
      SearchHandler.FillDocsTask fillDocsTask =
          new SearchHandler.FillDocsTask(searchContext, hitBuilders, searchContext.getQuery());
      fillDocsTask.run();
    }

    // execute all hits fetch tasks
    searchContext
        .getFetchTasks()
        .processAllHits(
            searchContext, searchContext.getResponseBuilder().getHitsBuilderList(), skipLogging);
  }

  /**
   * Find an instance of {@link CollectionTimeoutException} in the cause path of an exception.
   *
   * @return found exception instance or null
   */
  private static CollectionTimeoutException findTimeoutException(Throwable e) {
    if (e instanceof CollectionTimeoutException) {
      return (CollectionTimeoutException) e;
    }
    if (e.getCause() != null) {
      return findTimeoutException(e.getCause());
    }
    return null;
  }
}
