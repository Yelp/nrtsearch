/*
 * Copyright 2025 Yelp Inc.
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

import com.yelp.nrtsearch.server.facet.DrillSidewaysImpl;
import com.yelp.nrtsearch.server.facet.FacetTopDocs;
import com.yelp.nrtsearch.server.grpc.DeadlineUtils;
import com.yelp.nrtsearch.server.grpc.FacetResult;
import com.yelp.nrtsearch.server.grpc.ProfileResult;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.innerhit.InnerHitFetchTask;
import com.yelp.nrtsearch.server.monitoring.SearchResponseCollector;
import com.yelp.nrtsearch.server.query.GlobalBitSetQuery;
import com.yelp.nrtsearch.server.rescore.RescoreTask;
import com.yelp.nrtsearch.server.search.SearchContext;
import com.yelp.nrtsearch.server.search.SearchCutoffWrapper;
import com.yelp.nrtsearch.server.search.SearchRequestProcessor;
import com.yelp.nrtsearch.server.search.SearcherResult;
import com.yelp.nrtsearch.server.search.collectors.BitSetCollectorManager;
import com.yelp.nrtsearch.server.search.retrievers.RetrieverContext;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.FixedBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetrieverHandler extends SearchHandler {

  private static final Logger logger = LoggerFactory.getLogger(RetrieverHandler.class);

  public RetrieverHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public SearchResponse handle(IndexState indexState, SearchRequest searchRequest)
      throws SearchHandlerException {
    // this request may have been waiting in the grpc queue too long
    DeadlineUtils.checkDeadline("SearchHandler: start", "SEARCH");

    var diagnostics = SearchResponse.Diagnostics.newBuilder();
    diagnostics.setInitialDeadlineMs(DeadlineUtils.getDeadlineRemainingMs());

    ShardState shardState = indexState.getShard(0);

    // Index won't be started if we are currently warming
    if (!warming) {
      indexState.verifyStarted();
    }

    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    SearchContext searchContext;
    try {
      s =
          getSearcherAndTaxonomy(
              searchRequest, indexState, shardState, diagnostics, searchExecutor);

      ProfileResult.Builder profileResultBuilder = null;
      if (searchRequest.getProfile()) {
        profileResultBuilder = ProfileResult.newBuilder();
      }

      searchContext =
          SearchRequestProcessor.buildContextForRequest(
              searchRequest, indexState, shardState, s, diagnostics, profileResultBuilder, warming);

      long searchStartTime = System.nanoTime();

      // TODO: This shall be checked before and fail immediately if not
      if (searchContext.getRetrieverContexts() != null
          && !searchContext.getRetrieverContexts().isEmpty()) {
        // TODO: parallelism
        List<TopDocs> topDocsList = new ArrayList<>();
        SearcherResult searcherResult;

        boolean needJoinedResults =
            searchRequest.getCollectorsCount() > 0 || searchRequest.getFacetsCount() > 0;

        List<FixedBitSet> orFixedBitSets = null;
        List<FixedBitSet> andFixedBitSets = null;
        int maxDoc = 0;
        if (needJoinedResults) {
          orFixedBitSets = new ArrayList<>();
          andFixedBitSets = new ArrayList<>();
          maxDoc = s.searcher().getIndexReader().maxDoc();
        }

        for (RetrieverContext retrieverContext : searchContext.getRetrieverContexts()) {
          Object[] retrieverSearchResult;
          try {
            MultiCollectorManager multiCollectorManager;
            if (needJoinedResults) {
              multiCollectorManager =
                  new MultiCollectorManager(
                      new TopScoreDocCollectorManager(
                          // TODO: Verify if we shall only use topScoreDoc or Sorted here for
                          // the first Collector
                          retrieverContext.topHits(), 1000),
                      new BitSetCollectorManager(maxDoc));
            } else {
              multiCollectorManager =
                  new MultiCollectorManager(
                      new TopScoreDocCollectorManager(retrieverContext.topHits(), 1000));
            }
            retrieverSearchResult =
                s.searcher().search(retrieverContext.query(), multiCollectorManager);
            topDocsList.add((TopDocs) retrieverSearchResult[0]);
            if (retrieverContext.joinMethod() == Retriever.JoinMethod.OR
                && orFixedBitSets != null) {
              orFixedBitSets.add((FixedBitSet) retrieverSearchResult[1]);
            } else if (retrieverContext.joinMethod() == Retriever.JoinMethod.AND
                && andFixedBitSets != null) {
              andFixedBitSets.add((FixedBitSet) retrieverSearchResult[1]);
            }
          } catch (RuntimeException e) {
            SearchCutoffWrapper.CollectionTimeoutException timeoutException =
                findTimeoutException(e);
            if (timeoutException != null) {
              throw new SearchCutoffWrapper.CollectionTimeoutException(
                  timeoutException.getMessage(), e);
            }
            throw e;
          }
        }

        // TODO: create and use blender interface instead of simply addAll
        ScoreDoc[] scoreDocs =
            topDocsList.stream()
                .map(t -> t.scoreDocs)
                .flatMap(Arrays::stream)
                .toArray(ScoreDoc[]::new);
        int totalHits = scoreDocs.length;
        TopDocs topDocs =
            new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), scoreDocs);
        // TODO: retriever support rescorer? Maybe not as running rescorer for each might be too
        // expensive
        // TODO: how to properly fill the searchResult
        searcherResult = new SearcherResult(topDocs, Collections.emptyMap());

        if (needJoinedResults) {
          // TODO: do facets and collector here
          FixedBitSet mergeFixedBitSet = new FixedBitSet(maxDoc);
          for (FixedBitSet orFixedBitSet : orFixedBitSets) {
            mergeFixedBitSet.or(orFixedBitSet);
          }
          for (FixedBitSet andFixedBitSet : andFixedBitSets) {
            mergeFixedBitSet.and(andFixedBitSet);
          }
          Query matchedBitSetQuery = new GlobalBitSetQuery(mergeFixedBitSet);

          if (searchRequest.getFacetsList().isEmpty()) {
            searcherResult =
                s.searcher()
                    .search(
                        matchedBitSetQuery,
                        // TODO: We don't need topDocs here, just to fill the custom collectors
                        searchContext.getCollector().getWrappedManager());
          } else {
              //TODO: validate facets workflow
            DrillDownQuery ddq =
                SearchRequestProcessor.addDrillDowns(indexState, matchedBitSetQuery);
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
            DrillSideways.ConcurrentDrillSidewaysResult<SearcherResult>
                concurrentDrillSidewaysResult;
            try {
              concurrentDrillSidewaysResult =
                  drillS.search(ddq, searchContext.getCollector().getWrappedManager());
            } catch (RuntimeException e) {
              // Searching with DrillSideways wraps exceptions in a few layers.
              // Try to find if this was caused by a timeout, if so, re-wrap
              // so that the top level exception is the same as when not using facets.
              SearchCutoffWrapper.CollectionTimeoutException timeoutException =
                  findTimeoutException(e);
              if (timeoutException != null) {
                throw new SearchCutoffWrapper.CollectionTimeoutException(
                    timeoutException.getMessage(), e);
              }
              throw e;
            }
            // TODO: properly handle the results of DDQ
            searcherResult = concurrentDrillSidewaysResult.collectorResult;
            searchContext.getResponseBuilder().addAllFacetResult(grpcFacetResults);
            searchContext
                .getResponseBuilder()
                .addAllFacetResult(
                    FacetTopDocs.facetTopDocsSample(
                        searcherResult.getTopDocs(),
                        searchRequest.getFacetsList(),
                        indexState,
                        s.searcher(),
                        diagnostics));
          }
        }

        TopDocs hits = searcherResult.getTopDocs();

        // add results from any extra collectors
        searchContext
            .getResponseBuilder()
            .putAllCollectorResults(searcherResult.getCollectorResults());

        searchContext.getResponseBuilder().setHitTimeout(searchContext.getCollector().hadTimeout());
        searchContext
            .getResponseBuilder()
            .setTerminatedEarly(searchContext.getCollector().terminatedEarly());

        diagnostics.setFirstPassSearchTimeMs(((System.nanoTime() - searchStartTime) / 1000000.0));

        DeadlineUtils.checkDeadline("SearchHandler: post recall", diagnostics, "SEARCH");

        // add detailed timing metrics for query execution
        if (profileResultBuilder != null) {
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
                "SearchHandler: post " + rescorer.getName(), diagnostics, "SEARCH");
          }
          diagnostics.setRescoreTimeMs(((System.nanoTime() - rescoreStartTime) / 1000000.0));
        }

        long t0 = System.nanoTime();

        hits =
            getHitsFromOffset(
                hits,
                searchContext.getStartHit(),
                Math.max(
                    searchContext.getTopHits(),
                    searchContext.getHitsToLog() + searchContext.getStartHit()));

        // create Hit.Builder for each hit, and populate with lucene doc id and ranking info
        setResponseHits(searchContext, hits);

        // fill Hit.Builder with requested fields
        fetchFields(searchContext);

        // if there were extra hits for the logging, the response size needs to be reduced to match
        // the topHits
        if (searchContext.getFetchTasks().getHitsLoggerFetchTask() != null) {
          setResponseTopHits(searchContext);
        }

        SearchResponse.SearchState.Builder searchState = SearchResponse.SearchState.newBuilder();
        searchContext.getResponseBuilder().setSearchState(searchState);
        searchState.setTimestamp(searchContext.getTimestampSec());

        // Record searcher version that handled this request:
        searchState.setSearcherVersion(
            ((DirectoryReader) s.searcher().getIndexReader()).getVersion());

        // Fill in lastDoc for searchAfter:
        if (hits.scoreDocs.length != 0) {
          ScoreDoc lastHit = hits.scoreDocs[hits.scoreDocs.length - 1];
          searchState.setLastDocId(lastHit.doc);
          searchContext.getCollector().fillLastHit(searchState, lastHit);
        }
        searchContext.getResponseBuilder().setSearchState(searchState);

        diagnostics.setGetFieldsTimeMs(((System.nanoTime() - t0) / 1000000.0));

        if (searchContext.getFetchTasks().getHighlightFetchTask() != null) {
          diagnostics.setHighlightTimeMs(
              searchContext.getFetchTasks().getHighlightFetchTask().getTimeTakenMs());
        }
        if (searchContext.getFetchTasks().getHitsLoggerFetchTask() != null) {
          diagnostics.setLoggingHitsTimeMs(
              searchContext.getFetchTasks().getHitsLoggerFetchTask().getTimeTakenMs());
        }
        if (searchContext.getFetchTasks().getInnerHitFetchTaskList() != null) {
          diagnostics.putAllInnerHitsDiagnostics(
              searchContext.getFetchTasks().getInnerHitFetchTaskList().stream()
                  .collect(
                      Collectors.toMap(
                          task -> task.getInnerHitContext().getInnerHitName(),
                          InnerHitFetchTask::getDiagnostic)));
        }
        searchContext.getResponseBuilder().setDiagnostics(diagnostics);

        if (profileResultBuilder != null) {
          searchContext.getResponseBuilder().setProfileResult(profileResultBuilder);
        }
      }
    } catch (IOException | InterruptedException | ExecutionException e) {
      logger.warn(e.getMessage(), e);
      throw new SearchHandlerException(e);
    } finally {
      // NOTE: this is a little iffy, because we may not
      // have obtained this searcher from the NRTManager
      // (i.e. sometimes we pulled from
      // SearcherLifetimeManager, other times (if
      // snapshot was specified) we opened ourselves,
      // but under-the-hood all these methods just call
      // s.getIndexReader().decRef(), which is what release
      // does:
      try {
        if (s != null) {
          shardState.release(s);
        }
      } catch (IOException e) {
        logger.warn("Failed to release searcher reference previously acquired by acquire()", e);
        throw new SearchHandlerException(e);
      }
    }

    // Add searchRequest to warmer if needed
    try {
      if (!warming && indexState.getWarmer() != null) {
        indexState.getWarmer().addSearchRequest(searchRequest);
      }
    } catch (Exception e) {
      logger.error("Unable to add warming query", e);
    }

    // if we are out of time, don't bother with serialization
    DeadlineUtils.checkDeadline("SearchHandler: end", diagnostics, "SEARCH");
    SearchResponse searchResponse = searchContext.getResponseBuilder().build();
    if (!warming) {
      SearchResponseCollector.updateSearchResponseMetrics(
          searchResponse,
          searchContext.getIndexState().getName(),
          searchContext.getIndexState().getVerboseMetrics());
    }
    return searchResponse;
  }
}
