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

import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.ProfileResult;
import com.yelp.nrtsearch.server.grpc.Rescorer;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.search.SearchCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.luceneserver.search.SearchCutoffWrapper;
import com.yelp.nrtsearch.server.luceneserver.search.SearchStatsWrapper;
import com.yelp.nrtsearch.server.luceneserver.search.SearcherResult;
import com.yelp.nrtsearch.server.luceneserver.search.TerminateAfterWrapper;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.additional.CollectorStatsWrapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/** Abstract base for classes that manage the collection of documents when executing queries. */
public abstract class DocCollector {

  private final SearchRequest request;
  private final IndexState indexState;
  private final List<AdditionalCollectorManager<? extends Collector, ? extends CollectorResult>>
      additionalCollectors;
  private final int numHitsToCollect;
  private boolean hadTimeout = false;
  private boolean terminatedEarly = false;
  private SearchStatsWrapper<? extends Collector> statsWrapper = null;
  private List<CollectorStatsWrapper<?, ?>> collectorStatsWrappers = null;

  /**
   * Constructor
   *
   * @param context collector creation context
   * @param additionalCollectors additional collector implementations
   */
  public DocCollector(
      CollectorCreatorContext context,
      List<AdditionalCollectorManager<? extends Collector, ? extends CollectorResult>>
          additionalCollectors) {
    this.request = context.getRequest();
    this.indexState = context.getIndexState();
    this.additionalCollectors = additionalCollectors;

    // determine how many hits to collect based on request, facets and rescore window
    int collectHits = request.getTopHits();
    for (Facet facet : request.getFacetsList()) {
      int facetSample = facet.getSampleTopDocs();
      if (facetSample > 0 && facetSample > collectHits) {
        collectHits = facetSample;
      }
    }
    for (Rescorer rescorer : request.getRescorersList()) {
      int windowSize = rescorer.getWindowSize();
      if (windowSize > 0 && windowSize > collectHits) {
        collectHits = windowSize;
      }
    }
    numHitsToCollect = collectHits;
  }

  /**
   * Get the {@link CollectorManager} to use during search with any required wrapping, such as
   * timeout handling.
   */
  public CollectorManager<? extends Collector, SearcherResult> getWrappedManager() {
    List<AdditionalCollectorManager<? extends Collector, ? extends CollectorResult>> collectors =
        wrappedCollectors();
    SearchCollectorManager searchCollectorManager = new SearchCollectorManager(this, collectors);
    return wrapManager(searchCollectorManager);
  }

  /**
   * Add search profiling stats to {@link ProfileResult}, if it was enabled. Otherwise, this is a
   * noop.
   *
   * @param profileResultBuilder builder for profile results in response
   */
  public void maybeAddProfiling(ProfileResult.Builder profileResultBuilder) {
    if (statsWrapper != null) {
      statsWrapper.addProfiling(profileResultBuilder);
      if (collectorStatsWrappers != null) {
        for (CollectorStatsWrapper<?, ?> wrapper : collectorStatsWrappers) {
          wrapper.addProfiling(profileResultBuilder);
        }
      }
    }
  }

  /**
   * Get if the search operation performed with the last manager returned from {@link
   * #getWrappedManager()} timed out. This flag is reset to false on each call to {@link
   * #getWrappedManager()}.
   */
  public boolean hadTimeout() {
    return hadTimeout;
  }

  public boolean terminatedEarly() {
    return terminatedEarly;
  }

  /** Get a lucene level {@link CollectorManager} to rank document for search. */
  public abstract CollectorManager<? extends Collector, ? extends TopDocs> getManager();

  /**
   * Fill the response hit for the given {@link ScoreDoc}. This method is expected to fill the
   * ranking information (Score or Sort info) for the response {@link SearchResponse.Hit}. It is
   * expected that the lucene doc id is already set in the Hit.
   *
   * @param hitResponse hit response message
   * @param scoreDoc doc from lucene query
   */
  public abstract void fillHitRanking(SearchResponse.Hit.Builder hitResponse, ScoreDoc scoreDoc);

  /**
   * Add information on the last hit into the search response.
   *
   * @param stateBuilder state message returned in response
   * @param lastHit last hit document
   */
  public abstract void fillLastHit(
      SearchResponse.SearchState.Builder stateBuilder, ScoreDoc lastHit);

  /**
   * Get the maximum number of hits that should be collected during ranking. This value will at
   * least be as large as the query specified top hits, and may be larger if doing a facet sample
   * aggregation requiring a greater number of top docs.
   *
   * @return total top docs needed for query response and sample facets
   */
  public int getNumHitsToCollect() {
    return numHitsToCollect;
  }

  /**
   * Wrap a base {@link CollectorManager} with additional functionality, such as timeout handling.
   *
   * @param manager base manager
   * @param <C> collector type for base manager
   * @return wrapped manager, or base manager if no wrapping is required
   */
  <C extends Collector> CollectorManager<? extends Collector, SearcherResult> wrapManager(
      CollectorManager<C, SearcherResult> manager) {
    CollectorManager<? extends Collector, SearcherResult> wrapped = manager;
    double timeout =
        request.getTimeoutSec() > 0.0
            ? request.getTimeoutSec()
            : indexState.getDefaultSearchTimeoutSec();
    if (timeout > 0.0) {
      int timeoutCheckEvery =
          request.getTimeoutCheckEvery() > 0
              ? request.getTimeoutCheckEvery()
              : indexState.getDefaultSearchTimeoutCheckEvery();
      hadTimeout = false;
      wrapped =
          new SearchCutoffWrapper<>(
              wrapped,
              timeout,
              timeoutCheckEvery,
              request.getDisallowPartialResults(),
              () -> hadTimeout = true);
    }
    int terminateAfter =
        request.getTerminateAfter() > 0
            ? request.getTerminateAfter()
            : indexState.getDefaultTerminateAfter();
    if (terminateAfter > 0) {
      wrapped = new TerminateAfterWrapper<>(wrapped, terminateAfter, () -> terminatedEarly = true);
    }
    if (request.getProfile()) {
      statsWrapper = new SearchStatsWrapper<>(wrapped);
      wrapped = statsWrapper;
    }
    return wrapped;
  }

  /**
   * Produce a list of wrapped {@link AdditionalCollectorManager}, providing support for profiling
   * stats collection if needed.
   */
  List<AdditionalCollectorManager<? extends Collector, ? extends CollectorResult>>
      wrappedCollectors() {
    if (!additionalCollectors.isEmpty() && request.getProfile()) {
      List<AdditionalCollectorManager<? extends Collector, ? extends CollectorResult>>
          wrappedCollectors = new ArrayList<>(additionalCollectors.size());
      // hold the stats wrappers to fill profiling info later
      collectorStatsWrappers = new ArrayList<>(additionalCollectors.size());
      for (AdditionalCollectorManager<? extends Collector, ? extends CollectorResult> collector :
          additionalCollectors) {
        CollectorStatsWrapper<?, ?> statsWrapper = new CollectorStatsWrapper<>(collector);
        collectorStatsWrappers.add(statsWrapper);
        wrappedCollectors.add(statsWrapper);
      }
      return wrappedCollectors;
    } else {
      return additionalCollectors;
    }
  }

  /** Sets the search context in the underlying additional collectors */
  public void setSearchContext(SearchContext searchContext) {
    for (AdditionalCollectorManager<? extends Collector, ? extends CollectorResult> collector :
        additionalCollectors) {
      collector.setSearchContext(searchContext);
    }
  }
}
