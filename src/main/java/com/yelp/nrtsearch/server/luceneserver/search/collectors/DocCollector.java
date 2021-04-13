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

import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.ProfileResult;
import com.yelp.nrtsearch.server.grpc.Rescorer;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.search.SearchCutoffWrapper;
import com.yelp.nrtsearch.server.luceneserver.search.SearchStatsWrapper;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/** Abstract base for classes that manage the collection of documents when executing queries. */
public abstract class DocCollector {

  private final SearchRequest request;
  private final int numHitsToCollect;
  private boolean hadTimeout = false;
  private SearchStatsWrapper<? extends Collector, ? extends TopDocs> statsWrapper = null;

  public DocCollector(SearchRequest request) {
    this.request = request;

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
  public CollectorManager<? extends Collector, ? extends TopDocs> getWrappedManager() {
    return wrapManager(getManager());
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
  int getNumHitsToCollect() {
    return numHitsToCollect;
  }

  /**
   * Wrap a base {@link CollectorManager} with additional functionality, such as timeout handling.
   *
   * @param manager base manager
   * @param <C> collector type for base manager
   * @param <T> top docs type for base manager
   * @return wrapped manager, or base manager if no wrapping is required
   */
  <C extends Collector, T extends TopDocs>
      CollectorManager<? extends Collector, ? extends TopDocs> wrapManager(
          CollectorManager<C, T> manager) {
    CollectorManager<? extends Collector, ? extends TopDocs> wrapped = manager;
    if (request.getTimeoutSec() > 0.0) {
      hadTimeout = false;
      wrapped =
          new SearchCutoffWrapper<>(
              wrapped,
              request.getTimeoutSec(),
              request.getDisallowPartialResults(),
              () -> hadTimeout = true);
    }
    if (request.getProfile()) {
      statsWrapper = new SearchStatsWrapper<>(wrapped);
      wrapped = statsWrapper;
    }
    return wrapped;
  }
}
