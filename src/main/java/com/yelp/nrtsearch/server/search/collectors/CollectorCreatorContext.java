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
package com.yelp.nrtsearch.server.search.collectors;

import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.LastHitInfo;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.search.SearchContext;
import java.util.Map;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;

/**
 * Context information needed to create collectors. Used for {@link DocCollector} and {@link
 * AdditionalCollectorManager} building. The search context can be accessed through {@link
 * AdditionalCollectorManager#setSearchContext(SearchContext)}
 */
public class CollectorCreatorContext {
  private final IndexState indexState;
  private final ShardState shardState;
  private final Map<String, FieldDef> queryFields;
  private final SearcherAndTaxonomy searcherAndTaxonomy;
  private final int numHitsToCollect;
  private final double timeoutSec;
  private final int timeoutCheckEvery;
  private final int terminateAfter;
  private final int terminateAfterMaxRecallCount;
  private final boolean disallowPartialResults;
  private final boolean profile;
  private final int totalHitsThreshold;
  private final LastHitInfo searchAfter;
  private final QuerySortField querySort;
  private final Map<String, String> additionalOptions;
  private final Map<String, Collector> collectors;
  private final Query query;

  /**
   * Constructor for use with SearchRequest.
   *
   * @param request search request
   * @param indexState index state (must not be null)
   * @param shardState shard state
   * @param queryFields all possible fields usable for this query
   * @param searcherAndTaxonomy searcher for query
   */
  public CollectorCreatorContext(
      SearchRequest request,
      IndexState indexState,
      ShardState shardState,
      Map<String, FieldDef> queryFields,
      SearcherAndTaxonomy searcherAndTaxonomy) {
    if (indexState == null) {
      throw new IllegalArgumentException("indexState cannot be null");
    }
    this.indexState = indexState;
    this.shardState = shardState;
    this.queryFields = queryFields;
    this.searcherAndTaxonomy = searcherAndTaxonomy;
    this.numHitsToCollect = DocCollector.computeNumHitsToCollect(request);
    this.timeoutSec =
        request.getTimeoutSec() > 0.0
            ? request.getTimeoutSec()
            : indexState.getDefaultSearchTimeoutSec();
    this.timeoutCheckEvery =
        request.getTimeoutCheckEvery() > 0
            ? request.getTimeoutCheckEvery()
            : indexState.getDefaultSearchTimeoutCheckEvery();
    this.terminateAfter =
        request.getTerminateAfter() > 0
            ? request.getTerminateAfter()
            : indexState.getDefaultTerminateAfter();
    this.terminateAfterMaxRecallCount =
        request.getTerminateAfterMaxRecallCount() > 0
            ? request.getTerminateAfterMaxRecallCount()
            : indexState.getDefaultTerminateAfterMaxRecallCount();
    this.disallowPartialResults = request.getDisallowPartialResults();
    this.profile = request.getProfile();
    this.totalHitsThreshold = request.getTotalHitsThreshold();
    this.searchAfter = request.getSearchAfter();
    this.querySort = request.getQuerySort();
    this.additionalOptions = request.getAdditionalOptionsMap();
    this.collectors = request.getCollectorsMap();
    this.query = request.getQuery();
  }

  /**
   * Constructor for retriever use without SearchRequest. This constructor is designed for use cases
   * like retrievers that only need RelevanceCollector or SortFieldCollector without additional
   * collectors.
   *
   * @param indexState index state (must not be null)
   * @param queryFields all possible fields usable for this query
   * @param searcherAndTaxonomy searcher for query
   * @param numHitsToCollect number of hits to collect
   * @param totalHitsThreshold total hits threshold for search accuracy
   * @param searchAfter search after info for pagination (null if not paginating)
   * @param querySort sort configuration (null for relevance-based search)
   */
  public CollectorCreatorContext(
      IndexState indexState,
      Map<String, FieldDef> queryFields,
      SearcherAndTaxonomy searcherAndTaxonomy,
      int numHitsToCollect,
      int totalHitsThreshold,
      LastHitInfo searchAfter,
      QuerySortField querySort) {
    if (indexState == null) {
      throw new IllegalArgumentException("indexState cannot be null");
    }
    this.indexState = indexState;
    this.shardState = null;
    this.queryFields = queryFields;
    this.searcherAndTaxonomy = searcherAndTaxonomy;
    this.numHitsToCollect = numHitsToCollect;
    this.totalHitsThreshold = totalHitsThreshold;
    this.searchAfter = searchAfter;
    this.querySort = querySort;
    // Use index state defaults for timeout/terminate settings
    this.timeoutSec = indexState.getDefaultSearchTimeoutSec();
    this.timeoutCheckEvery = indexState.getDefaultSearchTimeoutCheckEvery();
    this.terminateAfter = indexState.getDefaultTerminateAfter();
    this.terminateAfterMaxRecallCount = indexState.getDefaultTerminateAfterMaxRecallCount();
    // Retriever defaults - no profiling, no partial results restriction
    this.disallowPartialResults = false;
    this.profile = false;
    // Not used for retrievers
    this.additionalOptions = java.util.Collections.emptyMap();
    this.collectors = java.util.Collections.emptyMap();
    this.query = null;
  }

  /** Get index state */
  public IndexState getIndexState() {
    return indexState;
  }

  /** Get shard state */
  public ShardState getShardState() {
    return shardState;
  }

  /** Get all possible field usable for this query */
  public Map<String, FieldDef> getQueryFields() {
    return queryFields;
  }

  /** Get searcher for query */
  public SearcherAndTaxonomy getSearcherAndTaxonomy() {
    return searcherAndTaxonomy;
  }

  /** Get the number of hits to collect */
  public int getNumHitsToCollect() {
    return numHitsToCollect;
  }

  /** Get the timeout in seconds (resolved with defaults) */
  public double getTimeoutSec() {
    return timeoutSec;
  }

  /** Get the timeout check frequency (resolved with defaults) */
  public int getTimeoutCheckEvery() {
    return timeoutCheckEvery;
  }

  /** Get the terminate after count (resolved with defaults) */
  public int getTerminateAfter() {
    return terminateAfter;
  }

  /** Get the terminate after max recall count (resolved with defaults) */
  public int getTerminateAfterMaxRecallCount() {
    return terminateAfterMaxRecallCount;
  }

  /** Get whether partial results are disallowed */
  public boolean getDisallowPartialResults() {
    return disallowPartialResults;
  }

  /** Get whether profiling is enabled */
  public boolean getProfile() {
    return profile;
  }

  /** Get the total hits threshold */
  public int getTotalHitsThreshold() {
    return totalHitsThreshold;
  }

  /** Get the search after info for pagination */
  public LastHitInfo getSearchAfter() {
    return searchAfter;
  }

  /** Get the query sort configuration */
  public QuerySortField getQuerySort() {
    return querySort;
  }

  /** Get additional options map */
  public Map<String, String> getAdditionalOptions() {
    return additionalOptions;
  }

  /** Get collectors map */
  public Map<String, Collector> getCollectors() {
    return collectors;
  }

  /** Get query */
  public Query getQuery() {
    return query;
  }
}
