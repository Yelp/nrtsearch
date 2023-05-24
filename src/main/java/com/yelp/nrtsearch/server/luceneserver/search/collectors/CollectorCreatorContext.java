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

import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.Rescorer;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import java.util.List;
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
  private final Query query;
  private Map<String, Collector> collectorsMap;
  private int totalHitsThreshold;
  private int topHits;
  private List<Facet> facetsList;
  private List<Rescorer> rescorersList;
  private double timeoutSec;
  private int timeoutCheckEvery;
  private int terminateAfter;
  private boolean profile;
  private boolean disallowPartialResults;
  private QuerySortField querySort;

  public CollectorCreatorContext(
      SearchRequest searchRequest,
      IndexState indexState,
      ShardState shardState,
      Map<String, FieldDef> queryFields,
      SearcherAndTaxonomy searcherAndTaxonomy) {
    this(
        indexState,
        shardState,
        queryFields,
        searcherAndTaxonomy,
        searchRequest.hasQuery() ? searchRequest.getQuery() : null,
        searchRequest.getCollectorsMap(),
        searchRequest.getTotalHitsThreshold(),
        searchRequest.getTopHits(),
        searchRequest.getFacetsList(),
        searchRequest.getRescorersList(),
        searchRequest.getTimeoutSec(),
        searchRequest.getTimeoutCheckEvery(),
        searchRequest.getTerminateAfter(),
        searchRequest.getProfile(),
        searchRequest.getDisallowPartialResults(),
        searchRequest.getQuerySort());
  }

  /**
   * Constructor.
   *
   * @param indexState index state
   * @param shardState shard state
   * @param queryFields all possible fields usable for this query
   * @param searcherAndTaxonomy searcher for query
   * @param query the query to search
   * @param collectorsMap all collectors in map keyed by their names
   * @param totalHitsThreshold total number of hit to count to
   * @param topHits number of hits to fetch
   * @param facetsList facets list
   * @param rescorersList rescorer list
   * @param timeoutSec search request timeout in seconds
   * @param timeoutCheckEvery check timeout is met every N hits per leaf
   * @param terminateAfter max number of hit to stop ranking
   * @param profile flag to determine wheteher to generate profile information
   * @param disallowPartialResults flag to determine whether partial results is allowed in case of
   *     timeout
   * @param querySort sort settings for the response
   */
  public CollectorCreatorContext(
      IndexState indexState,
      ShardState shardState,
      Map<String, FieldDef> queryFields,
      SearcherAndTaxonomy searcherAndTaxonomy,
      Query query,
      Map<String, Collector> collectorsMap,
      int totalHitsThreshold,
      int topHits,
      List<Facet> facetsList,
      List<Rescorer> rescorersList,
      double timeoutSec,
      int timeoutCheckEvery,
      int terminateAfter,
      boolean profile,
      boolean disallowPartialResults,
      QuerySortField querySort) {
    this.indexState = indexState;
    this.shardState = shardState;
    this.queryFields = queryFields;
    this.searcherAndTaxonomy = searcherAndTaxonomy;
    this.query = query;
    this.collectorsMap = collectorsMap;
    this.totalHitsThreshold = totalHitsThreshold;
    this.topHits = topHits;
    this.facetsList = facetsList;
    this.rescorersList = rescorersList;
    this.timeoutSec = timeoutSec;
    this.timeoutCheckEvery = timeoutCheckEvery;
    this.terminateAfter = terminateAfter;
    this.profile = profile;
    this.disallowPartialResults = disallowPartialResults;
    this.querySort = querySort;
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

  /** Get query */
  public Query getQuery() {
    return query;
  }

  /** Get total hits threshold */
  public int getTotalHitsThreshold() {
    return totalHitsThreshold;
  }

  /** Get query sort */
  public QuerySortField getQuerySort() {
    return querySort;
  }

  /** Get top hits */
  public int getTopHits() {
    return topHits;
  }

  /** Get facets list */
  public List<Facet> getFacetsList() {
    return facetsList;
  }

  /** Get rescorers list */
  public List<Rescorer> getRescorersList() {
    return rescorersList;
  }

  /** Get search request timeout in seconds */
  public double getTimeoutSec() {
    return timeoutSec;
  }

  /** Get search timeout check for every N hits */
  public int getTimeoutCheckEvery() {
    return timeoutCheckEvery;
  }

  /** Get terminate after */
  public int getTerminateAfter() {
    return terminateAfter;
  }

  /** Get is disallow partial results */
  public boolean isDisallowPartialResults() {
    return disallowPartialResults;
  }

  /** Get needs profile */
  public boolean isProfile() {
    return profile;
  }

  /** Get collectors map */
  public Map<String, Collector> getCollectorsMap() {
    return collectorsMap;
  }
}
