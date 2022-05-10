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

import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import java.util.Map;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;

/**
 * Context information needed to create collectors. Used for {@link DocCollector} and {@link
 * AdditionalCollectorManager} building. The search context can be accessed through {@link
 * AdditionalCollectorManager#setSearchContext(SearchContext)}
 */
public class CollectorCreatorContext {
  private final SearchRequest request;
  private final IndexState indexState;
  private final ShardState shardState;
  private final Map<String, FieldDef> queryFields;
  private final SearcherAndTaxonomy searcherAndTaxonomy;

  /**
   * Constructor.
   *
   * @param request search request
   * @param indexState index state
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
    this.request = request;
    this.indexState = indexState;
    this.shardState = shardState;
    this.queryFields = queryFields;
    this.searcherAndTaxonomy = searcherAndTaxonomy;
  }

  /** Get search request */
  public SearchRequest getRequest() {
    return request;
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
}
