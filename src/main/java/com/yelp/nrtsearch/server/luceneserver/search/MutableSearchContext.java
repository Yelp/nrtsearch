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
package com.yelp.nrtsearch.server.luceneserver.search;

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.DocCollector;
import java.util.Map;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.search.Query;

/** Mutable implementation of {@link SearchContext} built by the {@link SearchRequestProcessor}. */
class MutableSearchContext implements SearchContext {
  private final IndexState indexState;
  private final ShardState shardState;
  private final SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy;
  private final SearchResponse.Builder searchResponse;

  private long timestampSec;
  private int startHit;
  private Map<String, FieldDef> queryFields;
  private Query query;
  private DocCollector collector;

  MutableSearchContext(
      IndexState indexState,
      ShardState shardState,
      SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy,
      SearchResponse.Builder searchResponse) {
    this.indexState = indexState;
    this.shardState = shardState;
    this.searcherAndTaxonomy = searcherAndTaxonomy;
    this.searchResponse = searchResponse;
  }

  @Override
  public IndexState indexState() {
    return indexState;
  }

  @Override
  public ShardState shardState() {
    return shardState;
  }

  @Override
  public SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy() {
    return searcherAndTaxonomy;
  }

  @Override
  public SearchResponse.Builder searchResponse() {
    return searchResponse;
  }

  @Override
  public long timestampSec() {
    return timestampSec;
  }

  @Override
  public int startHit() {
    return startHit;
  }

  @Override
  public Map<String, FieldDef> queryFields() {
    return queryFields;
  }

  @Override
  public Query query() {
    return query;
  }

  @Override
  public DocCollector collector() {
    return collector;
  }

  void setTimestampSec(long timestampSec) {
    this.timestampSec = timestampSec;
  }

  void setStartHit(int startHit) {
    this.startHit = startHit;
  }

  void setQueryFields(Map<String, FieldDef> queryFields) {
    this.queryFields = queryFields;
  }

  void setQuery(Query query) {
    this.query = query;
  }

  void setCollector(DocCollector collector) {
    this.collector = collector;
  }
}
