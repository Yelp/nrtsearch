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
package com.yelp.nrtsearch.server.search;

import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.doc.SharedDocContext;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.rescore.RescoreTask;
import com.yelp.nrtsearch.server.search.collectors.DocCollector;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.search.Query;

/** Search context class to provide all the information to perform a search. */
public class SearchContext implements FieldFetchContext {
  private final IndexState indexState;
  private final DocLookup docLookup;
  private final ShardState shardState;
  private final SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy;
  private final SearchResponse.Builder responseBuilder;

  private final long timestampSec;
  private final int startHit;
  private final int topHits;
  private final Map<String, FieldDef> queryFields;
  private final Map<String, FieldDef> retrieveFields;
  private final Query query;
  private final DocCollector collector;
  private final FetchTasks fetchTasks;
  private final List<RescoreTask> rescorers;
  private final SharedDocContext sharedDocContext;
  private final Map<String, Object> extraContext;
  private final String queryNestedPath;

  public enum VectorScoringMode {
    NONE,
    VECTORS_ONLY,
    HYBRID
  }

  private final boolean explain;
  private final boolean warming;

  private SearchContext(Builder builder, boolean validate) {
    this.indexState = builder.indexState;
    this.docLookup = builder.docLookup;
    this.shardState = builder.shardState;
    this.searcherAndTaxonomy = builder.searcherAndTaxonomy;
    this.responseBuilder = builder.responseBuilder;
    this.timestampSec = builder.timestampSec;
    this.startHit = builder.startHit;
    this.topHits = builder.topHits;
    this.queryFields = builder.queryFields;
    this.retrieveFields = builder.retrieveFields;
    this.query = builder.query;
    this.collector = builder.collector;
    this.fetchTasks = builder.fetchTasks;
    this.rescorers = builder.rescorers;
    this.sharedDocContext = builder.sharedDocContext;
    this.extraContext = builder.extraContext;
    this.queryNestedPath = builder.queryNestedPath;
    this.explain = builder.explain;
    this.warming = builder.warming;

    if (validate) {
      validate();
    }
  }

  /** Get query index state. */
  public IndexState getIndexState() {
    return indexState;
  }

  /** Get DocLookup. */
  public DocLookup getDocLookup() {
    return docLookup;
  }

  /** Get query shard state. */
  public ShardState getShardState() {
    return shardState;
  }

  /** Get searcher instance for query. */
  @Override
  public SearcherAndTaxonomy getSearcherAndTaxonomy() {
    return searcherAndTaxonomy;
  }

  /** Get response message builder for search request */
  public SearchResponse.Builder getResponseBuilder() {
    return responseBuilder;
  }

  /** Get timestamp to use for query. */
  public long getTimestampSec() {
    return timestampSec;
  }

  /** Get the offset of the first hit to return from the top hits. */
  public int getStartHit() {
    return startHit;
  }

  /** Get the number of hits to collect to satisfy the search response */
  public int getTopHits() {
    return topHits;
  }

  /** Get the number of hits to log */
  public int getHitsToLog() {
    if (this.getFetchTasks().getHitsLoggerFetchTask() != null) {
      return getFetchTasks().getHitsLoggerFetchTask().getHitsToLog();
    }
    return 0;
  }

  /**
   * Get map of all fields usable for this query. This includes all fields defined in the index and
   * dynamic fields from the request.
   */
  public Map<String, FieldDef> getQueryFields() {
    return queryFields;
  }

  /** Get map of all fields that should be filled in the response */
  @Override
  public Map<String, FieldDef> getRetrieveFields() {
    return retrieveFields;
  }

  /** Get final lucene query to perform. */
  public Query getQuery() {
    return query;
  }

  /** Get collector for query. */
  public DocCollector getCollector() {
    return collector;
  }

  /** Get any extra tasks that should be run during fetch */
  @Override
  public FetchTasks getFetchTasks() {
    return fetchTasks;
  }

  /** Get rescorers that should be executed after the first pass */
  public List<RescoreTask> getRescorers() {
    return rescorers;
  }

  /** Get shared context accessor for documents */
  public SharedDocContext getSharedDocContext() {
    return sharedDocContext;
  }

  /**
   * Get the extra custom context map which can be used for cache or data sharing. This map should
   * be threadsafe.
   */
  public Map<String, Object> getExtraContext() {
    return extraContext;
  }

  /** Get the query nested path. By default, it is _root * */
  public String getQueryNestedPath() {
    return queryNestedPath;
  }

  /** Get the boolean flat whether to return the lucene explain */
  @Override
  public boolean isExplain() {
    return explain;
  }

  public boolean isWarming() {
    return warming;
  }

  /** Get new context builder instance * */
  public static Builder newBuilder() {
    return new Builder();
  }

  private void validate() {
    Objects.requireNonNull(indexState);
    Objects.requireNonNull(shardState);
    Objects.requireNonNull(searcherAndTaxonomy);
    Objects.requireNonNull(queryFields);
    Objects.requireNonNull(retrieveFields);
    Objects.requireNonNull(query);
    Objects.requireNonNull(collector);
    Objects.requireNonNull(fetchTasks);
    Objects.requireNonNull(rescorers);
    Objects.requireNonNull(sharedDocContext);
    Objects.requireNonNull(docLookup);

    if (timestampSec < 0) {
      throw new IllegalStateException("Invalid timestamp value: " + timestampSec);
    }
    if (startHit < 0) {
      throw new IllegalStateException("Invalid startHit value: " + startHit);
    }
    if (topHits < 0) {
      throw new IllegalStateException("Invalid topHits value: " + topHits);
    }
  }

  /** Get search context. */
  @Override
  public SearchContext getSearchContext() {
    return this;
  }

  /** Builder class for search context. */
  public static class Builder {

    private IndexState indexState;
    private DocLookup docLookup;
    private ShardState shardState;
    private SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy;
    private SearchResponse.Builder responseBuilder;

    private long timestampSec = -1;
    private int startHit = -1;
    private int topHits = -1;
    private Map<String, FieldDef> queryFields;
    private Map<String, FieldDef> retrieveFields;
    private Query query;
    private DocCollector collector;
    private FetchTasks fetchTasks;
    private List<RescoreTask> rescorers;
    private SharedDocContext sharedDocContext;
    private Map<String, Object> extraContext;
    private String queryNestedPath;
    private boolean explain;
    private boolean warming;

    private Builder() {}

    /** Set query index state. */
    public Builder setIndexState(IndexState indexState) {
      this.indexState = indexState;
      return this;
    }

    /** Set doclookup. */
    public Builder setDocLookup(DocLookup docLookup) {
      this.docLookup = docLookup;
      return this;
    }

    /** Set query shard state. */
    public Builder setShardState(ShardState shardState) {
      this.shardState = shardState;
      return this;
    }

    /** Set searcher instance for query. */
    public Builder setSearcherAndTaxonomy(SearcherAndTaxonomy s) {
      this.searcherAndTaxonomy = s;
      return this;
    }

    /** Set response message builder for search request */
    public Builder setResponseBuilder(SearchResponse.Builder responseBuilder) {
      this.responseBuilder = responseBuilder;
      return this;
    }

    /** Set timestamp to use for query. */
    public Builder setTimestampSec(long timestampSec) {
      this.timestampSec = timestampSec;
      return this;
    }

    /** Set the offset of the first hit to return from the top hits. */
    public Builder setStartHit(int startHit) {
      this.startHit = startHit;
      return this;
    }

    /** Set the number of hits to collect to satisfy the search response */
    public Builder setTopHits(int topHits) {
      this.topHits = topHits;
      return this;
    }

    /**
     * Set map of all fields usable for this query. This includes all fields defined in the index
     * and dynamic fields from the request.
     */
    public Builder setQueryFields(Map<String, FieldDef> queryFields) {
      this.queryFields = queryFields;
      return this;
    }

    /** Set map of all fields that should be filled in the response */
    public Builder setRetrieveFields(Map<String, FieldDef> retrieveFields) {
      this.retrieveFields = retrieveFields;
      return this;
    }

    /** Set final lucene query to perform. */
    public Builder setQuery(Query query) {
      this.query = query;
      return this;
    }

    /** Set collector for query. */
    public Builder setCollector(DocCollector collector) {
      this.collector = collector;
      return this;
    }

    /** Set any extra tasks that should be run during fetch */
    public Builder setFetchTasks(FetchTasks fetchTasks) {
      this.fetchTasks = fetchTasks;
      return this;
    }

    /** Set rescorers that should be executed after the first pass */
    public Builder setRescorers(List<RescoreTask> rescorers) {
      this.rescorers = rescorers;
      return this;
    }

    /** Set shared context accessor for documents */
    public Builder setSharedDocContext(SharedDocContext sharedDocContext) {
      this.sharedDocContext = sharedDocContext;
      return this;
    }

    public Builder setExtraContext(Map<String, Object> extraContext) {
      this.extraContext = extraContext;
      return this;
    }

    public Builder setQueryNestedPath(String queryNestedPath) {
      this.queryNestedPath = queryNestedPath;
      return this;
    }

    public Builder setExplain(boolean explain) {
      this.explain = explain;
      return this;
    }

    public Builder setWarming(boolean warming) {
      this.warming = warming;
      return this;
    }

    /**
     * Use builder to create new search context. Skipping validation is possible, but mainly
     * intended for tests that do not require a complete context.
     *
     * @param validate if validation should be performed on built context
     * @return search context
     */
    public SearchContext build(boolean validate) {
      return new SearchContext(this, validate);
    }
  }
}
