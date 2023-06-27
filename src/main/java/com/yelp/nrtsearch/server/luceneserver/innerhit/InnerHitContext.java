/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.innerhit;

import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.QueryNodeMapper;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler.SearchHandlerException;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.highlights.HighlightFetchTask;
import com.yelp.nrtsearch.server.luceneserver.search.FetchTasks;
import com.yelp.nrtsearch.server.luceneserver.search.FieldFetchContext;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.luceneserver.search.SortParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;

/**
 * Object to store all necessary context information for {@link InnerHitFetchTask} to search and
 * fetch for each hit.
 */
public class InnerHitContext implements FieldFetchContext {

  private static final int DEFAULT_INNER_HIT_TOP_HITS = 3;

  private final String innerHitName;
  private final Query parentFilterQuery;
  private final String queryNestedPath;
  private final Query childFilterQuery;
  private final Query query;
  private final IndexState indexState;
  private final ShardState shardState;
  private final SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy;
  private final int startHit;
  private final int topHits;
  private final Map<String, FieldDef> queryFields;
  private final Map<String, FieldDef> retrieveFields;
  private final List<String> sortedFieldNames;
  private final Sort sort;
  private final CollectorManager<? extends TopDocsCollector, ? extends TopDocs>
      topDocsCollectorManager;
  private final FetchTasks fetchTasks;
  private SearchContext searchContext = null;

  private InnerHitContext(InnerHitContextBuilder builder, boolean needValidation)
      throws IOException {
    this.innerHitName = builder.innerHitName;
    this.queryNestedPath = builder.queryNestedPath;
    this.indexState = builder.indexState;
    this.shardState = builder.shardState;
    this.searcherAndTaxonomy = builder.searcherAndTaxonomy;
    this.parentFilterQuery =
        QueryNodeMapper.getInstance().getNestedPathQuery(indexState, builder.parentQueryNestedPath);
    this.childFilterQuery =
        QueryNodeMapper.getInstance().getNestedPathQuery(indexState, queryNestedPath);
    this.query = builder.query;
    this.startHit = builder.startHit;
    // TODO: implement the totalCountCollector in case (topHits == 0 || startHit >= topHits).
    // Currently, return DEFAULT_INNER_HIT_TOP_HITS results in case of 0.
    this.topHits = builder.topHits == 0 ? DEFAULT_INNER_HIT_TOP_HITS : builder.topHits;
    this.queryFields = builder.queryFields;
    this.retrieveFields = builder.retrieveFields;
    this.fetchTasks = new FetchTasks(Collections.EMPTY_LIST, builder.highlightFetchTask, null);

    if (builder.querySort == null) {
      // relevance collector
      this.sortedFieldNames = Collections.EMPTY_LIST;
      this.sort = null;
      this.topDocsCollectorManager =
          TopScoreDocCollector.createSharedManager(topHits, null, Integer.MAX_VALUE);
    } else {
      // sortedField collector
      this.sortedFieldNames =
          new ArrayList<>(builder.querySort.getFields().getSortedFieldsList().size());
      try {
        this.sort =
            SortParser.parseSort(
                builder.querySort.getFields().getSortedFieldsList(), sortedFieldNames, queryFields);
        this.topDocsCollectorManager =
            TopFieldCollector.createSharedManager(sort, topHits, null, Integer.MAX_VALUE);
      } catch (SearchHandlerException e) {
        throw new IllegalArgumentException(e);
      }
    }

    if (needValidation) {
      validate();
    }
  }

  /**
   * A basic and non-exhausted validation at the construction time. Fail before search so that we
   * don't waste resources on invalid search request.
   */
  private void validate() {
    Objects.requireNonNull(indexState);
    Objects.requireNonNull(shardState);
    Objects.requireNonNull(searcherAndTaxonomy);
    Objects.requireNonNull(queryFields);
    Objects.requireNonNull(retrieveFields);
    Objects.requireNonNull(query);
    Objects.requireNonNull(queryNestedPath);
    Objects.requireNonNull(topDocsCollectorManager);

    if (startHit < 0) {
      throw new IllegalStateException(
          String.format("Invalid startHit value in InnerHit [%s]: %d", innerHitName, startHit));
    }
    if (topHits < 0) {
      throw new IllegalStateException(
          String.format("Invalid topHits value in InnerHit [%s]: %d", innerHitName, topHits));
    }
    if (queryNestedPath.isEmpty()) {
      throw new IllegalStateException(
          String.format("queryNestedPath in InnerHit [%s] cannot be empty", innerHitName));
    }
    if (!indexState.hasNestedChildFields()) {
      throw new IllegalStateException("InnerHit only works with indices that have childFields");
    }
  }

  /** Get parent filter query. */
  public Query getParentFilterQuery() {
    return parentFilterQuery;
  }

  /** Get the name of the innerHit task. */
  public String getInnerHitName() {
    return innerHitName;
  }

  /**
   * Get the nested path for the innerHit query. This path is the field name of the nested object.
   */
  public String getQueryNestedPath() {
    return queryNestedPath;
  }

  /** Get child filter query. */
  public Query getChildFilterQuery() {
    return childFilterQuery;
  }

  /**
   * Get the query for the innerHit. Should assume this query is directly searched against the child
   * documents only. Omitted this field to retrieve all children for each hit.
   */
  public Query getQuery() {
    return query;
  }

  /** Get IndexState */
  public IndexState getIndexState() {
    return indexState;
  }

  /** Get ShardState */
  public ShardState getShardState() {
    return shardState;
  }

  /** Get SearcherAndTaxonomy */
  @Override
  public SearcherAndTaxonomy getSearcherAndTaxonomy() {
    return searcherAndTaxonomy;
  }

  /** Get FetchTasks for the InnerHit. Currently, we only support highlight. */
  @Override
  public FetchTasks getFetchTasks() {
    return fetchTasks;
  }

  /** Get the base SearchContext. This is not used in InnerHit, and is always null. */
  @Override
  public SearchContext getSearchContext() {
    return searchContext;
  }

  /** Get the StartHit */
  public int getStartHit() {
    return startHit;
  }

  /** Get the topHits */
  public int getTopHits() {
    return topHits;
  }

  /**
   * Get map of all fields usable for this query. This includes all fields defined in the index and
   * dynamic fields from the request. This is read from the top level search.
   */
  public Map<String, FieldDef> getQueryFields() {
    return queryFields;
  }

  /** Get the fields to retrieve */
  @Override
  public Map<String, FieldDef> getRetrieveFields() {
    return retrieveFields;
  }

  /**
   * Get the field names used in sort if {@link QuerySortField} is in use, otherwise returns an
   * empty list.
   */
  public List<String> getSortedFieldNames() {
    return sortedFieldNames;
  }

  /** Get the sort object if {@link QuerySortField} is in use, otherwise returns null. */
  public Sort getSort() {
    return sort;
  }

  /** Get the topDocsCollectorManager to collect the search results. */
  public CollectorManager<? extends TopDocsCollector, ? extends TopDocs>
      getTopDocsCollectorManager() {
    return topDocsCollectorManager;
  }

  /**
   * A builder class to build the {@link InnerHitContext}. Use it to avoid the constructor with a
   * long arguments list.
   */
  public static final class InnerHitContextBuilder {

    private String innerHitName;
    private String parentQueryNestedPath;
    private String queryNestedPath;
    private Query query;
    private IndexState indexState;
    private ShardState shardState;
    private SearcherAndTaxonomy searcherAndTaxonomy;
    private int startHit;
    private int topHits;
    private Map<String, FieldDef> queryFields;
    private Map<String, FieldDef> retrieveFields;
    private HighlightFetchTask highlightFetchTask;
    private QuerySortField querySort;

    private InnerHitContextBuilder() {}

    public InnerHitContext build(boolean needValidation) {
      try {
        return new InnerHitContext(this, needValidation);
      } catch (IOException e) {
        throw new RuntimeException("Failed to build the InnerHitContext", e);
      }
    }

    public static InnerHitContextBuilder Builder() {
      return new InnerHitContextBuilder();
    }

    public InnerHitContextBuilder withInnerHitName(String innerHitName) {
      this.innerHitName = innerHitName;
      return this;
    }

    public InnerHitContextBuilder withParentQueryNestedPath(String parentQueryNestedPath) {
      this.parentQueryNestedPath = parentQueryNestedPath;
      return this;
    }

    public InnerHitContextBuilder withQueryNestedPath(String queryNestedPath) {
      this.queryNestedPath = queryNestedPath;
      return this;
    }

    public InnerHitContextBuilder withQuery(Query query) {
      this.query = query;
      return this;
    }

    public InnerHitContextBuilder withIndexState(IndexState indexState) {
      this.indexState = indexState;
      return this;
    }

    public InnerHitContextBuilder withShardState(ShardState shardState) {
      this.shardState = shardState;
      return this;
    }

    public InnerHitContextBuilder withSearcherAndTaxonomy(SearcherAndTaxonomy searcherAndTaxonomy) {
      this.searcherAndTaxonomy = searcherAndTaxonomy;
      return this;
    }

    public InnerHitContextBuilder withStartHit(int startHit) {
      this.startHit = startHit;
      return this;
    }

    public InnerHitContextBuilder withTopHits(int topHits) {
      this.topHits = topHits;
      return this;
    }

    public InnerHitContextBuilder withQueryFields(Map<String, FieldDef> queryFields) {
      this.queryFields = queryFields;
      return this;
    }

    public InnerHitContextBuilder withRetrieveFields(Map<String, FieldDef> retrieveFields) {
      this.retrieveFields = retrieveFields;
      return this;
    }

    public InnerHitContextBuilder withHighlightFetchTask(HighlightFetchTask highlightFetchTask) {
      this.highlightFetchTask = highlightFetchTask;
      return this;
    }

    public InnerHitContextBuilder withQuerySort(QuerySortField querySort) {
      this.querySort = querySort;
      return this;
    }
  }
}
