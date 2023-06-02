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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;

public class InnerHitContext implements FieldFetchContext {

  public BitSetProducer getParentFilter() {
    return parentFilter;
  }

  public String getInnerHitName() {
    return innerHitName;
  }

  private final String innerHitName;
  private final BitSetProducer parentFilter;
  private final String queryNestedPath;
  private final Query query;
  private final IndexState indexState;
  private final ShardState shardState;
  private final SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy;
  private final int startHit;
  private final int topHits;
  private final Map<String, FieldDef> queryFields;
  private final Map<String, FieldDef> retrieveFields;
  private final HighlightFetchTask highlightFetchTask;
  private final List<String> sortedFieldNames;
  private final Sort sort;
  // Each innerHit is processed in a single leaf, so no parallelism is needed.
  private final TopDocsCollector topDocsCollector;
  private final FetchTasks fetchTasks = new FetchTasks(Collections.EMPTY_LIST);
  private SearchContext searchContext;

  private InnerHitContext(InnerHitContextBuilder builder, boolean needValidation) {
    this.innerHitName = builder.innerHitName;
    this.queryNestedPath = builder.queryNestedPath;
    this.query = builder.query;
    this.parentFilter =
        new QueryBitSetProducer(
            QueryNodeMapper.getInstance().getNestedPathQuery(builder.parentQueryNestedPath));
    this.indexState = builder.indexState;
    this.shardState = builder.shardState;
    this.searcherAndTaxonomy = builder.searcherAndTaxonomy;
    this.startHit = builder.startHit;
    this.topHits = builder.topHits;
    this.queryFields = builder.queryFields;
    this.retrieveFields = builder.retrieveFields;
    this.highlightFetchTask = builder.highlightFetchTask;

    if (builder.querySort == null) {
      // relevance collector
      this.sortedFieldNames = Collections.EMPTY_LIST;
      this.sort = null;
      this.topDocsCollector = TopScoreDocCollector.create(topHits, null, Integer.MAX_VALUE);
    } else {
      // sortedField collector
      this.sortedFieldNames =
          new ArrayList<>(builder.querySort.getFields().getSortedFieldsList().size());
      try {
        this.sort =
            SortParser.parseSort(
                builder.querySort.getFields().getSortedFieldsList(), sortedFieldNames, queryFields);
        this.topDocsCollector = TopFieldCollector.create(sort, topHits, null, Integer.MAX_VALUE);
      } catch (SearchHandlerException e) {
        throw new IllegalArgumentException(e);
      }
    }

    if (needValidation) {
      validate();
    }
  }

  private void validate() {
    Objects.requireNonNull(indexState);
    Objects.requireNonNull(shardState);
    Objects.requireNonNull(searcherAndTaxonomy);
    Objects.requireNonNull(queryFields);
    Objects.requireNonNull(retrieveFields);
    Objects.requireNonNull(query);
    Objects.requireNonNull(queryNestedPath);
    Objects.requireNonNull(topDocsCollector);

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
  }

  public void setSearchContext(SearchContext searchContext) {
    this.searchContext = searchContext;
  }

  public String getQueryNestedPath() {
    return queryNestedPath;
  }

  public Query getQuery() {
    return query;
  }

  public IndexState getIndexState() {
    return indexState;
  }

  public ShardState getShardState() {
    return shardState;
  }

  @Override
  public SearcherAndTaxonomy getSearcherAndTaxonomy() {
    return searcherAndTaxonomy;
  }

  @Override
  public FetchTasks getFetchTasks() {
    return fetchTasks;
  }

  @Override
  public SearchContext getSearchContext() {
    return searchContext;
  }

  public int getStartHit() {
    return startHit;
  }

  public int getTopHits() {
    return topHits;
  }

  public Map<String, FieldDef> getQueryFields() {
    return queryFields;
  }

  @Override
  public Map<String, FieldDef> getRetrieveFields() {
    return retrieveFields;
  }

  public HighlightFetchTask getHighlightFetchTask() {
    return highlightFetchTask;
  }

  public List<String> getSortedFieldNames() {
    return sortedFieldNames;
  }

  public Sort getSort() {
    return sort;
  }

  public TopDocsCollector getTopDocsCollector() {
    return topDocsCollector;
  }

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
      return new InnerHitContext(this, needValidation);
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
