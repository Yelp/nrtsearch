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

import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.VirtualField;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.QueryNodeMapper;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.DocCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.LargeNumHitsCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.RelevanceCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.SortFieldCollector;
import com.yelp.nrtsearch.server.utils.ScriptParamsUtils;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;

/**
 * Class to process a {@link SearchRequest} grpc message into the data structures required for
 * search. Produces a {@link SearchContext} usable to perform search operations.
 */
public class SearchRequestProcessor {
  /**
   * By default we count hits accurately up to 1000. This makes sure that we don't spend most time
   * on computing hit counts
   */
  public static final int TOTAL_HITS_THRESHOLD = 1000;

  private static final QueryNodeMapper QUERY_NODE_MAPPER = new QueryNodeMapper();

  private SearchRequestProcessor() {}

  /**
   * Create a {@link SearchContext} representing the given {@link SearchRequest} and index/searcher
   * state.
   *
   * @param searchRequest grpc request message
   * @param indexState index state
   * @param shardState shard state
   * @param searcherAndTaxonomy index searcher
   * @param diagnostics container message for returned diagnostic info
   * @return context info needed to execute the search query
   * @throws IOException if query rewrite fails
   */
  public static SearchContext buildContextForRequest(
      SearchRequest searchRequest,
      IndexState indexState,
      ShardState shardState,
      SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy,
      SearchResponse.Diagnostics.Builder diagnostics)
      throws IOException {

    SearchContext.Builder contextBuilder = SearchContext.newBuilder();
    SearchResponse.Builder responseBuilder = SearchResponse.newBuilder();

    contextBuilder
        .setIndexState(indexState)
        .setShardState(shardState)
        .setSearcherAndTaxonomy(searcherAndTaxonomy)
        .setResponseBuilder(responseBuilder)
        .setTimestampSec(System.currentTimeMillis() / 1000)
        .setStartHit(searchRequest.getStartHit())
        .setTopHits(searchRequest.getTopHits());

    Map<String, FieldDef> queryVirtualFields = getVirtualFields(shardState, searchRequest);

    Map<String, FieldDef> queryFields = new HashMap<>(queryVirtualFields);
    addIndexFields(indexState, queryFields);
    contextBuilder.setQueryFields(Collections.unmodifiableMap(queryFields));

    Map<String, FieldDef> retrieveFields = new HashMap<>(queryVirtualFields);
    addRetrieveFields(indexState, searchRequest, retrieveFields);
    contextBuilder.setRetrieveFields(Collections.unmodifiableMap(retrieveFields));

    Query query = extractQuery(indexState, searchRequest);
    diagnostics.setParsedQuery(query.toString());

    query = searcherAndTaxonomy.searcher.rewrite(query);
    diagnostics.setRewrittenQuery(query.toString());

    if (searchRequest.getFacetsCount() > 0) {
      query = addDrillDowns(indexState, query);
      diagnostics.setDrillDownQuery(query.toString());
    }

    contextBuilder.setFetchTasks(new FetchTasks(searchRequest.getFetchTasksList()));

    contextBuilder.setQuery(query);
    contextBuilder.setCollector(buildDocCollector(queryFields, searchRequest));
    return contextBuilder.build(true);
  }

  /**
   * Parses any virtualFields, which define dynamic (expression) fields for this one request.
   *
   * @throws IllegalArgumentException if there are multiple virtual fields with the same name
   */
  private static Map<String, FieldDef> getVirtualFields(
      ShardState shardState, SearchRequest searchRequest) {
    if (searchRequest.getVirtualFieldsList().isEmpty()) {
      return new HashMap<>();
    }

    IndexState indexState = shardState.indexState;
    Map<String, FieldDef> virtualFields = new HashMap<>();
    for (VirtualField vf : searchRequest.getVirtualFieldsList()) {
      if (virtualFields.containsKey(vf.getName())) {
        throw new IllegalArgumentException(
            "Multiple definitions of Virtual field: " + vf.getName());
      }
      ScoreScript.Factory factory =
          ScriptService.getInstance().compile(vf.getScript(), ScoreScript.CONTEXT);
      Map<String, Object> params = ScriptParamsUtils.decodeParams(vf.getScript().getParamsMap());
      FieldDef virtualField =
          new VirtualFieldDef(vf.getName(), factory.newFactory(params, indexState.docLookup));
      virtualFields.put(vf.getName(), virtualField);
    }
    return virtualFields;
  }

  /**
   * Add specified retrieve fields to given query fields map.
   *
   * @param indexState state for query index
   * @param searchRequest request
   * @param queryFields mutable current map of query fields
   * @throws IllegalArgumentException if any retrieve field already exists
   */
  private static void addRetrieveFields(
      IndexState indexState, SearchRequest searchRequest, Map<String, FieldDef> queryFields) {
    for (String field : searchRequest.getRetrieveFieldsList()) {
      FieldDef fieldDef = getFieldDef(field, indexState);
      FieldDef current = queryFields.put(field, fieldDef);
      if (current != null) {
        throw new IllegalArgumentException("QueryFields: " + field + " specified multiple times");
      }
    }
  }

  /**
   * Add index fields to given query fields map.
   *
   * @param indexState state for query index
   * @param queryFields mutable current map of query fields
   * @throws IllegalArgumentException if any index field already exists
   */
  private static void addIndexFields(IndexState indexState, Map<String, FieldDef> queryFields) {
    for (Map.Entry<String, FieldDef> entry : indexState.getAllFields().entrySet()) {
      FieldDef current = queryFields.put(entry.getKey(), entry.getValue());
      if (current != null) {
        throw new IllegalArgumentException(
            "QueryFields: " + entry.getKey() + " specified multiple times");
      }
    }
  }

  /**
   * Find {@link FieldDef} for field name.
   *
   * @param name name of field
   * @param indexState state for query index
   * @return definition object for field
   * @throws IllegalArgumentException if field does not exist or is not retrievable
   */
  private static FieldDef getFieldDef(String name, IndexState indexState) {
    FieldDef fieldDef = indexState.getField(name);
    if (fieldDef == null) {
      throw new IllegalArgumentException("QueryFields: " + name + " does not exist");
    }
    if (!isRetrievable(fieldDef)) {
      throw new IllegalArgumentException(
          "QueryFields: "
              + name
              + " is not retrievable, must be stored"
              + " or have doc values enabled");
    }
    return fieldDef;
  }

  /** If a field's value can be retrieved */
  private static boolean isRetrievable(FieldDef fieldDef) {
    if (fieldDef instanceof VirtualFieldDef) {
      return true;
    }
    if (fieldDef instanceof IndexableFieldDef) {
      IndexableFieldDef indexableFieldDef = (IndexableFieldDef) fieldDef;
      return indexableFieldDef.hasDocValues() || indexableFieldDef.isStored();
    }
    return false;
  }

  /**
   * Get the lucene {@link Query} represented by this request.
   *
   * @param state index state
   * @param searchRequest request
   * @return lucene query
   */
  private static Query extractQuery(IndexState state, SearchRequest searchRequest) {
    Query q;
    if (!searchRequest.getQueryText().isEmpty()) {
      QueryBuilder queryParser = createQueryParser(state, null);

      String queryText = searchRequest.getQueryText();

      try {
        q = parseQuery(queryParser, queryText);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format("could not parse queryText: %s", queryText));
      }
    } else if (searchRequest.getQuery().getQueryNodeCase()
        != com.yelp.nrtsearch.server.grpc.Query.QueryNodeCase.QUERYNODE_NOT_SET) {
      q = QUERY_NODE_MAPPER.getQuery(searchRequest.getQuery(), state);
    } else {
      q = new MatchAllDocsQuery();
    }

    if (state.hasNestedChildFields()) {
      return QUERY_NODE_MAPPER.applyQueryNestedPath(q, searchRequest.getQueryNestedPath());
    }
    return q;
  }

  /** If field is non-null it overrides any specified defaultField. */
  private static QueryBuilder createQueryParser(IndexState state, String field) {
    // TODO: Support "queryParser" field provided by user e.g. MultiFieldQueryParser,
    // SimpleQueryParser, classic
    List<String> fields;
    if (field != null) {
      fields = Collections.singletonList(field);
    } else {
      // Default to MultiFieldQueryParser over all indexed fields:
      fields = state.getIndexedAnalyzedFields();
    }
    return new MultiFieldQueryParser(fields.toArray(new String[0]), state.searchAnalyzer);
  }

  /** Build lucene {@link Query} from text expression */
  private static Query parseQuery(QueryBuilder qp, String text)
      throws org.apache.lucene.queryparser.classic.ParseException {
    if (qp instanceof QueryParserBase) {
      return ((QueryParserBase) qp).parse(text);
    } else {
      return ((SimpleQueryParser) qp).parse(text);
    }
  }

  /** Fold in any drillDowns requests into the query. */
  private static DrillDownQuery addDrillDowns(IndexState state, Query q) {
    return new DrillDownQuery(state.facetsConfig, q);
  }

  /**
   * Build {@link DocCollector} to provide the {@link org.apache.lucene.search.CollectorManager} for
   * collecting hits for this query.
   *
   * @param queryFields all valid fields for this query
   * @param searchRequest request
   * @return collector
   */
  private static DocCollector buildDocCollector(
      Map<String, FieldDef> queryFields, SearchRequest searchRequest) {
    if (searchRequest.getQuerySort().getFields().getSortedFieldsList().isEmpty()) {
      if (hasLargeNumHits(searchRequest)) {
        return new LargeNumHitsCollector(searchRequest);
      } else {
        return new RelevanceCollector(searchRequest);
      }
    } else {
      return new SortFieldCollector(queryFields, searchRequest);
    }
  }

  /** If this query needs enough hits to use a {@link LargeNumHitsCollector}. */
  private static boolean hasLargeNumHits(SearchRequest searchRequest) {
    return searchRequest.hasQuery()
        && searchRequest.getQuery().getQueryNodeCase()
            == com.yelp.nrtsearch.server.grpc.Query.QueryNodeCase.QUERYNODE_NOT_SET;
  }
}
