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

import com.google.common.collect.Maps;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.grpc.VirtualField;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptParamsTransformer;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.DocCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.FieldCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.LargeNumHitsCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.ScoreCollector;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
 * search. Produces a {@link SearchContext} usable by the {@link SearchExecutor}.
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

    MutableSearchContext context =
        new MutableSearchContext(indexState, shardState, searcherAndTaxonomy, diagnostics);
    context.setTimestampSec(System.currentTimeMillis() / 1000);
    context.setStartHit(searchRequest.getStartHit());

    context.setQueryFields(getQueryFields(context, searchRequest));
    context.setSortFieldNames(getSortFieldNames(searchRequest));
    context.setRetrieveFieldNames(Set.copyOf(searchRequest.getRetrieveFieldsList()));

    Query query = extractQuery(context.indexState(), searchRequest);
    context.diagnostics().setParsedQuery(query.toString());

    Query rewrittenQuery = context.searcherAndTaxonomy().searcher.rewrite(query);
    context.diagnostics().setRewrittenQuery(rewrittenQuery.toString());

    Query drillDownQuery = addDrillDowns(context.indexState(), rewrittenQuery);
    context.diagnostics().setDrillDownQuery(drillDownQuery.toString());

    context.setQuery(drillDownQuery);
    context.setCollector(buildCollector(context, searchRequest));
    return context;
  }

  /** Get map containing all {@link FieldDef} needed for this query */
  private static Map<String, FieldDef> getQueryFields(
      SearchContext context, SearchRequest searchRequest) {
    Map<String, FieldDef> queryFields = getVirtualFields(context.shardState(), searchRequest);
    addRetrieveFields(context, searchRequest, queryFields);
    addSortFields(context, searchRequest, queryFields);
    return queryFields;
  }

  /**
   * Parses any virtualFields, which define dynamic (expression) fields for this one request.
   *
   * @throws IllegalArgumentException if there are multiple virtual fiels with the same name
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
      Map<String, Object> params =
          Maps.transformValues(vf.getScript().getParamsMap(), ScriptParamsTransformer.INSTANCE);
      FieldDef virtualField =
          new VirtualFieldDef(vf.getName(), factory.newFactory(params, indexState.docLookup));
      virtualFields.put(vf.getName(), virtualField);
    }
    return virtualFields;
  }

  /**
   * Add specified retrieve fields to given query fields map.
   *
   * @param context search context
   * @param searchRequest request
   * @param queryFields mutable current map of query fields
   * @throws IllegalArgumentException if any retrieve field already exists
   */
  private static void addRetrieveFields(
      SearchContext context, SearchRequest searchRequest, Map<String, FieldDef> queryFields) {
    for (String field : searchRequest.getRetrieveFieldsList()) {
      FieldDef fieldDef = getFieldDef(field, context);
      FieldDef current = queryFields.put(field, fieldDef);
      if (current != null) {
        throw new IllegalArgumentException("QueryFields: " + field + " specified multiple times");
      }
    }
  }

  /**
   * Add specified sort fields to given query fields map.
   *
   * @param context search context
   * @param searchRequest request
   * @param queryFields mutable current map of query fields
   */
  private static void addSortFields(
      SearchContext context, SearchRequest searchRequest, Map<String, FieldDef> queryFields) {
    if (searchRequest.hasQuerySort() && searchRequest.getQuerySort().hasFields()) {
      for (SortType sortType : searchRequest.getQuerySort().getFields().getSortedFieldsList()) {
        if (queryFields.containsKey(sortType.getFieldName())) {
          continue;
        }
        if (!SortParser.SPECIAL_FIELDS.contains(sortType.getFieldName())) {
          FieldDef fieldDef = getFieldDef(sortType.getFieldName(), context);
          queryFields.put(sortType.getFieldName(), fieldDef);
        }
      }
    }
  }

  /** Get set of sort field identifiers */
  private static Set<String> getSortFieldNames(SearchRequest searchRequest) {
    if (searchRequest.hasQuerySort() && searchRequest.getQuerySort().hasFields()) {
      return searchRequest.getQuerySort().getFields().getSortedFieldsList().stream()
          .map(SortType::getFieldName)
          .collect(Collectors.toUnmodifiableSet());
    }
    return Collections.emptySet();
  }

  /**
   * Find {@link FieldDef} for field name.
   *
   * @param name name of field
   * @param context search context
   * @return definition object for field
   * @throws IllegalArgumentException if field does not exist or is not retrievable
   */
  private static FieldDef getFieldDef(String name, SearchContext context) {
    FieldDef fieldDef = context.indexState().getField(name);
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
    // TOOD: support "drillDowns" in input SearchRequest
    // Always create a DrillDownQuery; if there
    // are no drill-downs it will just rewrite to the
    // original query:
    return new DrillDownQuery(state.facetsConfig, q);
  }

  /**
   * Build {@link DocCollector} to provide the {@link org.apache.lucene.search.CollectorManager} for
   * this query.
   *
   * @param context search context
   * @param searchRequest request
   * @return collector
   */
  private static DocCollector buildCollector(SearchContext context, SearchRequest searchRequest) {
    if (searchRequest.getQuerySort().getFields().getSortedFieldsList().isEmpty()) {
      return new ScoreCollector(context, searchRequest);
    } else if (hasLargeNumHits(searchRequest)) {
      return new LargeNumHitsCollector(searchRequest);
    } else {
      return new FieldCollector(context, searchRequest);
    }
  }

  /** If this query needs enough hits to use a {@link LargeNumHitsCollector}. */
  private static boolean hasLargeNumHits(SearchRequest searchRequest) {
    return searchRequest.hasQuery()
        && searchRequest.getQuery().getQueryNodeCase()
            == com.yelp.nrtsearch.server.grpc.Query.QueryNodeCase.QUERYNODE_NOT_SET;
  }
}
