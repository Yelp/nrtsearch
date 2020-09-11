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
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.VirtualField;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.facet.DrillSidewaysImpl;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptParamsTransformer;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreator;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CustomCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.DocCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.FieldCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.LargeNumHitsCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.ScoreCollector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.Collector;
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
   * @param threadPoolExecutor search handler executor
   * @return context info needed to execute the search query
   * @throws IOException if query rewrite fails
   */
  public static SearchContext buildContextForRequest(
      SearchRequest searchRequest,
      IndexState indexState,
      ShardState shardState,
      SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy,
      SearchResponse.Diagnostics.Builder diagnostics,
      ThreadPoolExecutor threadPoolExecutor)
      throws IOException {

    MutableSearchContext context =
        new MutableSearchContext(
            indexState, shardState, searcherAndTaxonomy, SearchResponse.newBuilder());

    context.setTimestampSec(System.currentTimeMillis() / 1000);
    context.setStartHit(searchRequest.getStartHit());

    Map<String, FieldDef> queryVirtualFields = getVirtualFields(shardState, searchRequest);

    Map<String, FieldDef> queryFields = new HashMap<>(queryVirtualFields);
    addIndexFields(context, queryFields);
    context.setQueryFields(Collections.unmodifiableMap(queryFields));

    Map<String, FieldDef> retrieveFields = new HashMap<>(queryVirtualFields);
    addRetrieveFields(context, searchRequest, retrieveFields);
    context.setRetrieveFields(Collections.unmodifiableMap(retrieveFields));

    Query query = extractQuery(context.indexState(), searchRequest);
    diagnostics.setParsedQuery(query.toString());

    query = context.searcherAndTaxonomy().searcher.rewrite(query);
    diagnostics.setRewrittenQuery(query.toString());

    if (searchRequest.getFacetsCount() > 0) {
      query = addDrillDowns(context.indexState(), query);
      diagnostics.setDrillDownQuery(query.toString());

      context.setDrillSideways(
          new DrillSidewaysImpl(
              indexState.facetsConfig,
              searchRequest.getFacetsList(),
              searcherAndTaxonomy,
              shardState,
              context.queryFields(),
              context.searchResponse(),
              threadPoolExecutor));
    }

    context.setQuery(query);
    context.setCollectorManager(buildCollectorManager(context, searchRequest));
    context.searchResponse().setDiagnostics(diagnostics);
    return context.freeze();
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
   * Add index fields to given query fields map.
   *
   * @param context search context
   * @param queryFields mutable current map of query fields
   * @throws IllegalArgumentException if any index field already exists
   */
  private static void addIndexFields(SearchContext context, Map<String, FieldDef> queryFields) {
    for (Map.Entry<String, FieldDef> entry : context.indexState().getAllFields().entrySet()) {
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
   * Build the primary {@link org.apache.lucene.search.CollectorManager} to pass to the {@link
   * org.apache.lucene.search.IndexSearcher}. This will handle collection of the top documents and
   * any additional custom collectors.
   *
   * @param context search context
   * @param searchRequest request
   * @return primary query collector manager
   */
  private static SearchCollectorManager buildCollectorManager(
      SearchContext context, SearchRequest searchRequest) {
    DocCollector docCollector = buildDocCollector(context, searchRequest);
    if (searchRequest.getCollectorsCount() > 0) {
      List<CustomCollectorManager<? extends Collector, ? extends CollectorResult>>
          collectorManagers = new ArrayList<>(searchRequest.getCollectorsCount());
      for (Map.Entry<String, com.yelp.nrtsearch.server.grpc.Collector> entry :
          searchRequest.getCollectorsMap().entrySet()) {
        collectorManagers.add(
            CollectorCreator.createCollectorManager(context, entry.getKey(), entry.getValue()));
      }
      return new SearchCollectorManager(docCollector, collectorManagers);
    } else {
      return new SearchCollectorManager(docCollector, Collections.emptyList());
    }
  }

  /**
   * Build {@link DocCollector} to provide the {@link org.apache.lucene.search.CollectorManager} for
   * collecting hits for this query.
   *
   * @param context search context
   * @param searchRequest request
   * @return collector
   */
  private static DocCollector buildDocCollector(
      SearchContext context, SearchRequest searchRequest) {
    if (searchRequest.getQuerySort().getFields().getSortedFieldsList().isEmpty()) {
      if (hasLargeNumHits(searchRequest)) {
        return new LargeNumHitsCollector(searchRequest);
      } else {
        return new ScoreCollector(context, searchRequest);
      }
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
