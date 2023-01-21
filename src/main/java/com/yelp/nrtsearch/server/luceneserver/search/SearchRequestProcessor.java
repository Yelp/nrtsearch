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

import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.Highlight;
import com.yelp.nrtsearch.server.grpc.PluginRescorer;
import com.yelp.nrtsearch.server.grpc.ProfileResult;
import com.yelp.nrtsearch.server.grpc.QueryRescorer;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.VirtualField;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.QueryNodeMapper;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.doc.DefaultSharedDocContext;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.TextBaseFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.luceneserver.highlights.HighlightFetchTask;
import com.yelp.nrtsearch.server.luceneserver.rescore.QueryRescore;
import com.yelp.nrtsearch.server.luceneserver.rescore.RescoreOperation;
import com.yelp.nrtsearch.server.luceneserver.rescore.RescoreTask;
import com.yelp.nrtsearch.server.luceneserver.rescore.RescorerCreator;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.AdditionalCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreator;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreatorContext;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.DocCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.HitCountCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.LargeNumHitsCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.MyTopSuggestDocsCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.RelevanceCollector;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.SortFieldCollector;
import com.yelp.nrtsearch.server.utils.ScriptParamsUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
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

  public static final String WILDCARD = "*";

  private static final QueryNodeMapper QUERY_NODE_MAPPER = QueryNodeMapper.getInstance();

  private SearchRequestProcessor() {}

  /**
   * Create a {@link SearchContext} representing the given {@link SearchRequest} and index/searcher
   * state.
   *
   * @param searchRequest grpc request message
   * @param indexState index state
   * @param shardState shard state
   * @param searcherAndTaxonomy index searcher
   * @param profileResult container message for returned debug info
   * @return context info needed to execute the search query
   * @throws IOException if query rewrite fails
   */
  public static SearchContext buildContextForRequest(
      SearchRequest searchRequest,
      IndexState indexState,
      ShardState shardState,
      SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy,
      ProfileResult.Builder profileResult)
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

    Map<String, FieldDef> queryVirtualFields = getVirtualFields(indexState, searchRequest);

    Map<String, FieldDef> queryFields = new HashMap<>(queryVirtualFields);
    addIndexFields(indexState, queryFields);
    contextBuilder.setQueryFields(Collections.unmodifiableMap(queryFields));

    Map<String, FieldDef> retrieveFields = getRetrieveFields(searchRequest, queryFields);
    contextBuilder.setRetrieveFields(Collections.unmodifiableMap(retrieveFields));

    Query query = extractQuery(indexState, searchRequest);
    if (profileResult != null) {
      profileResult.setParsedQuery(query.toString());
    }

    query = searcherAndTaxonomy.searcher.rewrite(query);
    if (profileResult != null) {
      profileResult.setRewrittenQuery(query.toString());
    }

    if (searchRequest.getFacetsCount() > 0) {
      query = addDrillDowns(indexState, query);
      if (profileResult != null) {
        profileResult.setDrillDownQuery(query.toString());
      }
    }

    contextBuilder.setFetchTasks(new FetchTasks(searchRequest.getFetchTasksList()));

    contextBuilder.setQuery(query);

    CollectorCreatorContext collectorCreatorContext =
        new CollectorCreatorContext(
            searchRequest, indexState, shardState, queryFields, searcherAndTaxonomy);
    DocCollector docCollector = buildDocCollector(collectorCreatorContext);
    contextBuilder.setCollector(docCollector);

    contextBuilder.setRescorers(
        getRescorers(indexState, searcherAndTaxonomy.searcher, searchRequest));
    contextBuilder.setSharedDocContext(new DefaultSharedDocContext());

    Highlight highlight = searchRequest.getHighlight();
    if (!highlight.getFieldsList().isEmpty()) {
      verifyHighlights(indexState, highlight);
      HighlightFetchTask highlightFetchTask =
          new HighlightFetchTask(indexState, searcherAndTaxonomy, query, highlight);
      contextBuilder.setHighlightFetchTask(highlightFetchTask);
    }

    SearchContext searchContext = contextBuilder.build(true);
    // Give underlying collectors access to the search context
    docCollector.setSearchContext(searchContext);
    return searchContext;
  }

  /**
   * Parses any virtualFields, which define dynamic (expression) fields for this one request.
   *
   * @throws IllegalArgumentException if there are multiple virtual fields with the same name
   */
  private static Map<String, FieldDef> getVirtualFields(
      IndexState indexState, SearchRequest searchRequest) {
    if (searchRequest.getVirtualFieldsList().isEmpty()) {
      return new HashMap<>();
    }

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
   * Get map of fields that need to be retrieved for the given request.
   *
   * @param request search requests
   * @param queryFields all valid fields for this query
   * @return map of all fields to retrieve
   * @throws IllegalArgumentException if a field does not exist, or is not retrievable
   */
  private static Map<String, FieldDef> getRetrieveFields(
      SearchRequest request, Map<String, FieldDef> queryFields) {
    Map<String, FieldDef> retrieveFields = new HashMap<>();
    if (request.getRetrieveFieldsCount() == 1 && request.getRetrieveFields(0).equals(WILDCARD)) {
      for (Entry<String, FieldDef> entry : queryFields.entrySet()) {
        if (isRetrievable(entry.getValue())) {
          retrieveFields.put(entry.getKey(), entry.getValue());
        }
      }
      return retrieveFields;
    }
    for (String field : request.getRetrieveFieldsList()) {
      FieldDef fieldDef = queryFields.get(field);
      if (fieldDef == null) {
        throw new IllegalArgumentException("RetrieveFields: " + field + " does not exist");
      }
      if (!isRetrievable(fieldDef)) {
        throw new IllegalArgumentException(
            "RetrieveFields: "
                + field
                + " is not retrievable, must be stored"
                + " or have doc values enabled");
      }
      retrieveFields.put(field, fieldDef);
    }
    return retrieveFields;
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
    } else {
      q = QUERY_NODE_MAPPER.getQuery(searchRequest.getQuery(), state);
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
    return new DrillDownQuery(state.getFacetsConfig(), q);
  }

  /**
   * Build {@link DocCollector} to provide the {@link org.apache.lucene.search.CollectorManager} for
   * collecting hits for this query.
   *
   * @return collector
   */
  private static DocCollector buildDocCollector(CollectorCreatorContext collectorCreatorContext) {
    SearchRequest searchRequest = collectorCreatorContext.getRequest();
    List<AdditionalCollectorManager<? extends Collector, ? extends CollectorResult>>
        additionalCollectors =
            searchRequest.getCollectorsMap().entrySet().stream()
                .map(
                    e ->
                        CollectorCreator.getInstance()
                            .createCollectorManager(
                                collectorCreatorContext, e.getKey(), e.getValue()))
                .collect(Collectors.toList());

    DocCollector docCollector;
    if (searchRequest.getQuery().hasCompletionQuery()) {
      docCollector = new MyTopSuggestDocsCollector(collectorCreatorContext, additionalCollectors);
    } else if (searchRequest.getQuerySort().getFields().getSortedFieldsList().isEmpty()) {
      if (hasLargeNumHits(searchRequest)) {
        docCollector = new LargeNumHitsCollector(collectorCreatorContext, additionalCollectors);
      } else {
        docCollector = new RelevanceCollector(collectorCreatorContext, additionalCollectors);
      }
    } else {
      docCollector = new SortFieldCollector(collectorCreatorContext, additionalCollectors);
    }
    // If we don't need hits, just count recalled docs
    if (docCollector.getNumHitsToCollect() == 0) {
      docCollector = new HitCountCollector(collectorCreatorContext, additionalCollectors);
    }
    return docCollector;
  }

  /** If this query needs enough hits to use a {@link LargeNumHitsCollector}. */
  private static boolean hasLargeNumHits(SearchRequest searchRequest) {
    return searchRequest.hasQuery()
        && searchRequest.getQuery().getQueryNodeCase()
            == com.yelp.nrtsearch.server.grpc.Query.QueryNodeCase.QUERYNODE_NOT_SET;
  }

  /** Parses rescorers defined in this search request. */
  private static List<RescoreTask> getRescorers(
      IndexState indexState, IndexSearcher searcher, SearchRequest searchRequest)
      throws IOException {

    List<RescoreTask> rescorers = new ArrayList<>();

    for (int i = 0; i < searchRequest.getRescorersList().size(); ++i) {
      com.yelp.nrtsearch.server.grpc.Rescorer rescorer = searchRequest.getRescorers(i);
      String rescorerName = rescorer.getName();
      RescoreOperation thisRescoreOperation;

      if (rescorer.hasQueryRescorer()) {
        QueryRescorer queryRescorer = rescorer.getQueryRescorer();
        Query query = QUERY_NODE_MAPPER.getQuery(queryRescorer.getRescoreQuery(), indexState);
        query = searcher.rewrite(query);

        thisRescoreOperation =
            QueryRescore.newBuilder()
                .setQuery(query)
                .setQueryWeight(queryRescorer.getQueryWeight())
                .setRescoreQueryWeight(queryRescorer.getRescoreQueryWeight())
                .build();
      } else if (rescorer.hasPluginRescorer()) {
        PluginRescorer plugin = rescorer.getPluginRescorer();
        thisRescoreOperation = RescorerCreator.getInstance().createRescorer(plugin);
      } else {
        throw new IllegalArgumentException(
            "Rescorer should define either QueryRescorer or PluginRescorer");
      }

      rescorers.add(
          RescoreTask.newBuilder()
              .setRescoreOperation(thisRescoreOperation)
              .setWindowSize(rescorer.getWindowSize())
              .setName(
                  rescorerName != null && !rescorerName.equals("")
                      ? rescorerName
                      : String.format("rescorer_%d", i))
              .build());
    }
    return rescorers;
  }

  private static void verifyHighlights(IndexState indexState, Highlight highlight) {
    for (String fieldName : highlight.getFieldsList()) {
      FieldDef field = indexState.getField(fieldName);
      if (!(field instanceof TextBaseFieldDef)) {
        throw new IllegalArgumentException(
            String.format(
                "Field %s is not a text field and does not support highlights", fieldName));
      }
      if (!((TextBaseFieldDef) field).isSearchable()) {
        throw new IllegalArgumentException(
            String.format("Field %s is not searchable and cannot support highlights", fieldName));
      }
      if (!((TextBaseFieldDef) field).isStored()) {
        throw new IllegalArgumentException(
            String.format("Field %s is not stored and cannot support highlights", fieldName));
      }
      FieldType fieldType = ((TextBaseFieldDef) field).getFieldType();
      if (!fieldType.storeTermVectors()
          || !fieldType.storeTermVectorPositions()
          || !fieldType.storeTermVectorOffsets()) {
        throw new IllegalArgumentException(
            String.format(
                "Field %s does not have term vectors with positions and offsets and cannot support highlights",
                fieldName));
      }
    }
  }
}
