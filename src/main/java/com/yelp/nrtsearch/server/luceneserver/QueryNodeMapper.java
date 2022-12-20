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
package com.yelp.nrtsearch.server.luceneserver;

import static com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator.isAnalyzerDefined;

import com.yelp.nrtsearch.server.grpc.ExistsQuery;
import com.yelp.nrtsearch.server.grpc.FunctionFilterQuery;
import com.yelp.nrtsearch.server.grpc.GeoBoundingBoxQuery;
import com.yelp.nrtsearch.server.grpc.GeoPointQuery;
import com.yelp.nrtsearch.server.grpc.GeoRadiusQuery;
import com.yelp.nrtsearch.server.grpc.MatchOperator;
import com.yelp.nrtsearch.server.grpc.MatchPhraseQuery;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.MultiMatchQuery;
import com.yelp.nrtsearch.server.grpc.PrefixQuery;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.RewriteMethod;
import com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.GeoQueryable;
import com.yelp.nrtsearch.server.luceneserver.field.properties.PolygonQueryable;
import com.yelp.nrtsearch.server.luceneserver.field.properties.RangeQueryable;
import com.yelp.nrtsearch.server.luceneserver.field.properties.TermQueryable;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.luceneserver.search.query.MatchPhrasePrefixQuery;
import com.yelp.nrtsearch.server.luceneserver.search.query.multifunction.MultiFunctionScoreQuery;
import com.yelp.nrtsearch.server.utils.ScriptParamsUtils;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionMatchQuery;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.FuzzyCompletionQuery;
import org.apache.lucene.search.suggest.document.MyContextQuery;
import org.apache.lucene.search.suggest.document.PrefixCompletionQuery;
import org.apache.lucene.util.QueryBuilder;

/** This class maps our GRPC Query object to a Lucene Query object. */
public class QueryNodeMapper {

  private static final QueryNodeMapper INSTANCE = new QueryNodeMapper();

  public static QueryNodeMapper getInstance() {
    return INSTANCE;
  }

  private final Map<com.yelp.nrtsearch.server.grpc.BooleanClause.Occur, BooleanClause.Occur>
      occurMapping = initializeOccurMapping();
  private final Map<MatchOperator, BooleanClause.Occur> matchOperatorOccurMapping =
      new EnumMap<>(
          Map.of(
              MatchOperator.SHOULD, BooleanClause.Occur.SHOULD,
              MatchOperator.MUST, BooleanClause.Occur.MUST));

  public Query getQuery(com.yelp.nrtsearch.server.grpc.Query query, IndexState state) {
    Query queryNode = getQueryNode(query, state);

    if (query.getBoost() < 0) {
      throw new IllegalArgumentException("Boost must be a positive number");
    }

    if (query.getBoost() > 0) {
      return new BoostQuery(queryNode, query.getBoost());
    }
    return queryNode;
  }

  public Query applyQueryNestedPath(Query query, String path) {
    if (path == null || path.length() == 0) {
      path = IndexState.ROOT;
    }
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(new TermQuery(new Term(IndexState.NESTED_PATH, path)), BooleanClause.Occur.FILTER);
    builder.add(query, BooleanClause.Occur.MUST);
    return builder.build();
  }

  private Query getQueryNode(com.yelp.nrtsearch.server.grpc.Query query, IndexState state) {
    switch (query.getQueryNodeCase()) {
      case BOOLEANQUERY:
        return getBooleanQuery(query.getBooleanQuery(), state);
      case PHRASEQUERY:
        return getPhraseQuery(query.getPhraseQuery());
      case FUNCTIONSCOREQUERY:
        return getFunctionScoreQuery(query.getFunctionScoreQuery(), state);
      case TERMQUERY:
        return getTermQuery(query.getTermQuery(), state);
      case TERMINSETQUERY:
        return getTermInSetQuery(query.getTermInSetQuery(), state);
      case DISJUNCTIONMAXQUERY:
        return getDisjunctionMaxQuery(query.getDisjunctionMaxQuery(), state);
      case MATCHQUERY:
        return getMatchQuery(query.getMatchQuery(), state);
      case MATCHPHRASEQUERY:
        return getMatchPhraseQuery(query.getMatchPhraseQuery(), state);
      case MULTIMATCHQUERY:
        return getMultiMatchQuery(query.getMultiMatchQuery(), state);
      case RANGEQUERY:
        return getRangeQuery(query.getRangeQuery(), state);
      case GEOBOUNDINGBOXQUERY:
        return getGeoBoundingBoxQuery(query.getGeoBoundingBoxQuery(), state);
      case GEOPOINTQUERY:
        return getGeoPointQuery(query.getGeoPointQuery(), state);
      case NESTEDQUERY:
        return getNestedQuery(query.getNestedQuery(), state);
      case EXISTSQUERY:
        return getExistsQuery(query.getExistsQuery(), state);
      case GEORADIUSQUERY:
        return getGeoRadiusQuery(query.getGeoRadiusQuery(), state);
      case FUNCTIONFILTERQUERY:
        return getFunctionFilterQuery(query.getFunctionFilterQuery(), state);
      case COMPLETIONQUERY:
        return getCompletionQuery(query.getCompletionQuery(), state);
      case MULTIFUNCTIONSCOREQUERY:
        return MultiFunctionScoreQuery.build(query.getMultiFunctionScoreQuery(), state);
      case MATCHPHRASEPREFIXQUERY:
        return MatchPhrasePrefixQuery.build(query.getMatchPhrasePrefixQuery(), state);
      case PREFIXQUERY:
        return getPrefixQuery(query.getPrefixQuery(), state);
      case QUERYNODE_NOT_SET:
        return new MatchAllDocsQuery();
      default:
        throw new UnsupportedOperationException(
            "Unsupported query type received: " + query.getQueryNodeCase());
    }
  }

  private Query getCompletionQuery(
      com.yelp.nrtsearch.server.grpc.CompletionQuery completionQueryDef, IndexState state) {
    CompletionQuery completionQuery;
    switch (completionQueryDef.getQueryType()) {
      case PREFIX_QUERY:
        completionQuery =
            new PrefixCompletionQuery(
                state.searchAnalyzer,
                new Term(completionQueryDef.getField(), completionQueryDef.getText()));
        break;
      case FUZZY_QUERY:
        completionQuery =
            new FuzzyCompletionQuery(
                state.searchAnalyzer,
                new Term(completionQueryDef.getField(), completionQueryDef.getText()));
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported suggest query type received: " + completionQueryDef.getQueryType());
    }
    MyContextQuery contextQuery = new MyContextQuery(completionQuery);
    contextQuery.addContexts(completionQueryDef.getContextsList());
    return contextQuery;
  }

  private Query getNestedQuery(
      com.yelp.nrtsearch.server.grpc.NestedQuery nestedQuery, IndexState state) {
    Query childRawQuery = getQuery(nestedQuery.getQuery(), state);
    Query childQuery =
        new BooleanQuery.Builder()
            .add(
                new TermQuery(new Term(IndexState.NESTED_PATH, nestedQuery.getPath())),
                BooleanClause.Occur.FILTER)
            .add(childRawQuery, BooleanClause.Occur.MUST)
            .build();
    Query parentQuery = new TermQuery(new Term(IndexState.NESTED_PATH, IndexState.ROOT));
    return new ToParentBlockJoinQuery(
        childQuery, new QueryBitSetProducer(parentQuery), getScoreMode(nestedQuery));
  }

  private ScoreMode getScoreMode(com.yelp.nrtsearch.server.grpc.NestedQuery nestedQuery) {
    switch (nestedQuery.getScoreMode()) {
      case NONE:
        return ScoreMode.None;
      case AVG:
        return ScoreMode.Avg;
      case MAX:
        return ScoreMode.Max;
      case MIN:
        return ScoreMode.Min;
      case SUM:
        return ScoreMode.Total;
      default:
        throw new UnsupportedOperationException(
            "Unsupported score mode received: " + nestedQuery.getScoreMode());
    }
  }

  private BooleanQuery getBooleanQuery(
      com.yelp.nrtsearch.server.grpc.BooleanQuery booleanQuery, IndexState state) {
    BooleanQuery.Builder builder =
        new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(booleanQuery.getMinimumNumberShouldMatch());

    AtomicBoolean allMustNot = new AtomicBoolean(true);
    booleanQuery
        .getClausesList()
        .forEach(
            clause -> {
              com.yelp.nrtsearch.server.grpc.BooleanClause.Occur occur = clause.getOccur();
              builder.add(getQuery(clause.getQuery(), state), occurMapping.get(occur));
              if (occur != com.yelp.nrtsearch.server.grpc.BooleanClause.Occur.MUST_NOT) {
                allMustNot.set(false);
              }
            });

    if (allMustNot.get()) {
      builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER);
    }
    return builder.build();
  }

  private PhraseQuery getPhraseQuery(com.yelp.nrtsearch.server.grpc.PhraseQuery phraseQuery) {
    PhraseQuery.Builder builder = new PhraseQuery.Builder().setSlop(phraseQuery.getSlop());

    phraseQuery.getTermsList().forEach(term -> builder.add(new Term(phraseQuery.getField(), term)));

    return builder.build();
  }

  private FunctionScoreQuery getFunctionScoreQuery(
      com.yelp.nrtsearch.server.grpc.FunctionScoreQuery functionScoreQuery, IndexState state) {
    ScoreScript.Factory scriptFactory =
        ScriptService.getInstance().compile(functionScoreQuery.getScript(), ScoreScript.CONTEXT);

    Map<String, Object> params =
        ScriptParamsUtils.decodeParams(functionScoreQuery.getScript().getParamsMap());
    return new FunctionScoreQuery(
        getQuery(functionScoreQuery.getQuery(), state),
        scriptFactory.newFactory(params, state.docLookup));
  }

  private FunctionMatchQuery getFunctionFilterQuery(
      FunctionFilterQuery functionFilterQuery, IndexState state) {
    ScoreScript.Factory scriptFactory =
        ScriptService.getInstance().compile(functionFilterQuery.getScript(), ScoreScript.CONTEXT);
    Map<String, Object> params =
        ScriptParamsUtils.decodeParams(functionFilterQuery.getScript().getParamsMap());
    return new FunctionMatchQuery(
        scriptFactory.newFactory(params, state.docLookup), score -> score > 0);
  }

  private Query getTermQuery(com.yelp.nrtsearch.server.grpc.TermQuery termQuery, IndexState state) {
    String fieldName = termQuery.getField();
    FieldDef fieldDef = state.getField(fieldName);

    if (fieldDef instanceof TermQueryable) {
      validateTermQueryIsSearchable(fieldDef);
      return ((TermQueryable) fieldDef).getTermQuery(termQuery);
    }

    String message =
        "Unable to create TermQuery: %s, field type: %s is not supported for TermQuery";
    throw new IllegalArgumentException(String.format(message, termQuery, fieldDef.getType()));
  }

  private void validateTermQueryIsSearchable(FieldDef fieldDef) {
    if (fieldDef instanceof IndexableFieldDef && !((IndexableFieldDef) fieldDef).isSearchable()) {
      throw new IllegalStateException(
          "Field "
              + fieldDef.getName()
              + " is not searchable, which is required for TermQuery / TermInSetQuery");
    }
  }

  private Query getTermInSetQuery(
      com.yelp.nrtsearch.server.grpc.TermInSetQuery termInSetQuery, IndexState state) {
    String fieldName = termInSetQuery.getField();
    FieldDef fieldDef = state.getField(fieldName);

    if (fieldDef instanceof TermQueryable) {
      validateTermQueryIsSearchable(fieldDef);
      return ((TermQueryable) fieldDef).getTermInSetQuery(termInSetQuery);
    }

    String message =
        "Unable to create TermInSetQuery: %s, field type: %s is not supported for TermInSetQuery";
    throw new IllegalArgumentException(String.format(message, termInSetQuery, fieldDef.getType()));
  }

  private DisjunctionMaxQuery getDisjunctionMaxQuery(
      com.yelp.nrtsearch.server.grpc.DisjunctionMaxQuery disjunctionMaxQuery, IndexState state) {
    List<Query> disjuncts =
        disjunctionMaxQuery.getDisjunctsList().stream()
            .map(query -> getQuery(query, state))
            .collect(Collectors.toList());
    return new DisjunctionMaxQuery(disjuncts, disjunctionMaxQuery.getTieBreakerMultiplier());
  }

  private Query getMatchQuery(MatchQuery matchQuery, IndexState state) {
    Analyzer analyzer =
        isAnalyzerDefined(matchQuery.getAnalyzer())
            ? AnalyzerCreator.getInstance().getAnalyzer(matchQuery.getAnalyzer())
            : state.searchAnalyzer;

    QueryBuilder queryBuilder = new MatchQueryBuilder(analyzer, matchQuery.getFuzzyParams());

    // This created query will be TermQuery or FuzzyQuery if only one token is found after analysis,
    // otherwise BooleanQuery. The BooleanQuery may include clauses with TermQuery or FuzzyQuery.
    Query query =
        queryBuilder.createBooleanQuery(
            matchQuery.getField(),
            matchQuery.getQuery(),
            matchOperatorOccurMapping.get(matchQuery.getOperator()));

    // This can happen if there are no tokens found after analyzing the query text
    if (query == null) {
      return new MatchNoDocsQuery();
    }

    // TODO: investigate using createMinShouldMatchQuery instead
    if (matchQuery.getMinimumNumberShouldMatch() == 0
        || query instanceof TermQuery
        || query instanceof FuzzyQuery) {
      return query;
    }

    BooleanQuery.Builder builder =
        new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(matchQuery.getMinimumNumberShouldMatch());

    ((BooleanQuery) query).clauses().forEach(builder::add);

    return builder.build();
  }

  private Query getMatchPhraseQuery(MatchPhraseQuery matchPhraseQuery, IndexState state) {
    Analyzer analyzer =
        isAnalyzerDefined(matchPhraseQuery.getAnalyzer())
            ? AnalyzerCreator.getInstance().getAnalyzer(matchPhraseQuery.getAnalyzer())
            : state.searchAnalyzer;

    QueryBuilder queryBuilder = new QueryBuilder(analyzer);
    // This created query will be TermQuery if only one token is found after analysis, otherwise
    // PhraseQuery
    Query phraseQuery =
        queryBuilder.createPhraseQuery(
            matchPhraseQuery.getField(), matchPhraseQuery.getQuery(), matchPhraseQuery.getSlop());

    // This can happen if there are no tokens found after analyzing the query text
    if (phraseQuery == null) {
      return new MatchNoDocsQuery();
    }
    return phraseQuery;
  }

  private Query getMultiMatchQuery(MultiMatchQuery multiMatchQuery, IndexState state) {
    Map<String, Float> fieldBoosts = multiMatchQuery.getFieldBoostsMap();
    Collection<String> fields;

    // Take all fields if none are provided
    if (multiMatchQuery.getFieldsList().isEmpty()) {
      fields = state.getAllFields().keySet();
    } else {
      fields = multiMatchQuery.getFieldsList();
    }

    List<Query> matchQueries =
        fields.stream()
            .map(
                field -> {
                  Query query;
                  switch (multiMatchQuery.getType()) {
                    case BEST_FIELDS:
                      MatchQuery matchQuery =
                          MatchQuery.newBuilder()
                              .setField(field)
                              .setQuery(multiMatchQuery.getQuery())
                              .setOperator(multiMatchQuery.getOperator())
                              .setMinimumNumberShouldMatch(
                                  multiMatchQuery.getMinimumNumberShouldMatch())
                              .setAnalyzer(
                                  multiMatchQuery
                                      .getAnalyzer()) // TODO: making the analyzer once and using it
                              // for
                              // all match queries would be more efficient
                              .setFuzzyParams(multiMatchQuery.getFuzzyParams())
                              .build();
                      query = getMatchQuery(matchQuery, state);
                      break;
                    case PHRASE_PREFIX:
                      query =
                          MatchPhrasePrefixQuery.build(
                              com.yelp.nrtsearch.server.grpc.MatchPhrasePrefixQuery.newBuilder()
                                  .setField(field)
                                  .setQuery(multiMatchQuery.getQuery())
                                  .setAnalyzer(multiMatchQuery.getAnalyzer())
                                  .setSlop(multiMatchQuery.getSlop())
                                  .setMaxExpansions(multiMatchQuery.getMaxExpansions())
                                  .build(),
                              state);
                      break;
                    default:
                      throw new IllegalArgumentException(
                          "Unknown multi match type: " + multiMatchQuery.getType());
                  }
                  Float boost = fieldBoosts.get(field);
                  if (boost != null) {
                    if (boost < 0) {
                      throw new IllegalArgumentException(
                          String.format(
                              "Invalid boost %f for field: %s, query: %s",
                              boost, field, multiMatchQuery));
                    }
                    return new BoostQuery(query, boost);
                  } else {
                    return query;
                  }
                })
            .collect(Collectors.toList());
    return new DisjunctionMaxQuery(matchQueries, 0);
  }

  private Query getRangeQuery(RangeQuery rangeQuery, IndexState state) {
    String fieldName = rangeQuery.getField();
    FieldDef field = state.getField(fieldName);

    if (!(field instanceof RangeQueryable)) {
      throw new IllegalArgumentException("Field: " + fieldName + " does not support RangeQuery");
    }

    return ((RangeQueryable) field).getRangeQuery(rangeQuery);
  }

  private Query getGeoBoundingBoxQuery(GeoBoundingBoxQuery geoBoundingBoxQuery, IndexState state) {
    String fieldName = geoBoundingBoxQuery.getField();
    FieldDef field = state.getField(fieldName);

    if (!(field instanceof GeoQueryable)) {
      throw new IllegalArgumentException(
          "Field: " + fieldName + " does not support GeoBoundingBoxQuery");
    }

    return ((GeoQueryable) field).getGeoBoundingBoxQuery(geoBoundingBoxQuery);
  }

  private Query getGeoRadiusQuery(GeoRadiusQuery geoRadiusQuery, IndexState state) {
    String fieldName = geoRadiusQuery.getField();
    FieldDef field = state.getField(fieldName);
    if (!(field instanceof GeoQueryable)) {
      throw new IllegalArgumentException(
          "Field: " + fieldName + " does not support GeoRadiusQuery");
    }
    return ((GeoQueryable) field).getGeoRadiusQuery(geoRadiusQuery);
  }

  private Query getGeoPointQuery(GeoPointQuery geoPolygonQuery, IndexState state) {
    String fieldName = geoPolygonQuery.getField();
    FieldDef field = state.getField(fieldName);

    if (!(field instanceof PolygonQueryable)) {
      throw new IllegalArgumentException("Field " + fieldName + "does not support GeoPolygonQuery");
    }
    return ((PolygonQueryable) field).getGeoPointQuery(geoPolygonQuery);
  }

  private Map<com.yelp.nrtsearch.server.grpc.BooleanClause.Occur, BooleanClause.Occur>
      initializeOccurMapping() {
    return Arrays.stream(com.yelp.nrtsearch.server.grpc.BooleanClause.Occur.values())
        .filter(v -> v != com.yelp.nrtsearch.server.grpc.BooleanClause.Occur.UNRECOGNIZED)
        .collect(
            () -> new EnumMap<>(com.yelp.nrtsearch.server.grpc.BooleanClause.Occur.class),
            (map, v) -> map.put(v, BooleanClause.Occur.valueOf(v.name())),
            EnumMap::putAll);
  }

  private Query getExistsQuery(ExistsQuery existsQuery, IndexState state) {
    String fieldName = existsQuery.getField();
    return new ConstantScoreQuery(new TermQuery(new Term(IndexState.FIELD_NAMES, fieldName)));
  }

  private static Query getPrefixQuery(PrefixQuery prefixQuery, IndexState state) {
    FieldDef fieldDef = state.getField(prefixQuery.getField());
    if (!(fieldDef instanceof IndexableFieldDef)) {
      throw new IllegalArgumentException(
          "Field \"" + prefixQuery.getPrefix() + "\" is not indexable");
    }
    IndexOptions indexOptions = ((IndexableFieldDef) fieldDef).getFieldType().indexOptions();
    if (indexOptions == IndexOptions.NONE) {
      throw new IllegalArgumentException(
          "Field \"" + prefixQuery.getField() + "\" is not indexed with terms");
    }

    org.apache.lucene.search.PrefixQuery query =
        new org.apache.lucene.search.PrefixQuery(
            new Term(prefixQuery.getField(), prefixQuery.getPrefix()));
    query.setRewriteMethod(
        getRewriteMethod(prefixQuery.getRewrite(), prefixQuery.getRewriteTopTermsSize()));
    return query;
  }

  private static MultiTermQuery.RewriteMethod getRewriteMethod(
      RewriteMethod rewriteMethodGrpc, int topTermsSize) {
    switch (rewriteMethodGrpc) {
      case CONSTANT_SCORE:
        return MultiTermQuery.CONSTANT_SCORE_REWRITE;
      case CONSTANT_SCORE_BOOLEAN:
        return MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE;
      case SCORING_BOOLEAN:
        return MultiTermQuery.SCORING_BOOLEAN_REWRITE;
      case TOP_TERMS_BLENDED_FREQS:
        return new MultiTermQuery.TopTermsBlendedFreqScoringRewrite(topTermsSize);
      case TOP_TERMS_BOOST:
        return new MultiTermQuery.TopTermsBoostOnlyBooleanQueryRewrite(topTermsSize);
      case TOP_TERMS:
        return new MultiTermQuery.TopTermsScoringBooleanQueryRewrite(topTermsSize);
      default:
        throw new IllegalArgumentException("Unknown rewrite method: " + rewriteMethodGrpc);
    }
  }
}
