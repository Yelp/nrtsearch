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
package com.yelp.nrtsearch.server.query;

import static com.yelp.nrtsearch.server.analysis.AnalyzerCreator.isAnalyzerDefined;

import com.yelp.nrtsearch.server.analysis.AnalyzerCreator;
import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.TextBaseFieldDef;
import com.yelp.nrtsearch.server.field.properties.*;
import com.yelp.nrtsearch.server.grpc.ExistsQuery;
import com.yelp.nrtsearch.server.grpc.FunctionFilterQuery;
import com.yelp.nrtsearch.server.grpc.GeoBoundingBoxQuery;
import com.yelp.nrtsearch.server.grpc.GeoPointQuery;
import com.yelp.nrtsearch.server.grpc.GeoPolygonQuery;
import com.yelp.nrtsearch.server.grpc.GeoRadiusQuery;
import com.yelp.nrtsearch.server.grpc.MatchOperator;
import com.yelp.nrtsearch.server.grpc.MatchPhraseQuery;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.MultiMatchQuery;
import com.yelp.nrtsearch.server.grpc.MultiMatchQuery.MatchType;
import com.yelp.nrtsearch.server.grpc.PrefixQuery;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.RewriteMethod;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.query.multifunction.MultiFunctionScoreQuery;
import com.yelp.nrtsearch.server.script.ScoreScript;
import com.yelp.nrtsearch.server.script.ScriptService;
import com.yelp.nrtsearch.server.utils.ScriptParamsUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionMatchQuery;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
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
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.FuzzyCompletionQuery;
import org.apache.lucene.search.suggest.document.MyContextQuery;
import org.apache.lucene.search.suggest.document.PrefixCompletionQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

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
    return getQuery(query, state.docLookup);
  }

  public Query getQuery(com.yelp.nrtsearch.server.grpc.Query query, DocLookup docLookup) {
    Query queryNode = getQueryNode(query, docLookup);

    if (query.getBoost() < 0) {
      throw new IllegalArgumentException("Boost must be a positive number");
    }

    if (query.getBoost() > 0) {
      return new BoostQuery(queryNode, query.getBoost());
    }
    return queryNode;
  }

  public Query applyQueryNestedPath(Query query, IndexState indexState, String path) {
    if (path == null || path.isEmpty()) {
      return query;
    }
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(getNestedPathQuery(indexState, path), BooleanClause.Occur.FILTER);
    builder.add(query, BooleanClause.Occur.MUST);
    return builder.build();
  }

  /**
   * Create the query to filter the parent/child document based on the path.
   *
   * @param indexState index state
   * @param path nested path
   */
  public Query getNestedPathQuery(IndexState indexState, String path) {
    return getNestedPathQuery(indexState.docLookup, path);
  }

  /**
   * Create the query to filter the parent/child document based on the path.
   *
   * @param docLookup lookup for document field data
   * @param path nested path
   */
  public Query getNestedPathQuery(DocLookup docLookup, String path) {
    return new TermQuery(
        new Term(IndexState.NESTED_PATH, IndexState.resolveQueryNestedPath(path, docLookup)));
  }

  private Query getQueryNode(com.yelp.nrtsearch.server.grpc.Query query, DocLookup docLookup) {
    return switch (query.getQueryNodeCase()) {
      case BOOLEANQUERY -> getBooleanQuery(query.getBooleanQuery(), docLookup);
      case PHRASEQUERY -> getPhraseQuery(query.getPhraseQuery());
      case FUNCTIONSCOREQUERY -> getFunctionScoreQuery(query.getFunctionScoreQuery(), docLookup);
      case TERMQUERY -> getTermQuery(query.getTermQuery(), docLookup);
      case TERMINSETQUERY -> getTermInSetQuery(query.getTermInSetQuery(), docLookup);
      case DISJUNCTIONMAXQUERY -> getDisjunctionMaxQuery(query.getDisjunctionMaxQuery(), docLookup);
      case MATCHQUERY -> getMatchQuery(query.getMatchQuery(), docLookup);
      case MATCHPHRASEQUERY -> getMatchPhraseQuery(query.getMatchPhraseQuery(), docLookup);
      case MULTIMATCHQUERY -> getMultiMatchQuery(query.getMultiMatchQuery(), docLookup);
      case RANGEQUERY -> getRangeQuery(query.getRangeQuery(), docLookup);
      case GEOBOUNDINGBOXQUERY -> getGeoBoundingBoxQuery(query.getGeoBoundingBoxQuery(), docLookup);
      case GEOPOINTQUERY -> getGeoPointQuery(query.getGeoPointQuery(), docLookup);
      case NESTEDQUERY -> getNestedQuery(query.getNestedQuery(), docLookup);
      case EXISTSQUERY -> getExistsQuery(query.getExistsQuery());
      case GEORADIUSQUERY -> getGeoRadiusQuery(query.getGeoRadiusQuery(), docLookup);
      case FUNCTIONFILTERQUERY -> getFunctionFilterQuery(query.getFunctionFilterQuery(), docLookup);
      case COMPLETIONQUERY -> getCompletionQuery(query.getCompletionQuery(), docLookup);
      case MULTIFUNCTIONSCOREQUERY ->
          MultiFunctionScoreQuery.build(query.getMultiFunctionScoreQuery(), docLookup);
      case MATCHPHRASEPREFIXQUERY ->
          MatchPhrasePrefixQuery.build(query.getMatchPhrasePrefixQuery(), docLookup);
      case PREFIXQUERY -> getPrefixQuery(query.getPrefixQuery(), docLookup, false);
      case CONSTANTSCOREQUERY -> getConstantScoreQuery(query.getConstantScoreQuery(), docLookup);
      case SPANQUERY -> getSpanQuery(query.getSpanQuery(), docLookup);
      case GEOPOLYGONQUERY -> getGeoPolygonQuery(query.getGeoPolygonQuery(), docLookup);
      case EXACTVECTORQUERY -> getExactVectorQuery(query.getExactVectorQuery(), docLookup);
      case MATCHALLQUERY, QUERYNODE_NOT_SET -> new MatchAllDocsQuery();
      default ->
          throw new UnsupportedOperationException(
              "Unsupported query type received: " + query.getQueryNodeCase());
    };
  }

  private Query getCompletionQuery(
      com.yelp.nrtsearch.server.grpc.CompletionQuery completionQueryDef, DocLookup docLookup) {
    Analyzer fieldAnalyzer =
        IndexState.getFieldSearchAnalyzer(completionQueryDef.getField(), docLookup);
    CompletionQuery completionQuery =
        switch (completionQueryDef.getQueryType()) {
          case PREFIX_QUERY ->
              new PrefixCompletionQuery(
                  fieldAnalyzer,
                  new Term(completionQueryDef.getField(), completionQueryDef.getText()));
          case FUZZY_QUERY ->
              new FuzzyCompletionQuery(
                  fieldAnalyzer,
                  new Term(completionQueryDef.getField(), completionQueryDef.getText()));
          default ->
              throw new UnsupportedOperationException(
                  "Unsupported suggest query type received: " + completionQueryDef.getQueryType());
        };
    MyContextQuery contextQuery = new MyContextQuery(completionQuery);
    contextQuery.addContexts(completionQueryDef.getContextsList());
    return contextQuery;
  }

  private Query getNestedQuery(
      com.yelp.nrtsearch.server.grpc.NestedQuery nestedQuery, DocLookup docLookup) {
    Query childRawQuery = getQuery(nestedQuery.getQuery(), docLookup);
    Query childQuery =
        new BooleanQuery.Builder()
            .add(getNestedPathQuery(docLookup, nestedQuery.getPath()), BooleanClause.Occur.FILTER)
            .add(childRawQuery, BooleanClause.Occur.MUST)
            .build();
    Query parentQuery = getNestedPathQuery(docLookup, IndexState.ROOT);
    return new ToParentBlockJoinQuery(
        childQuery, new QueryBitSetProducer(parentQuery), getScoreMode(nestedQuery));
  }

  private ScoreMode getScoreMode(com.yelp.nrtsearch.server.grpc.NestedQuery nestedQuery) {
    return switch (nestedQuery.getScoreMode()) {
      case NONE -> ScoreMode.None;
      case AVG -> ScoreMode.Avg;
      case MAX -> ScoreMode.Max;
      case MIN -> ScoreMode.Min;
      case SUM -> ScoreMode.Total;
      default ->
          throw new UnsupportedOperationException(
              "Unsupported score mode received: " + nestedQuery.getScoreMode());
    };
  }

  private BooleanQuery getBooleanQuery(
      com.yelp.nrtsearch.server.grpc.BooleanQuery booleanQuery, DocLookup docLookup) {
    BooleanQuery.Builder builder =
        new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(booleanQuery.getMinimumNumberShouldMatch());

    if (booleanQuery.getClausesCount() == 0) {
      return builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST).build();
    }

    AtomicBoolean allMustNot = new AtomicBoolean(true);
    booleanQuery
        .getClausesList()
        .forEach(
            clause -> {
              com.yelp.nrtsearch.server.grpc.BooleanClause.Occur occur = clause.getOccur();
              builder.add(getQuery(clause.getQuery(), docLookup), occurMapping.get(occur));
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
      com.yelp.nrtsearch.server.grpc.FunctionScoreQuery functionScoreQuery, DocLookup docLookup) {
    ScoreScript.Factory scriptFactory =
        ScriptService.getInstance().compile(functionScoreQuery.getScript(), ScoreScript.CONTEXT);

    Map<String, Object> params =
        ScriptParamsUtils.decodeParams(functionScoreQuery.getScript().getParamsMap());
    return new FunctionScoreQuery(
        getQuery(functionScoreQuery.getQuery(), docLookup),
        scriptFactory.newFactory(params, docLookup));
  }

  private FunctionMatchQuery getFunctionFilterQuery(
      FunctionFilterQuery functionFilterQuery, DocLookup docLookup) {
    ScoreScript.Factory scriptFactory =
        ScriptService.getInstance().compile(functionFilterQuery.getScript(), ScoreScript.CONTEXT);
    Map<String, Object> params =
        ScriptParamsUtils.decodeParams(functionFilterQuery.getScript().getParamsMap());
    return new FunctionMatchQuery(scriptFactory.newFactory(params, docLookup), score -> score > 0);
  }

  private Query getTermQuery(
      com.yelp.nrtsearch.server.grpc.TermQuery termQuery, DocLookup docLookup) {
    String fieldName = termQuery.getField();
    FieldDef fieldDef = docLookup.getFieldDefOrThrow(fieldName);

    if (fieldDef instanceof TermQueryable termQueryable) {
      return termQueryable.getTermQuery(termQuery);
    }

    String message =
        "Unable to create TermQuery: %s, field type: %s is not supported for TermQuery";
    throw new IllegalArgumentException(String.format(message, termQuery, fieldDef.getType()));
  }

  private Query getTermInSetQuery(
      com.yelp.nrtsearch.server.grpc.TermInSetQuery termInSetQuery, DocLookup docLookup) {
    String fieldName = termInSetQuery.getField();
    FieldDef fieldDef = docLookup.getFieldDefOrThrow(fieldName);

    if (fieldDef instanceof TermQueryable termQueryable) {
      return termQueryable.getTermInSetQuery(termInSetQuery);
    }

    String message =
        "Unable to create TermInSetQuery: %s, field type: %s is not supported for TermInSetQuery";
    throw new IllegalArgumentException(String.format(message, termInSetQuery, fieldDef.getType()));
  }

  private DisjunctionMaxQuery getDisjunctionMaxQuery(
      com.yelp.nrtsearch.server.grpc.DisjunctionMaxQuery disjunctionMaxQuery, DocLookup docLookup) {
    List<Query> disjuncts =
        disjunctionMaxQuery.getDisjunctsList().stream()
            .map(query -> getQuery(query, docLookup))
            .collect(Collectors.toList());
    return new DisjunctionMaxQuery(disjuncts, disjunctionMaxQuery.getTieBreakerMultiplier());
  }

  private Query getMatchQuery(MatchQuery matchQuery, DocLookup docLookup) {
    Analyzer analyzer =
        isAnalyzerDefined(matchQuery.getAnalyzer())
            ? AnalyzerCreator.getInstance().getAnalyzer(matchQuery.getAnalyzer())
            : IndexState.getFieldSearchAnalyzer(matchQuery.getField(), docLookup);

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

  private Query getMatchPhraseQuery(MatchPhraseQuery matchPhraseQuery, DocLookup docLookup) {
    Analyzer analyzer =
        isAnalyzerDefined(matchPhraseQuery.getAnalyzer())
            ? AnalyzerCreator.getInstance().getAnalyzer(matchPhraseQuery.getAnalyzer())
            : IndexState.getFieldSearchAnalyzer(matchPhraseQuery.getField(), docLookup);

    QueryBuilder queryBuilder = new QueryBuilder(analyzer);
    // This created query will be TermQuery if only one token is found after analysis, otherwise
    // PhraseQuery
    Query phraseQuery =
        queryBuilder.createPhraseQuery(
            matchPhraseQuery.getField(), matchPhraseQuery.getQuery(), matchPhraseQuery.getSlop());

    // This can happen if there are no tokens found after analyzing the query text
    if (phraseQuery == null) {
      MatchPhraseQuery.ZeroTerms zeroTermsQuery = matchPhraseQuery.getZeroTermsQuery();
      switch (zeroTermsQuery) {
        case NONE_ZERO_TERMS -> {
          return new MatchNoDocsQuery();
        }
        case ALL_ZERO_TERMS -> {
          return new MatchAllDocsQuery();
        }
        default ->
            throw new IllegalArgumentException(
                zeroTermsQuery
                    + " not valid. ZeroTermsQuery should be NONE_ZERO_TERMS or ALL_ZERO_TERMS");
      }
    }
    return phraseQuery;
  }

  private Query getMultiMatchQuery(MultiMatchQuery multiMatchQuery, DocLookup docLookup) {
    Map<String, Float> fieldBoosts = multiMatchQuery.getFieldBoostsMap();
    Collection<String> fields;

    // Take all fields if none are provided
    if (multiMatchQuery.getFieldsList().isEmpty()) {
      fields = docLookup.getAllFieldNames();
    } else {
      fields = multiMatchQuery.getFieldsList();
    }

    if (multiMatchQuery.getType() == MatchType.CROSS_FIELDS) {
      return getMultiMatchCrossFieldsQuery(fields, multiMatchQuery, docLookup);
    }

    List<Query> matchQueries =
        fields.stream()
            .map(
                field -> {
                  Query query =
                      switch (multiMatchQuery.getType()) {
                        case BEST_FIELDS -> {
                          MatchQuery matchQuery =
                              MatchQuery.newBuilder()
                                  .setField(field)
                                  .setQuery(multiMatchQuery.getQuery())
                                  .setOperator(multiMatchQuery.getOperator())
                                  .setMinimumNumberShouldMatch(
                                      multiMatchQuery.getMinimumNumberShouldMatch())
                                  .setAnalyzer(
                                      multiMatchQuery
                                          .getAnalyzer()) // TODO: making the analyzer once and
                                  // using it
                                  // for
                                  // all match queries would be more efficient
                                  .setFuzzyParams(multiMatchQuery.getFuzzyParams())
                                  .build();
                          yield getMatchQuery(matchQuery, docLookup);
                        }
                        case PHRASE_PREFIX ->
                            MatchPhrasePrefixQuery.build(
                                com.yelp.nrtsearch.server.grpc.MatchPhrasePrefixQuery.newBuilder()
                                    .setField(field)
                                    .setQuery(multiMatchQuery.getQuery())
                                    .setAnalyzer(multiMatchQuery.getAnalyzer())
                                    .setSlop(multiMatchQuery.getSlop())
                                    .setMaxExpansions(multiMatchQuery.getMaxExpansions())
                                    .build(),
                                docLookup);
                        default ->
                            throw new IllegalArgumentException(
                                "Unknown multi match type: " + multiMatchQuery.getType());
                      };
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

  private Query getMultiMatchCrossFieldsQuery(
      Collection<String> fields, MultiMatchQuery multiMatchQuery, DocLookup docLookup) {
    Analyzer analyzer = null;
    for (String field : fields) {
      FieldDef fieldDef = docLookup.getFieldDefOrThrow(field);
      if (!(fieldDef instanceof TextBaseFieldDef textBaseFieldDef)) {
        throw new IllegalArgumentException("Field must be analyzable: " + field);
      }
      if (!textBaseFieldDef.isSearchable()) {
        throw new IllegalArgumentException("Field must be searchable: " + field);
      }
      if (analyzer == null) {
        analyzer =
            multiMatchQuery.hasAnalyzer()
                ? AnalyzerCreator.getInstance().getAnalyzer(multiMatchQuery.getAnalyzer())
                : textBaseFieldDef.getSearchAnalyzer().orElse(null);
      }
    }
    if (analyzer == null) {
      throw new IllegalArgumentException("Could not determine analyzer for query");
    }
    return MatchCrossFieldsQuery.build(
        multiMatchQuery.getQuery(),
        new ArrayList<>(fields),
        multiMatchQuery.getFieldBoostsMap(),
        multiMatchQuery.getOperator(),
        multiMatchQuery.getMinimumNumberShouldMatch(),
        multiMatchQuery.getTieBreakerMultiplier(),
        analyzer);
  }

  private Query getRangeQuery(RangeQuery rangeQuery, DocLookup docLookup) {
    String fieldName = rangeQuery.getField();
    FieldDef field = docLookup.getFieldDefOrThrow(fieldName);

    if (!(field instanceof RangeQueryable)) {
      throw new IllegalArgumentException("Field: " + fieldName + " does not support RangeQuery");
    }

    return ((RangeQueryable) field).getRangeQuery(rangeQuery);
  }

  private Query getGeoBoundingBoxQuery(
      GeoBoundingBoxQuery geoBoundingBoxQuery, DocLookup docLookup) {
    String fieldName = geoBoundingBoxQuery.getField();
    FieldDef field = docLookup.getFieldDefOrThrow(fieldName);

    if (!(field instanceof GeoQueryable)) {
      throw new IllegalArgumentException(
          "Field: " + fieldName + " does not support GeoBoundingBoxQuery");
    }

    return ((GeoQueryable) field).getGeoBoundingBoxQuery(geoBoundingBoxQuery);
  }

  private Query getGeoRadiusQuery(GeoRadiusQuery geoRadiusQuery, DocLookup docLookup) {
    String fieldName = geoRadiusQuery.getField();
    FieldDef field = docLookup.getFieldDefOrThrow(fieldName);
    if (!(field instanceof GeoQueryable)) {
      throw new IllegalArgumentException(
          "Field: " + fieldName + " does not support GeoRadiusQuery");
    }
    return ((GeoQueryable) field).getGeoRadiusQuery(geoRadiusQuery);
  }

  private Query getGeoPointQuery(GeoPointQuery geoPolygonQuery, DocLookup docLookup) {
    String fieldName = geoPolygonQuery.getField();
    FieldDef field = docLookup.getFieldDefOrThrow(fieldName);

    if (!(field instanceof PolygonQueryable)) {
      throw new IllegalArgumentException("Field " + fieldName + "does not support GeoPolygonQuery");
    }
    return ((PolygonQueryable) field).getGeoPointQuery(geoPolygonQuery);
  }

  private Query getGeoPolygonQuery(GeoPolygonQuery geoPolygonQuery, DocLookup docLookup) {
    String fieldName = geoPolygonQuery.getField();
    FieldDef field = docLookup.getFieldDefOrThrow(fieldName);

    if (!(field instanceof GeoQueryable)) {
      throw new IllegalArgumentException(
          "Field " + fieldName + " does not support GeoPolygonQuery");
    }
    return ((GeoQueryable) field).getGeoPolygonQuery(geoPolygonQuery);
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

  private Query getExistsQuery(ExistsQuery existsQuery) {
    String fieldName = existsQuery.getField();
    return new ConstantScoreQuery(new TermQuery(new Term(IndexState.FIELD_NAMES, fieldName)));
  }

  private static Query getPrefixQuery(
      PrefixQuery prefixQuery, DocLookup docLookup, boolean spanQuery) {
    FieldDef fieldDef = docLookup.getFieldDefOrThrow(prefixQuery.getField());

    if (!(fieldDef instanceof PrefixQueryable)) {
      throw new IllegalArgumentException(
          "Field " + fieldDef.getName() + " does not support PrefixQuery");
    }

    MultiTermQuery.RewriteMethod rewriteMethod =
        getRewriteMethod(prefixQuery.getRewrite(), prefixQuery.getRewriteTopTermsSize());

    return ((PrefixQueryable) fieldDef).getPrefixQuery(prefixQuery, rewriteMethod, spanQuery);
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

  private Query getConstantScoreQuery(
      com.yelp.nrtsearch.server.grpc.ConstantScoreQuery constantScoreQueryGrpc,
      DocLookup docLookup) {
    Query filterQuery = getQuery(constantScoreQueryGrpc.getFilter(), docLookup);
    return new ConstantScoreQuery(filterQuery);
  }

  private SpanQuery getSpanQuery(
      com.yelp.nrtsearch.server.grpc.SpanQuery protoSpanQuery, DocLookup docLookup) {
    List<SpanQuery> clauses = new ArrayList<>();

    com.yelp.nrtsearch.server.grpc.SpanQuery.QueryCase queryCase = protoSpanQuery.getQueryCase();
    switch (queryCase) {
      case SPANNEARQUERY:
        com.yelp.nrtsearch.server.grpc.SpanNearQuery protoSpanNearQuery =
            protoSpanQuery.getSpanNearQuery();
        for (com.yelp.nrtsearch.server.grpc.SpanQuery protoClause :
            protoSpanNearQuery.getClausesList()) {
          SpanQuery luceneClause = getSpanQuery(protoClause, docLookup);
          clauses.add(luceneClause);
        }
        SpanQuery[] clausesArray = clauses.toArray(new SpanQuery[0]);
        int slop = protoSpanNearQuery.getSlop();
        boolean inOrder = protoSpanNearQuery.getInOrder();
        return new SpanNearQuery(clausesArray, slop, inOrder);
      case SPANTERMQUERY:
        com.yelp.nrtsearch.server.grpc.TermQuery protoSpanTermQuery =
            protoSpanQuery.getSpanTermQuery();
        return new SpanTermQuery(
            new Term(protoSpanTermQuery.getField(), protoSpanTermQuery.getTextValue()));
      case SPANMULTITERMQUERY:
        com.yelp.nrtsearch.server.grpc.SpanMultiTermQuery protoSpanMultiTermQuery =
            protoSpanQuery.getSpanMultiTermQuery();
        return getSpanMultiTermQueryWrapper(protoSpanMultiTermQuery, docLookup);
      default:
        throw new IllegalArgumentException("Unsupported Span Query: " + protoSpanQuery);
    }
  }

  private SpanMultiTermQueryWrapper<?> getSpanMultiTermQueryWrapper(
      com.yelp.nrtsearch.server.grpc.SpanMultiTermQuery protoSpanMultiTermQuery,
      DocLookup docLookup) {

    com.yelp.nrtsearch.server.grpc.SpanMultiTermQuery.WrappedQueryCase wrappedQueryCase =
        protoSpanMultiTermQuery.getWrappedQueryCase();

    switch (wrappedQueryCase) {
      case WILDCARDQUERY:
        MultiTermQuery.RewriteMethod rewriteMethod =
            getRewriteMethod(
                protoSpanMultiTermQuery.getWildcardQuery().getRewrite(),
                protoSpanMultiTermQuery.getWildcardQuery().getRewriteTopTermsSize());
        WildcardQuery wildcardQuery =
            new WildcardQuery(
                new Term(
                    protoSpanMultiTermQuery.getWildcardQuery().getField(),
                    protoSpanMultiTermQuery.getWildcardQuery().getText()),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                rewriteMethod);
        return new SpanMultiTermQueryWrapper<>(wildcardQuery);
      case FUZZYQUERY:
        FuzzyQuery fuzzyQuery = getFuzzyQuery(protoSpanMultiTermQuery);
        return new SpanMultiTermQueryWrapper<>(fuzzyQuery);
      case PREFIXQUERY:
        Query prefixQuery =
            getPrefixQuery(protoSpanMultiTermQuery.getPrefixQuery(), docLookup, true);
        return new SpanMultiTermQueryWrapper<>((MultiTermQuery) prefixQuery);
      case REGEXPQUERY:
        RegexpQuery regexpQuery = getRegexpQuery(protoSpanMultiTermQuery);
        return new SpanMultiTermQueryWrapper<>(regexpQuery);
      case TERMRANGEQUERY:
        TermRangeQuery termRangeQuery =
            getTermRangeQuery(protoSpanMultiTermQuery.getTermRangeQuery());
        return new SpanMultiTermQueryWrapper<>(termRangeQuery);
      default:
        throw new IllegalArgumentException(
            "Unsupported Span Multi Query Term Wrapper: " + protoSpanMultiTermQuery);
    }
  }

  private static FuzzyQuery getFuzzyQuery(
      com.yelp.nrtsearch.server.grpc.SpanMultiTermQuery protoSpanMultiTermQuery) {
    com.yelp.nrtsearch.server.grpc.FuzzyQuery protoFuzzyQuery =
        protoSpanMultiTermQuery.getFuzzyQuery();
    Term term = new Term(protoFuzzyQuery.getField(), protoFuzzyQuery.getText());

    int maxEdits = FuzzyQuery.defaultMaxEdits;
    if (protoFuzzyQuery.hasAuto()) {
      maxEdits = QueryUtils.computeMaxEditsFromTermLength(term, protoFuzzyQuery.getAuto());
    } else {
      if (protoFuzzyQuery.hasMaxEdits()) {
        maxEdits = protoFuzzyQuery.getMaxEdits();
      }
    }

    int prefixLength =
        protoFuzzyQuery.hasPrefixLength()
            ? protoFuzzyQuery.getPrefixLength()
            : FuzzyQuery.defaultPrefixLength;

    int maxExpansions =
        protoFuzzyQuery.hasMaxExpansions()
            ? protoFuzzyQuery.getMaxExpansions()
            : FuzzyQuery.defaultMaxExpansions;

    // Set the default transpositions to true, if it is not provided.
    boolean transpositions =
        protoFuzzyQuery.hasTranspositions()
            ? protoFuzzyQuery.getTranspositions()
            : FuzzyQuery.defaultTranspositions;

    MultiTermQuery.RewriteMethod rewriteMethod =
        getRewriteMethod(protoFuzzyQuery.getRewrite(), protoFuzzyQuery.getRewriteTopTermsSize());
    return new FuzzyQuery(
        term, maxEdits, prefixLength, maxExpansions, transpositions, rewriteMethod);
  }

  private static RegexpQuery getRegexpQuery(
      com.yelp.nrtsearch.server.grpc.SpanMultiTermQuery protoSpanMultiTermQuery) {

    com.yelp.nrtsearch.server.grpc.RegexpQuery protoRegexpQuery =
        protoSpanMultiTermQuery.getRegexpQuery();

    Term term = new Term(protoRegexpQuery.getField(), protoRegexpQuery.getText());

    int flags =
        switch (protoRegexpQuery.getFlag()) {
          case REGEXP_ALL -> RegExp.ALL;
          case REGEXP_ANYSTRING -> RegExp.ANYSTRING;
          case REGEXP_AUTOMATON -> RegExp.AUTOMATON;
          case REGEXP_COMPLEMENT -> RegExp.DEPRECATED_COMPLEMENT;
          case REGEXP_EMPTY -> RegExp.EMPTY;
          case REGEXP_INTERSECTION -> RegExp.INTERSECTION;
          case REGEXP_INTERVAL -> RegExp.INTERVAL;
          case REGEXP_NONE -> RegExp.NONE;
          default -> RegExp.ALL;
        };

    int maxDeterminizedStates =
        protoRegexpQuery.hasMaxDeterminizedStates()
            ? protoRegexpQuery.getMaxDeterminizedStates()
            : Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;

    MultiTermQuery.RewriteMethod rewriteMethod =
        getRewriteMethod(protoRegexpQuery.getRewrite(), protoRegexpQuery.getRewriteTopTermsSize());
    return new RegexpQuery(
        term, flags, 0, RegexpQuery.DEFAULT_PROVIDER, maxDeterminizedStates, rewriteMethod);
  }

  private static TermRangeQuery getTermRangeQuery(
      com.yelp.nrtsearch.server.grpc.TermRangeQuery protoTermRangeQuery) {

    MultiTermQuery.RewriteMethod rewriteMethod =
        getRewriteMethod(
            protoTermRangeQuery.getRewrite(), protoTermRangeQuery.getRewriteTopTermsSize());
    return new TermRangeQuery(
        protoTermRangeQuery.getField(),
        new BytesRef(protoTermRangeQuery.getLowerTerm()),
        new BytesRef(protoTermRangeQuery.getUpperTerm()),
        protoTermRangeQuery.getIncludeLower(),
        protoTermRangeQuery.getIncludeUpper(),
        rewriteMethod);
  }

  private static Query getExactVectorQuery(
      com.yelp.nrtsearch.server.grpc.ExactVectorQuery exactVectorQuery, DocLookup docLookup) {
    String fieldName = exactVectorQuery.getField();
    FieldDef field = docLookup.getFieldDefOrThrow(fieldName);

    if (field instanceof VectorQueryable vectorQueryable) {
      return vectorQueryable.getExactQuery(exactVectorQuery);
    }
    throw new IllegalArgumentException(
        "Field: " + fieldName + " does not support ExactVectorQuery");
  }
}
