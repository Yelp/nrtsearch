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

import com.google.common.collect.Maps;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.RangeQueryable;
import com.yelp.nrtsearch.server.luceneserver.field.properties.TermQueryable;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptParamsTransformer;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.QueryBuilder;

/** This class maps our GRPC Query object to a Lucene Query object. */
class QueryNodeMapper {

  private final Map<com.yelp.nrtsearch.server.grpc.BooleanClause.Occur, BooleanClause.Occur>
      occurMapping = initializeOccurMapping();
  private final Map<MatchOperator, BooleanClause.Occur> matchOperatorOccurMapping =
      new EnumMap<>(
          Map.of(
              MatchOperator.SHOULD, BooleanClause.Occur.SHOULD,
              MatchOperator.MUST, BooleanClause.Occur.MUST));

  Query getQuery(com.yelp.nrtsearch.server.grpc.Query query, IndexState state) {
    Query queryNode = getQueryNode(query, state);

    if (query.getBoost() < 0) {
      throw new IllegalArgumentException("Boost must be a positive number, query: " + query);
    }

    if (query.getBoost() > 0) {
      return new BoostQuery(queryNode, query.getBoost());
    }
    return queryNode;
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
      default:
        throw new UnsupportedOperationException(
            "Unsupported query type received: " + query.getQueryNodeCase());
    }
  }

  private BooleanQuery getBooleanQuery(
      com.yelp.nrtsearch.server.grpc.BooleanQuery booleanQuery, IndexState state) {
    BooleanQuery.Builder builder =
        new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(booleanQuery.getMinimumNumberShouldMatch());

    booleanQuery
        .getClausesList()
        .forEach(
            clause ->
                builder.add(
                    getQuery(clause.getQuery(), state), occurMapping.get(clause.getOccur())));

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
        Maps.transformValues(
            functionScoreQuery.getScript().getParamsMap(), ScriptParamsTransformer.INSTANCE);
    return new FunctionScoreQuery(
        getQuery(functionScoreQuery.getQuery(), state),
        scriptFactory.newFactory(params, state.docLookup));
  }

  private Query getTermQuery(com.yelp.nrtsearch.server.grpc.TermQuery termQuery, IndexState state) {
    String fieldName = termQuery.getField();
    FieldDef fieldDef = state.getField(fieldName);

    if (fieldDef instanceof TermQueryable) {
      return ((TermQueryable) fieldDef).getTermQuery(termQuery);
    }

    String message =
        "Unable to create TermQuery: %s, field type: %s is not supported for TermQuery";
    throw new IllegalArgumentException(String.format(message, termQuery, fieldDef.getType()));
  }

  private Query getTermInSetQuery(
      com.yelp.nrtsearch.server.grpc.TermInSetQuery termInSetQuery, IndexState state) {
    String fieldName = termInSetQuery.getField();
    FieldDef fieldDef = state.getField(fieldName);

    if (fieldDef instanceof TermQueryable) {
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
    // otherwise BooleanQuery. The
    // BooleanQuery may include clauses with TermQuery or FuzzyQuery.
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
    if (matchQuery.getMinimumNumberShouldMatch() == 0 || query instanceof TermQuery) {
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
                  MatchQuery matchQuery =
                      MatchQuery.newBuilder()
                          .setField(field)
                          .setQuery(multiMatchQuery.getQuery())
                          .setOperator(multiMatchQuery.getOperator())
                          .setMinimumNumberShouldMatch(
                              multiMatchQuery.getMinimumNumberShouldMatch())
                          .setAnalyzer(
                              multiMatchQuery
                                  .getAnalyzer()) // TODO: making the analyzer once and using it for
                          // all match queries would be more efficient
                          .setFuzzyParams(multiMatchQuery.getFuzzyParams())
                          .build();
                  Query query = getMatchQuery(matchQuery, state);
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

  private Map<com.yelp.nrtsearch.server.grpc.BooleanClause.Occur, BooleanClause.Occur>
      initializeOccurMapping() {
    return Arrays.stream(com.yelp.nrtsearch.server.grpc.BooleanClause.Occur.values())
        .filter(v -> v != com.yelp.nrtsearch.server.grpc.BooleanClause.Occur.UNRECOGNIZED)
        .collect(
            () -> new EnumMap<>(com.yelp.nrtsearch.server.grpc.BooleanClause.Occur.class),
            (map, v) -> map.put(v, BooleanClause.Occur.valueOf(v.name())),
            EnumMap::putAll);
  }
}
