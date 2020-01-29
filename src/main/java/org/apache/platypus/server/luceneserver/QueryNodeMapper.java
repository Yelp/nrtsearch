/*
 * Copyright 2020 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.platypus.server.luceneserver;

import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.platypus.server.grpc.QueryType;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class maps our GRPC Query object to a Lucene Query object.
 */
class QueryNodeMapper {

    private final Map<org.apache.platypus.server.grpc.BooleanClause.Occur, BooleanClause.Occur> occurMapping
            = initializeOccurMapping();

    Query getQuery(org.apache.platypus.server.grpc.Query query, IndexState state) {
        QueryType queryType = query.getQueryType();
        switch (queryType) {
            case BOOLEAN_QUERY: return getBooleanQuery(query.getBooleanQuery(), state);
            case PHRASE_QUERY: return getPhraseQuery(query.getPhraseQuery());
            case FUNCTION_SCORE_QUERY: return getFunctionScoreQuery(query.getFunctionScoreQuery(), state);
            case TERM_QUERY: return getTermQuery(query.getTermQuery());
            case TERM_IN_SET_QUERY: return getTermInSetQuery(query.getTermInSetQuery());
            case DISJUNCTION_MAX: return getDisjunctionMaxQuery(query.getDisjunctionMaxQuery(), state);
            default: throw new UnsupportedOperationException("Unsupported query type received: " + queryType);
        }
    }

    private BooleanQuery getBooleanQuery(org.apache.platypus.server.grpc.BooleanQuery booleanQuery, IndexState state) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder()
                .setMinimumNumberShouldMatch(booleanQuery.getMinimumNumberShouldMatch());

        booleanQuery.getClausesList()
                .forEach(clause -> builder
                        .add(getQuery(clause.getQuery(), state), occurMapping.get(clause.getOccur()))
                );

        return builder.build();
    }

    private PhraseQuery getPhraseQuery(org.apache.platypus.server.grpc.PhraseQuery phraseQuery) {
        PhraseQuery.Builder builder = new PhraseQuery.Builder()
                .setSlop(phraseQuery.getSlop());

        phraseQuery.getTermsList()
                .forEach(term -> builder.add(new Term(phraseQuery.getField(), term)));

        return builder.build();
    }

    private FunctionScoreQuery getFunctionScoreQuery(org.apache.platypus.server.grpc.FunctionScoreQuery functionScoreQuery,
                                                     IndexState state) {
        Expression expr;
        String exprString = functionScoreQuery.getFunction();
        try {
            expr = JavascriptCompiler.compile(exprString);
        } catch (ParseException pe) {
            // Static error (e.g. bad JavaScript syntax):
            throw new IllegalArgumentException(String.format("could not parse expression: %s", exprString), pe);
        }
        return new FunctionScoreQuery(getQuery(functionScoreQuery.getQuery(), state), expr.getDoubleValuesSource(state.exprBindings));
    }

    private TermQuery getTermQuery(org.apache.platypus.server.grpc.TermQuery termQuery) {
        return new TermQuery(new Term(termQuery.getField(), termQuery.getTerm()));
    }

    private TermInSetQuery getTermInSetQuery(org.apache.platypus.server.grpc.TermInSetQuery termInSetQuery) {
        List<BytesRef> terms = termInSetQuery.getTermsList()
                .stream()
                .map(BytesRef::new)
                .collect(Collectors.toList());
        return new TermInSetQuery(termInSetQuery.getField(), terms);
    }

    private DisjunctionMaxQuery getDisjunctionMaxQuery(org.apache.platypus.server.grpc.DisjunctionMaxQuery disjunctionMaxQuery,
                                                       IndexState state) {
        List<Query> disjuncts = disjunctionMaxQuery.getDisjunctsList()
                .stream()
                .map(query -> getQuery(query, state))
                .collect(Collectors.toList());
        return new DisjunctionMaxQuery(disjuncts, disjunctionMaxQuery.getTieBreakerMultiplier());
    }

    private Map<org.apache.platypus.server.grpc.BooleanClause.Occur, BooleanClause.Occur> initializeOccurMapping() {
        return Arrays.stream(org.apache.platypus.server.grpc.BooleanClause.Occur.values())
                .filter(v -> v != org.apache.platypus.server.grpc.BooleanClause.Occur.UNRECOGNIZED)
                .collect(
                        () -> new EnumMap<>(org.apache.platypus.server.grpc.BooleanClause.Occur.class),
                        (map, v) -> map.put(v, BooleanClause.Occur.valueOf(v.name())),
                        EnumMap::putAll
                );
    }

}
