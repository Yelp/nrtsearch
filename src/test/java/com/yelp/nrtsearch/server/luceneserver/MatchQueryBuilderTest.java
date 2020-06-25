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

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.FuzzyParams;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class MatchQueryBuilderTest {

  private static final String FIELD = "test_field";

  private static final String SINGLE_TERM_TEXT = "<br> TEST </br>";
  private static final Term SINGLE_TERM = new Term(FIELD, "test");

  private static final String MULTIPLE_TERMS_TEXT = "<br> multiple TERMS here </br>";
  private static final List<Term> MULTIPLE_TERMS =
      Stream.of("multiple", "terms", "here")
          .map(text -> new Term(FIELD, text))
          .collect(Collectors.toList());

  private static final FuzzyParams FUZZY_PARAMS =
      FuzzyParams.newBuilder()
          .setMaxEdits(2)
          .setPrefixLength(5)
          .setMaxExpansions(7)
          .setTranspositions(true)
          .build();
  private static final BooleanClause.Occur OCCUR = BooleanClause.Occur.MUST;

  @Test
  public void testSingleTerm() {
    MatchQueryBuilder matchQueryBuilder =
        new MatchQueryBuilder(getTestAnalyzer(), FuzzyParams.newBuilder().build());
    Query query = matchQueryBuilder.createBooleanQuery(FIELD, SINGLE_TERM_TEXT);

    assertEquals(new TermQuery(SINGLE_TERM), query);
  }

  @Test
  public void testMultipleTerms() {
    MatchQueryBuilder matchQueryBuilder =
        new MatchQueryBuilder(getTestAnalyzer(), FuzzyParams.newBuilder().build());
    Query query = matchQueryBuilder.createBooleanQuery(FIELD, MULTIPLE_TERMS_TEXT, OCCUR);

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    MULTIPLE_TERMS.forEach(term -> builder.add(new BooleanClause(new TermQuery(term), OCCUR)));
    BooleanQuery expected = builder.build();

    assertEquals(expected, query);
  }

  @Test
  public void testSingleTermWithFuzzyParams() {
    MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(getTestAnalyzer(), FUZZY_PARAMS);
    Query query = matchQueryBuilder.createBooleanQuery(FIELD, SINGLE_TERM_TEXT);

    FuzzyQuery expectedQuery =
        new FuzzyQuery(
            SINGLE_TERM,
            FUZZY_PARAMS.getMaxEdits(),
            FUZZY_PARAMS.getPrefixLength(),
            FUZZY_PARAMS.getMaxExpansions(),
            FUZZY_PARAMS.getTranspositions());
    assertEquals(expectedQuery, query);
  }

  @Test
  public void testMultipleTermsWithFuzzyParams() {
    MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(getTestAnalyzer(), FUZZY_PARAMS);
    Query query = matchQueryBuilder.createBooleanQuery(FIELD, MULTIPLE_TERMS_TEXT, OCCUR);

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    MULTIPLE_TERMS.forEach(
        term -> {
          FuzzyQuery fuzzyQuery =
              new FuzzyQuery(
                  term,
                  FUZZY_PARAMS.getMaxEdits(),
                  FUZZY_PARAMS.getPrefixLength(),
                  FUZZY_PARAMS.getMaxExpansions(),
                  FUZZY_PARAMS.getTranspositions());
          builder.add(new BooleanClause(fuzzyQuery, OCCUR));
        });
    BooleanQuery expected = builder.build();

    assertEquals(expected, query);
  }

  private Analyzer getTestAnalyzer() {
    try {
      return CustomAnalyzer.builder()
          .withDefaultMatchVersion(Version.LATEST)
          .addCharFilter("htmlstrip")
          .withTokenizer("standard")
          .addTokenFilter("lowercase")
          .build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
