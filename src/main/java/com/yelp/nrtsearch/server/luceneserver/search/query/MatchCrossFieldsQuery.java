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
package com.yelp.nrtsearch.server.luceneserver.search.query;

import com.yelp.nrtsearch.server.grpc.MatchOperator;
import com.yelp.nrtsearch.server.luceneserver.search.query.QueryUtils.TermsAndPositions;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;

/**
 * Class for building a match query of the CROSS_FIELDS type. This is a terms centric match, where a
 * number of different fields are treated as if they were a single field for matching. A single
 * Analyzer is used to analyze the query text. Therefore, it is recommended that all fields use the
 * same analyzer.
 */
public class MatchCrossFieldsQuery {

  /**
   * Create a cross fields match query.
   *
   * @param queryText match query text
   * @param fields fields to match over
   * @param fieldBoostMap any per field boost values
   * @param matchOperator boolean clause type for term matches
   * @param minimumShouldMatch when using SHOULD operator, minimum terms that must match, or 0 for
   *     default
   * @param tieBreakerMultiplier tiebreaker to use for BlendedTermQuery
   * @param analyzer analyzer to extract terms from query text
   * @return built query
   */
  public static Query build(
      String queryText,
      List<String> fields,
      Map<String, Float> fieldBoostMap,
      MatchOperator matchOperator,
      int minimumShouldMatch,
      float tieBreakerMultiplier,
      Analyzer analyzer) {
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("No fields specified");
    }
    BooleanClause.Occur occur = matchOperator == MatchOperator.MUST ? Occur.MUST : Occur.SHOULD;
    try {
      // use first field for name in extracted terms
      TermsAndPositions termsAndPositions =
          QueryUtils.getTermsAndPositions(fields.get(0), queryText, analyzer);
      Query query;
      if (fields.size() == 1) {
        query = buildSingleFieldQuery(termsAndPositions, fields.get(0), occur, minimumShouldMatch);
        // Add field boost if specified
        Float boost = fieldBoostMap.get(fields.get(0));
        if (query != null && boost != null) {
          query = new BoostQuery(query, boost);
        }
      } else {
        query =
            buildMultiFieldQuery(
                termsAndPositions,
                fields,
                fieldBoostMap,
                occur,
                minimumShouldMatch,
                tieBreakerMultiplier);
      }

      return query == null ? new MatchNoDocsQuery() : query;
    } catch (IOException e) {
      throw new RuntimeException("Error analyzing query", e);
    }
  }

  /**
   * Build match query when only searching over a single field.
   *
   * @param termsAndPositions analyzed terms with positions
   * @param field field name
   * @param occur occur type for boolean query
   * @param minimumShouldMatch when using SHOULD occur, minimum terms that must match, or 0 for
   *     default
   * @return match query, or null
   * @throws IOException
   */
  private static Query buildSingleFieldQuery(
      TermsAndPositions termsAndPositions,
      String field,
      BooleanClause.Occur occur,
      int minimumShouldMatch)
      throws IOException {
    if (termsAndPositions.getTermArrays().isEmpty()) {
      return null;
    }

    if (termsAndPositions.getTermArrays().size() == 1
        && termsAndPositions.getTermArrays().get(0).length == 1) {
      // single token
      return new TermQuery(termsAndPositions.getTermArrays().get(0)[0]);
    } else if (termsAndPositions.getPositions().size() == 1) {
      // synonyms for single position
      SynonymQuery.Builder builder = new SynonymQuery.Builder(field);
      for (Term term : termsAndPositions.getTermArrays().get(0)) {
        builder.addTerm(term);
      }
      return builder.build();
    } else {
      // multiple positions
      BooleanQuery.Builder builder = new Builder();
      for (Term[] terms : termsAndPositions.getTermArrays()) {
        if (terms.length == 1) {
          builder.add(new TermQuery(terms[0]), occur);
        } else {
          SynonymQuery.Builder synBuilder = new SynonymQuery.Builder(field);
          for (Term term : terms) {
            synBuilder.addTerm(term);
          }
          builder.add(synBuilder.build(), occur);
        }
      }
      maybeSetMinimumShouldMatch(
          builder, occur, termsAndPositions.getTermArrays().size(), minimumShouldMatch);
      return builder.build();
    }
  }

  /**
   * Build match query when searching over multiple fields.
   *
   * @param termsAndPositions analyzed terms with positions
   * @param fields field names
   * @param fieldBoostMap per field boost values
   * @param occur occur type for boolean query
   * @param minimumShouldMatch when using SHOULD occur, minimum terms that must match, or 0 for
   *     default
   * @param tieBreakerMultiplier tiebreaker to use for BlendedTermQuery
   * @return match query, or null
   * @throws IOException
   */
  private static Query buildMultiFieldQuery(
      TermsAndPositions termsAndPositions,
      List<String> fields,
      Map<String, Float> fieldBoostMap,
      BooleanClause.Occur occur,
      int minimumShouldMatch,
      float tieBreakerMultiplier)
      throws IOException {
    if (termsAndPositions.getTermArrays().isEmpty()) {
      return null;
    }

    if (termsAndPositions.getTermArrays().size() == 1) {
      // single position
      return blendTerms(
          termsAndPositions.getTermArrays().get(0), fields, fieldBoostMap, tieBreakerMultiplier);
    } else {
      // multiple positions
      BooleanQuery.Builder builder = new Builder();
      for (Term[] terms : termsAndPositions.getTermArrays()) {
        builder.add(blendTerms(terms, fields, fieldBoostMap, tieBreakerMultiplier), occur);
      }
      maybeSetMinimumShouldMatch(
          builder, occur, termsAndPositions.getTermArrays().size(), minimumShouldMatch);
      return builder.build();
    }
  }

  /**
   * Set the minimum should match value on the boolean query builder to the min of the clause count
   * and specified minimum should match value, only if using the SHOULD occur type.
   *
   * @param builder boolean query builder
   * @param occur match query occur type
   * @param clauseCount number of clauses in the builder
   * @param minimumShouldMatch specified minimum should match
   */
  private static void maybeSetMinimumShouldMatch(
      BooleanQuery.Builder builder, Occur occur, int clauseCount, int minimumShouldMatch) {
    if (occur == Occur.SHOULD) {
      int resolvedMinShouldMatch = Math.min(clauseCount, minimumShouldMatch);
      if (resolvedMinShouldMatch > 0) {
        builder.setMinimumNumberShouldMatch(resolvedMinShouldMatch);
      }
    }
  }

  /**
   * Create a {@link BlendedTermQuery} for the given set of terms over the given set of fields.
   *
   * @param values term values
   * @param fields field names
   * @param fieldBoostMap per field boost values
   * @param tieBreakerMultiplier tiebreaker to use for BlendedTermQuery
   * @return blended term query
   */
  private static Query blendTerms(
      Term[] values,
      List<String> fields,
      Map<String, Float> fieldBoostMap,
      float tieBreakerMultiplier) {
    Term[] terms = new Term[fields.size() * values.length];
    float[] blendedBoost = new float[fields.size() * values.length];
    int i = 0;
    for (String field : fields) {
      for (Term term : values) {
        terms[i] = new Term(field, term.bytes());
        blendedBoost[i] = fieldBoostMap.getOrDefault(field, 1.0f);
        i++;
      }
    }
    if (i > 0) {
      terms = Arrays.copyOf(terms, i);
      blendedBoost = Arrays.copyOf(blendedBoost, i);
      return BlendedTermQuery.dismaxBlendedQuery(terms, blendedBoost, tieBreakerMultiplier);
    } else {
      throw new IllegalArgumentException("No terms to blend");
    }
  }
}
