/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.search.query.multifunction;

import com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery.BoostMode;
import com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery.FunctionScoreMode;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.QueryNodeMapper;
import com.yelp.nrtsearch.server.luceneserver.search.query.QueryUtils;
import com.yelp.nrtsearch.server.luceneserver.search.query.multifunction.FilterFunction.LeafFunction;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

/**
 * Minimal port of Elasticsearch <a
 * href="https://github.com/elastic/elasticsearch/blob/v7.2.0/server/src/main/java/org/elasticsearch/common/lucene/search/function/FunctionScoreQuery.java">FunctionScoreQuery</a>.
 * As we already have a query type by that name, the name has been changed to reflect the use of
 * multiple functions. It may be worth adding full feature support at a future time.
 */
public class MultiFunctionScoreQuery extends Query {

  private final Query innerQuery;
  private final FilterFunction[] functions;
  private final FunctionScoreMode scoreMode;
  private final BoostMode boostMode;

  /**
   * Builder method that creates a {@link MultiFunctionScoreQuery} from its gRPC message definiton.
   *
   * @param multiFunctionScoreQueryGrpc grpc definition
   * @param indexState index state
   * @return multi function score query
   */
  public static MultiFunctionScoreQuery build(
      com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery multiFunctionScoreQueryGrpc,
      IndexState indexState) {
    Query innerQuery =
        QueryNodeMapper.getInstance().getQuery(multiFunctionScoreQueryGrpc.getQuery(), indexState);
    FilterFunction[] functions =
        new FilterFunction[multiFunctionScoreQueryGrpc.getFunctionsCount()];
    for (int i = 0; i < functions.length; ++i) {
      functions[i] = FilterFunction.build(multiFunctionScoreQueryGrpc.getFunctions(i), indexState);
    }
    return new MultiFunctionScoreQuery(
        innerQuery,
        functions,
        multiFunctionScoreQueryGrpc.getScoreMode(),
        multiFunctionScoreQueryGrpc.getBoostMode());
  }

  /**
   * Constructor.
   *
   * @param innerQuery main query used for initial doc recall and scoring.
   * @param functions functions used to produce function score for documents
   * @param scoreMode mode to combine function scores
   * @param boostMode mode to combine function and document scores
   */
  public MultiFunctionScoreQuery(
      Query innerQuery,
      FilterFunction[] functions,
      FunctionScoreMode scoreMode,
      BoostMode boostMode) {
    this.innerQuery = innerQuery;
    this.functions = functions;
    this.scoreMode = scoreMode;
    this.boostMode = boostMode;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewritten = super.rewrite(reader);
    if (rewritten != this) {
      return rewritten;
    }
    Query rewrittenInner = innerQuery.rewrite(reader);
    boolean needsRewrite = rewrittenInner != innerQuery;
    FilterFunction[] rewrittenFunctions = new FilterFunction[functions.length];
    for (int i = 0; i < functions.length; ++i) {
      rewrittenFunctions[i] = functions[i].rewrite(reader);
      needsRewrite |= (rewrittenFunctions[i] != functions[i]);
    }
    if (needsRewrite) {
      return new MultiFunctionScoreQuery(rewrittenInner, rewrittenFunctions, scoreMode, boostMode);
    } else {
      return this;
    }
  }

  @Override
  public Weight createWeight(
      IndexSearcher searcher, org.apache.lucene.search.ScoreMode scoreMode, float boost)
      throws IOException {
    if (scoreMode == ScoreMode.COMPLETE_NO_SCORES) {
      return innerQuery.createWeight(searcher, scoreMode, boost);
    }
    Weight[] filterWeights = new Weight[functions.length];
    for (int i = 0; i < filterWeights.length; ++i) {
      if (functions[i].hasFilterQuery()) {
        filterWeights[i] =
            searcher.createWeight(
                searcher.rewrite(functions[i].getFilterQuery()),
                ScoreMode.COMPLETE_NO_SCORES,
                1.0f);
      }
    }
    Weight innerWeight = innerQuery.createWeight(searcher, ScoreMode.COMPLETE, boost);
    return new MultiFunctionWeight(this, innerWeight, filterWeights);
  }

  /** Weight that produces scorers that modify document scores with provided functions. */
  class MultiFunctionWeight extends Weight {
    private final Weight innerWeight;
    private final Weight[] filterWeights;

    protected MultiFunctionWeight(Query query, Weight innerWeight, Weight[] filterWeights) {
      super(query);
      this.innerWeight = innerWeight;
      this.filterWeights = filterWeights;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      innerWeight.extractTerms(terms);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Explanation expl = innerWeight.explain(context, doc);
      if (!expl.isMatch()) {
        return expl;
      }
      boolean singleFunction = functions.length == 1 && !functions[0].hasFilterQuery();
      if (functions.length > 0) {
        // First: Gather explanations for all functions/filters
        List<Explanation> functionsExplanations = new ArrayList<>();
        for (int i = 0; i < functions.length; ++i) {
          if (filterWeights[i] != null) {
            final Bits docSet =
                QueryUtils.asSequentialAccessBits(
                    context.reader().maxDoc(), filterWeights[i].scorerSupplier(context));
            if (!docSet.get(doc)) {
              continue;
            }
          }
          FilterFunction function = functions[i];
          Explanation functionExplanation =
              function.getLeafFunction(context).explainScore(doc, expl);
          if (function.hasFilterQuery()) {
            float factor = functionExplanation.getValue().floatValue();
            Query filterQuery = function.getFilterQuery();
            Explanation filterExplanation =
                Explanation.match(
                    factor,
                    "function score, product of:",
                    Explanation.match(1.0f, "match filter: " + filterQuery.toString()),
                    functionExplanation);
            functionsExplanations.add(filterExplanation);
          } else {
            functionsExplanations.add(functionExplanation);
          }
        }
        final Explanation factorExplanation;
        if (functionsExplanations.size() == 0) {
          // it is a little weird to add a match although no function matches but that is the way
          // function_score behaves right now
          factorExplanation =
              Explanation.match(1.0f, "No function matched", Collections.emptyList());
        } else if (singleFunction && functionsExplanations.size() == 1) {
          factorExplanation = functionsExplanations.get(0);
        } else {
          MultiFunctionScorer scorer = (MultiFunctionScorer) scorer(context);
          int actualDoc = scorer.iterator().advance(doc);
          assert (actualDoc == doc);
          double score = scorer.computeFunctionScore(doc, expl.getValue().floatValue());
          factorExplanation =
              Explanation.match(
                  (float) score,
                  "function score, score mode [" + scoreMode.toString().toLowerCase() + "]",
                  functionsExplanations);
        }
        expl = explainBoost(expl, factorExplanation);
      }
      return expl;
    }

    private Explanation explainBoost(Explanation queryExpl, Explanation funcExpl) {
      switch (boostMode) {
        case BOOST_MODE_MULTIPLY:
          return Explanation.match(
              queryExpl.getValue().floatValue() * funcExpl.getValue().floatValue(),
              "function score, product of:",
              queryExpl,
              funcExpl);
        case BOOST_MODE_SUM:
          return Explanation.match(
              funcExpl.getValue().floatValue() + queryExpl.getValue().floatValue(),
              "sum of",
              queryExpl,
              funcExpl);
        default:
          throw new IllegalStateException("Unknown boost mode type: " + boostMode);
      }
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      Scorer innerScorer = innerWeight.scorer(context);
      if (innerScorer == null) {
        return null;
      }

      LeafFunction[] leafFunctions = new LeafFunction[functions.length];
      Bits[] docSets = new Bits[functions.length];
      for (int i = 0; i < filterWeights.length; ++i) {
        leafFunctions[i] = functions[i].getLeafFunction(context);
        if (filterWeights[i] != null) {
          ScorerSupplier filterScorerSupplier = filterWeights[i].scorerSupplier(context);
          docSets[i] =
              QueryUtils.asSequentialAccessBits(context.reader().maxDoc(), filterScorerSupplier);
        } else {
          docSets[i] = new Bits.MatchAllBits(context.reader().maxDoc());
        }
      }
      return new MultiFunctionScorer(
          innerScorer, this, scoreMode, boostMode, leafFunctions, docSets);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }
  }

  /**
   * Scorer that computes the function score value for segment document, and uses it to modify the
   * query document score.
   */
  public static class MultiFunctionScorer extends FilterScorer {
    private final FunctionScoreMode scoreMode;
    private final BoostMode boostMode;
    private final LeafFunction[] leafFunctions;
    private final Bits[] docSets;

    public MultiFunctionScorer(
        Scorer innerScorer,
        MultiFunctionWeight weight,
        FunctionScoreMode scoreMode,
        BoostMode boostMode,
        LeafFunction[] leafFunctions,
        Bits[] docSets) {
      super(innerScorer, weight);
      this.scoreMode = scoreMode;
      this.boostMode = boostMode;
      this.leafFunctions = leafFunctions;
      this.docSets = docSets;
    }

    @Override
    public float score() throws IOException {
      int docId = docID();
      float innerQueryScore = super.score();
      if (leafFunctions.length == 0) {
        return innerQueryScore;
      }
      double functionScore = computeFunctionScore(docId, innerQueryScore);
      float finalScore = computeFinalScore(innerQueryScore, functionScore);
      if (finalScore < 0f || Float.isNaN(finalScore)) {
        throw new RuntimeException(
            "multi function score query returned an invalid score: "
                + finalScore
                + " for doc: "
                + docId);
      }
      return finalScore;
    }

    private double computeFunctionScore(int docId, float innerQueryScore) throws IOException {
      switch (scoreMode) {
        case SCORE_MODE_MULTIPLY:
          double multiplyValue = 1.0;
          for (int i = 0; i < leafFunctions.length; ++i) {
            if (docSets[i].get(docId)) {
              multiplyValue *= leafFunctions[i].score(docId, innerQueryScore);
            }
          }
          return multiplyValue;
        case SCORE_MODE_SUM:
          double sumValue = 0.0;
          boolean filterMatched = false;
          for (int i = 0; i < leafFunctions.length; ++i) {
            if (docSets[i].get(docId)) {
              sumValue += leafFunctions[i].score(docId, innerQueryScore);
              filterMatched = true;
            }
          }
          if (filterMatched) {
            return sumValue;
          } else {
            return 1.0;
          }
        default:
          throw new IllegalStateException("Unknown score mode type: " + scoreMode);
      }
    }

    private float computeFinalScore(float innerQueryScore, double functionScore) {
      switch (boostMode) {
        case BOOST_MODE_MULTIPLY:
          return (float) (innerQueryScore * functionScore);
        case BOOST_MODE_SUM:
          return (float) (innerQueryScore + functionScore);
        default:
          throw new IllegalStateException("Unknown boost mode type: " + boostMode);
      }
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return Float.MAX_VALUE;
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    sb.append("multi function score (").append(innerQuery.toString(field)).append(", functions: [");
    for (FilterFunction function : functions) {
      sb.append("{" + (function == null ? "" : function.toString()) + "}");
    }
    sb.append("])");
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!sameClassAs(obj)) {
      return false;
    }
    MultiFunctionScoreQuery other = (MultiFunctionScoreQuery) obj;
    return Objects.equals(this.innerQuery, other.innerQuery)
        && Objects.equals(this.scoreMode, other.scoreMode)
        && Objects.equals(this.boostMode, other.boostMode)
        && Arrays.equals(this.functions, other.functions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), innerQuery, scoreMode, boostMode, Arrays.hashCode(functions));
  }
}
