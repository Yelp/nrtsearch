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
package com.yelp.nrtsearch.server.query.multifunction;

import com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery.BoostMode;
import com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery.FunctionScoreMode;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.query.QueryNodeMapper;
import com.yelp.nrtsearch.server.query.QueryUtils;
import com.yelp.nrtsearch.server.query.multifunction.FilterFunction.LeafFunction;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
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
  private final float minScore;
  private final boolean minExcluded;

  private static boolean hasPassedMinScore(
      float currentScore, float minimalScore, boolean minimalExcluded) {
    if (currentScore > minimalScore) {
      return true;
    } else return !minimalExcluded && currentScore == minimalScore;
  }

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
        multiFunctionScoreQueryGrpc.getBoostMode(),
        multiFunctionScoreQueryGrpc.getMinScore(),
        multiFunctionScoreQueryGrpc.getMinExcluded());
  }

  /**
   * Constructor.
   *
   * @param innerQuery main query used for initial doc recall and scoring.
   * @param functions functions used to produce function score for documents
   * @param scoreMode mode to combine function scores
   * @param boostMode mode to combine function and document scores
   * @param minScore min score to match
   * @param minExcluded is min score excluded
   */
  public MultiFunctionScoreQuery(
      Query innerQuery,
      FilterFunction[] functions,
      FunctionScoreMode scoreMode,
      BoostMode boostMode,
      float minScore,
      boolean minExcluded) {
    this.innerQuery = innerQuery;
    this.functions = functions;
    this.scoreMode = scoreMode;
    this.boostMode = boostMode;
    this.minScore = minScore;
    this.minExcluded = minExcluded;

    if (minScore < 0) {
      throw new IllegalArgumentException(
          "minScore must be a non-negative number, but got " + minScore);
    }
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    Query rewritten = super.rewrite(indexSearcher);
    if (rewritten != this) {
      return rewritten;
    }
    Query rewrittenInner = innerQuery.rewrite(indexSearcher);
    boolean needsRewrite = rewrittenInner != innerQuery;
    FilterFunction[] rewrittenFunctions = new FilterFunction[functions.length];
    for (int i = 0; i < functions.length; ++i) {
      rewrittenFunctions[i] = functions[i].rewrite(indexSearcher);
      needsRewrite |= (rewrittenFunctions[i] != functions[i]);
    }
    if (needsRewrite) {
      return new MultiFunctionScoreQuery(
          rewrittenInner, rewrittenFunctions, scoreMode, boostMode, minScore, minExcluded);
    } else {
      return this;
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    innerQuery.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
  }

  @Override
  public Weight createWeight(
      IndexSearcher searcher, org.apache.lucene.search.ScoreMode scoreMode, float boost)
      throws IOException {
    if (scoreMode == ScoreMode.COMPLETE_NO_SCORES && !isMinScoreWrapperUsed()) {
      // Even if the outer query doesn't require score, inner score is needed if the MinScoreWrapper
      // is used for filtering
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
    Weight innerWeight =
        innerQuery.createWeight(
            searcher,
            boostMode == BoostMode.BOOST_MODE_REPLACE
                ? ScoreMode.COMPLETE_NO_SCORES
                : ScoreMode.COMPLETE,
            boost);
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
        if (functionsExplanations.isEmpty()) {
          // it is a little weird to add a match although no function matches but that is the way
          // function_score behaves right now
          factorExplanation =
              Explanation.match(1.0f, "No function matched", Collections.emptyList());
        } else if (singleFunction && functionsExplanations.size() == 1) {
          factorExplanation = functionsExplanations.getFirst();
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
      float curScore = expl.getValue().floatValue();
      if (!hasPassedMinScore(curScore, minScore, minExcluded)) {
        expl =
            Explanation.noMatch(
                "Score value is too low, expected at least "
                    + minScore
                    + (minExcluded ? " (excluded)" : " (included)")
                    + " but got "
                    + curScore,
                expl);
      }
      return expl;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      ScorerSupplier innerScorerSupplier = innerWeight.scorerSupplier(context);
      if (innerScorerSupplier == null) {
        return null;
      }
      return new ScorerSupplier() {
        @Override
        public Scorer get(long leadCost) throws IOException {
          Scorer innerScorer = innerWeight.scorer(context);
          LeafFunction[] leafFunctions = new LeafFunction[functions.length];
          Bits[] docSets = new Bits[functions.length];
          for (int i = 0; i < filterWeights.length; ++i) {
            leafFunctions[i] = functions[i].getLeafFunction(context);
            if (filterWeights[i] != null) {
              ScorerSupplier filterScorerSupplier = filterWeights[i].scorerSupplier(context);
              docSets[i] =
                  QueryUtils.asSequentialAccessBits(
                      context.reader().maxDoc(), filterScorerSupplier);
            } else {
              docSets[i] = new Bits.MatchAllBits(context.reader().maxDoc());
            }
          }

          Scorer scorer =
              new MultiFunctionScorer(innerScorer, scoreMode, boostMode, leafFunctions, docSets);
          if (isMinScoreWrapperUsed()) {
            scorer = new MinScoreWrapper(scorer, minScore, minExcluded);
          }
          return scorer;
        }

        @Override
        public long cost() {
          return innerScorerSupplier.cost();
        }
      };
    }

    private Explanation explainBoost(Explanation queryExpl, Explanation funcExpl) {
      return switch (boostMode) {
        case BOOST_MODE_MULTIPLY ->
            Explanation.match(
                queryExpl.getValue().floatValue() * funcExpl.getValue().floatValue(),
                "function score, product of:",
                queryExpl,
                funcExpl);
        case BOOST_MODE_SUM ->
            Explanation.match(
                funcExpl.getValue().floatValue() + queryExpl.getValue().floatValue(),
                "sum of",
                queryExpl,
                funcExpl);
        case BOOST_MODE_REPLACE ->
            Explanation.match(
                funcExpl.getValue().floatValue(),
                "Ignoring query score, function score of",
                queryExpl,
                funcExpl);
        default -> throw new IllegalStateException("Unknown boost mode type: " + boostMode);
      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  /*
   *  minScoreWrapper is used under either condition:
   * 1. minScore is set to be a positive number (no matter min is excluded or not)
   * 2. minScore is zero, but zero is excluded
   * * */
  private boolean isMinScoreWrapperUsed() {
    return minScore > 0 || minExcluded;
  }

  /**
   * A port with minimal modification of Elasticsearch <a
   * href="https://github.com/elastic/elasticsearch/blob/v7.2.0/server/src/main/java/org/elasticsearch/common/lucene/search/function/MinScoreScorer.java">MinScoreScorer</a>.
   * We add minExcluded to make the boundary clear for inclusion/exclusion.
   */
  public static class MinScoreWrapper extends Scorer {
    private final Scorer in;
    private final float minScore;
    private float curScore;
    private final boolean minExcluded;

    public MinScoreWrapper(Scorer in, float minScore, boolean minExcluded) {
      this.in = in;
      this.minScore = minScore;
      this.minExcluded = minExcluded;
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
      TwoPhaseIterator inTwoPhase = in.twoPhaseIterator();
      DocIdSetIterator approximation;
      if (inTwoPhase == null) {
        approximation = in.iterator();
        if (TwoPhaseIterator.unwrap(approximation) != null) {
          inTwoPhase = TwoPhaseIterator.unwrap(approximation);
          approximation = inTwoPhase.approximation();
        }
      } else {
        approximation = inTwoPhase.approximation();
      }
      final TwoPhaseIterator finalTwoPhase = inTwoPhase;
      return new TwoPhaseIterator(approximation) {

        @Override
        public boolean matches() throws IOException {
          if (finalTwoPhase != null && !finalTwoPhase.matches()) {
            return false;
          }
          // we need to check the two-phase iterator first
          // otherwise calling score() is illegal
          curScore = in.score();
          return hasPassedMinScore(curScore, minScore, minExcluded);
        }

        @Override
        public float matchCost() {
          return 1000f // random constant for the score computation
              + (finalTwoPhase == null ? 0 : finalTwoPhase.matchCost());
        }
      };
    }

    @Override
    public DocIdSetIterator iterator() {
      return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return in.getMaxScore(upTo);
    }

    @Override
    public float score() throws IOException {
      return curScore;
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      return in.advanceShallow(target);
    }

    @Override
    public int docID() {
      return in.docID();
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
        FunctionScoreMode scoreMode,
        BoostMode boostMode,
        LeafFunction[] leafFunctions,
        Bits[] docSets) {
      super(innerScorer);
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
      return switch (boostMode) {
        case BOOST_MODE_MULTIPLY -> (float) (innerQueryScore * functionScore);
        case BOOST_MODE_SUM -> (float) (innerQueryScore + functionScore);
        case BOOST_MODE_REPLACE -> (float) functionScore;
        default -> throw new IllegalStateException("Unknown boost mode type: " + boostMode);
      };
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
      sb.append("{").append(function == null ? "" : function.toString()).append("}");
    }
    sb.append("])");
    sb.append(", minScore: ").append(minScore).append(minExcluded ? " (excluded)" : " (included)");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof MultiFunctionScoreQuery that)) return false;
    return Float.compare(that.minScore, minScore) == 0
        && minExcluded == that.minExcluded
        && Objects.equals(innerQuery, that.innerQuery)
        && Arrays.equals(functions, that.functions)
        && scoreMode == that.scoreMode
        && boostMode == that.boostMode;
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(innerQuery, scoreMode, boostMode, minScore, minExcluded);
    result = 31 * result + Arrays.hashCode(functions);
    return result;
  }
}
