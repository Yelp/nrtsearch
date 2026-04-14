/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.search.multiretriever.blender.score;

import org.apache.lucene.search.ScoreDoc;

/**
 * {@link BlendedScoreDoc} implementing Reciprocal Rank Fusion (RRF). {@link #score} accumulates the
 * sum of {@code weight / (k + rank)} contributions across all retrievers and is updated on every
 * {@link #add} call.
 *
 * <p>{@code k} is a smoothing constant (default {@value #DEFAULT_K}) that dampens the advantage of
 * top-ranked documents. Higher values give more weight to documents appearing consistently across
 * retrievers; lower values amplify first-place dominance.
 *
 * <p>Reference: Cormack, Clarke &amp; Buettcher (2009), "Reciprocal Rank Fusion outperforms
 * Condorcet and individual Rank Learning Methods."
 */
public class WeightedRRFScoreDoc extends BlendedScoreDoc {

  /** Default smoothing constant used by most RRF literature and practice. */
  public static final int DEFAULT_K = 60;

  private final int k;

  /**
   * Convenience constructor using {@link #DEFAULT_K} and weight 1.0.
   *
   * @param baseDoc the first retriever hit
   * @param firstRank 1-based rank in the first retriever's result list; must be &ge; 1
   */
  public WeightedRRFScoreDoc(ScoreDoc baseDoc, int firstRank) {
    this(baseDoc, firstRank, 1.0f, DEFAULT_K);
  }

  /**
   * Full constructor.
   *
   * @param baseDoc the first retriever hit
   * @param firstRank 1-based rank in the first retriever's result list; must be &ge; 1
   * @param firstWeight per-retriever weight for the first retriever
   * @param k RRF smoothing constant; must be &ge; 1
   * @throws IllegalArgumentException if {@code firstRank} &lt; 1 or {@code k} &lt; 1
   */
  public WeightedRRFScoreDoc(ScoreDoc baseDoc, int firstRank, float firstWeight, int k) {
    super(baseDoc, firstWeight / (k + firstRank));
    if (k < 1) throw new IllegalArgumentException("k must be >= 1, got: " + k);
    if (firstRank < 1)
      throw new IllegalArgumentException("firstRank must be >= 1, got: " + firstRank);
    this.k = k;
  }

  /**
   * Adds {@code weight / (k + rank)} to {@link #score} and appends the raw hit to {@link
   * #scoreDocs}.
   */
  @Override
  public void add(int rank, float weight, ScoreDoc scoreDoc) {
    score += weight / (k + rank);
    scoreDocs.add(scoreDoc);
  }

  /** Returns the smoothing constant {@code k} this instance was constructed with. */
  public int getK() {
    return k;
  }
}
