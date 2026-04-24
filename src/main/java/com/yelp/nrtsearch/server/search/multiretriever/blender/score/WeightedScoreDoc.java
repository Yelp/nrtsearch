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
 * {@link BlendedScoreDoc} that blends per-retriever scores into {@link #score}. Each retriever's
 * raw score is multiplied by its weight before being combined according to the configured {@link
 * ScoreMode}:
 *
 * <ul>
 *   <li>{@link ScoreMode#MAX} — highest weighted score across retrievers (default).
 *   <li>{@link ScoreMode#SUM} — sum of all weighted retriever scores.
 *   <li>{@link ScoreMode#AVG} — running average of all weighted retriever scores.
 * </ul>
 */
public class WeightedScoreDoc extends BlendedScoreDoc {

  /**
   * Strategy for combining per-retriever scores when the same document appears in more than one
   * retriever's results.
   */
  public enum ScoreMode {
    /** Use the highest weighted score across all retrievers. */
    MAX,
    /** Sum all weighted retriever scores. */
    SUM,
    /** Average all weighted retriever scores. */
    AVG
  }

  private final ScoreMode scoreMode;

  /**
   * @param firstRetrieverName name of the first retriever contributing a hit
   * @param baseDoc the first retriever hit; its raw score is multiplied by {@code firstWeight} to
   *     produce the initial blended score
   * @param firstWeight per-retriever weight applied to the first score
   * @param scoreMode how per-retriever scores are combined
   */
  public WeightedScoreDoc(
      String firstRetrieverName, ScoreDoc baseDoc, float firstWeight, ScoreMode scoreMode) {
    super(firstRetrieverName, baseDoc, baseDoc.score * firstWeight);
    this.scoreMode = scoreMode;
  }

  /**
   * Updates {@link #score} by combining the weighted {@code scoreDoc.score} with the current value
   * according to {@link #scoreMode}, then stores the raw hit in {@link #scoreDocs} under {@code
   * retrieverName}. The {@code rank} parameter is unused — score blending is rank-independent.
   */
  @Override
  public void add(String retrieverName, int rank, float weight, ScoreDoc scoreDoc) {
    float weighted = scoreDoc.score * weight;
    score =
        switch (scoreMode) {
          case MAX -> Math.max(score, weighted);
          case SUM -> score + weighted;
          case AVG -> (score * scoreDocs.size() + weighted) / (scoreDocs.size() + 1);
        };
    scoreDocs.put(retrieverName, scoreDoc);
  }
}
