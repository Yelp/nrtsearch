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

import static org.junit.Assert.assertEquals;

import org.apache.lucene.search.ScoreDoc;
import org.junit.Test;

public class WeightedRRFScoreDocTest {

  private static final float DELTA = 1e-6f;

  @Test
  public void testDefaultConstructorScore() {
    // weight=1.0, k=DEFAULT_K=60, rank=1 → score = 1.0 / (60 + 1)
    WeightedRRFScoreDoc doc = new WeightedRRFScoreDoc("r1", new ScoreDoc(10, 0f, 0), 1);
    assertEquals(1.0f / (WeightedRRFScoreDoc.DEFAULT_K + 1), doc.score, DELTA);
    assertEquals(10, doc.doc);
    assertEquals(0, doc.shardIndex);
    assertEquals(WeightedRRFScoreDoc.DEFAULT_K, doc.getK());
    assertEquals(1, doc.getScoreDocs().size());
  }

  @Test
  public void testFullConstructorScore() {
    // weight=2.0, k=10, rank=3 → score = 2.0 / (10 + 3)
    WeightedRRFScoreDoc doc = new WeightedRRFScoreDoc("r1", new ScoreDoc(5, 0f, 1), 3, 2.0f, 10);
    assertEquals(2.0f / 13, doc.score, DELTA);
    assertEquals(5, doc.doc);
    assertEquals(1, doc.shardIndex);
    assertEquals(10, doc.getK());
  }

  @Test
  public void testAddAccumulatesScore() {
    WeightedRRFScoreDoc doc = new WeightedRRFScoreDoc("r1", new ScoreDoc(1, 0f, 0), 1, 1.0f, 10);
    float initialScore = doc.score; // 1.0 / 11

    ScoreDoc hit = new ScoreDoc(1, 0.5f, 0);
    doc.add("r2", 2, 1.0f, hit); // += 1.0 / (10 + 2) = 1/12

    assertEquals(initialScore + 1.0f / 12, doc.score, DELTA);
  }

  @Test
  public void testAddAppendsScoredDocs() {
    WeightedRRFScoreDoc doc = new WeightedRRFScoreDoc("r1", new ScoreDoc(1, 0f, 0), 1, 1.0f, 10);
    assertEquals(1, doc.getScoreDocs().size()); // baseDoc pre-populated

    ScoreDoc hit1 = new ScoreDoc(1, 0.9f, 0);
    ScoreDoc hit2 = new ScoreDoc(1, 0.8f, 0);
    doc.add("r2", 2, 1.0f, hit1);
    doc.add("r3", 3, 1.0f, hit2);

    assertEquals(3, doc.getScoreDocs().size());
    assertEquals(hit1, doc.getScoreDocs().get("r2"));
    assertEquals(hit2, doc.getScoreDocs().get("r3"));
  }

  @Test
  public void testBoostScalesScore() {
    WeightedRRFScoreDoc unboosted =
        new WeightedRRFScoreDoc("r1", new ScoreDoc(1, 0f, 0), 1, 1.0f, 60);
    WeightedRRFScoreDoc boosted =
        new WeightedRRFScoreDoc("r1", new ScoreDoc(1, 0f, 0), 1, 3.0f, 60);
    assertEquals(3.0f * unboosted.score, boosted.score, DELTA);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidK() {
    new WeightedRRFScoreDoc("r1", new ScoreDoc(1, 0f, 0), 1, 1.0f, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFirstRank() {
    new WeightedRRFScoreDoc("r1", new ScoreDoc(1, 0f, 0), 0, 1.0f, 60);
  }
}
