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

public class WeightedScoreDocTest {

  private static final float DELTA = 1e-6f;

  // --- constructor ---

  @Test
  public void testInitialScore() {
    WeightedScoreDoc doc =
        new WeightedScoreDoc(new ScoreDoc(5, 0.8f, 1), 2.0f, WeightedScoreDoc.ScoreMode.MAX);
    assertEquals(5, doc.doc);
    assertEquals(1, doc.shardIndex);
    assertEquals(2.0f * 0.8f, doc.score, DELTA);
  }

  @Test
  public void testInitialScoreDocsContainsBaseDoc() {
    ScoreDoc base = new ScoreDoc(3, 0.5f, 0);
    WeightedScoreDoc doc = new WeightedScoreDoc(base, 1.0f, WeightedScoreDoc.ScoreMode.SUM);
    assertEquals(1, doc.scoreDocs.size());
    assertEquals(base, doc.scoreDocs.get(0));
  }

  // --- MAX mode ---

  @Test
  public void testMaxKeepsHigher() {
    // initial score = 1.0 * 0.6 = 0.6
    WeightedScoreDoc doc =
        new WeightedScoreDoc(new ScoreDoc(1, 0.6f, 0), 1.0f, WeightedScoreDoc.ScoreMode.MAX);
    // add weighted = 1.0 * 0.9 = 0.9 → max(0.6, 0.9) = 0.9
    doc.add(1, 1.0f, new ScoreDoc(1, 0.9f, 0));
    assertEquals(0.9f, doc.score, DELTA);
  }

  @Test
  public void testMaxKeepsCurrentWhenHigher() {
    // initial score = 1.0 * 0.9 = 0.9
    WeightedScoreDoc doc =
        new WeightedScoreDoc(new ScoreDoc(1, 0.9f, 0), 1.0f, WeightedScoreDoc.ScoreMode.MAX);
    // add weighted = 1.0 * 0.3 = 0.3 → max(0.9, 0.3) = 0.9
    doc.add(1, 1.0f, new ScoreDoc(1, 0.3f, 0));
    assertEquals(0.9f, doc.score, DELTA);
  }

  @Test
  public void testMaxWithWeight() {
    // initial score = 1.0 * 0.5 = 0.5
    WeightedScoreDoc doc =
        new WeightedScoreDoc(new ScoreDoc(1, 0.5f, 0), 1.0f, WeightedScoreDoc.ScoreMode.MAX);
    // add: weight=3.0, rawScore=0.3 → weighted = 0.9 → max(0.5, 0.9) = 0.9
    doc.add(1, 3.0f, new ScoreDoc(1, 0.3f, 0));
    assertEquals(0.9f, doc.score, DELTA);
  }

  // --- SUM mode ---

  @Test
  public void testSumAccumulates() {
    // initial score = 1.0 * 0.4 = 0.4
    WeightedScoreDoc doc =
        new WeightedScoreDoc(new ScoreDoc(1, 0.4f, 0), 1.0f, WeightedScoreDoc.ScoreMode.SUM);
    doc.add(1, 1.0f, new ScoreDoc(1, 0.3f, 0)); // += 0.3 → 0.7
    doc.add(1, 2.0f, new ScoreDoc(1, 0.1f, 0)); // += 0.2 → 0.9
    assertEquals(0.9f, doc.score, DELTA);
  }

  @Test
  public void testSumWithWeight() {
    WeightedScoreDoc doc =
        new WeightedScoreDoc(new ScoreDoc(1, 0.5f, 0), 1.0f, WeightedScoreDoc.ScoreMode.SUM);
    doc.add(1, 2.0f, new ScoreDoc(1, 0.5f, 0)); // weighted = 1.0
    assertEquals(1.5f, doc.score, DELTA);
  }

  // --- AVG mode ---

  @Test
  public void testAvgTwoRetrievers() {
    // initial score = 1.0 * 0.6 = 0.6
    WeightedScoreDoc doc =
        new WeightedScoreDoc(new ScoreDoc(1, 0.6f, 0), 1.0f, WeightedScoreDoc.ScoreMode.AVG);
    // add weighted = 1.0 * 0.4 = 0.4 → avg(0.6, 0.4) = 0.5
    doc.add(1, 1.0f, new ScoreDoc(1, 0.4f, 0));
    assertEquals(0.5f, doc.score, DELTA);
  }

  @Test
  public void testAvgThreeRetrievers() {
    // initial = 0.9
    WeightedScoreDoc doc =
        new WeightedScoreDoc(new ScoreDoc(1, 0.9f, 0), 1.0f, WeightedScoreDoc.ScoreMode.AVG);
    doc.add(1, 1.0f, new ScoreDoc(1, 0.3f, 0)); // avg(0.9, 0.3) = 0.6
    doc.add(1, 1.0f, new ScoreDoc(1, 0.6f, 0)); // avg(0.6, 0.6, 0.6) = 0.6
    assertEquals(0.6f, doc.score, DELTA);
  }

  // --- scoreDocs tracking ---

  @Test
  public void testScoreDocsGrowsOnAdd() {
    WeightedScoreDoc doc =
        new WeightedScoreDoc(new ScoreDoc(1, 0.5f, 0), 1.0f, WeightedScoreDoc.ScoreMode.SUM);
    assertEquals(1, doc.scoreDocs.size());
    doc.add(1, 1.0f, new ScoreDoc(1, 0.3f, 0));
    doc.add(1, 1.0f, new ScoreDoc(1, 0.2f, 0));
    assertEquals(3, doc.scoreDocs.size());
  }
}
