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
package com.yelp.nrtsearch.server.search.multiretriever.blender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.search.multiretriever.blender.score.BlendedScoreDoc;
import com.yelp.nrtsearch.server.search.multiretriever.blender.score.WeightedScoreDoc;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

public class BlenderOperationSortAndPaginateTest {

  private static BlendedScoreDoc doc(int docId, float score) {
    return new WeightedScoreDoc(
        "r1", new ScoreDoc(docId, score, 0), 1.0f, WeightedScoreDoc.ScoreMode.MAX);
  }

  @Test
  public void testEmpty() {
    TopDocs result = BlenderOperation.sortAndPaginate(List.of(), 0, 10, 0f, false);
    assertEquals(0, result.scoreDocs.length);
    assertEquals(0, result.totalHits.value());
  }

  @Test
  public void testSingleDoc() {
    TopDocs result = BlenderOperation.sortAndPaginate(List.of(doc(1, 1.0f)), 0, 10, 0f, false);
    assertEquals(1, result.scoreDocs.length);
    assertEquals(1, result.scoreDocs[0].doc);
    assertEquals(1, result.totalHits.value());
  }

  @Test
  public void testSortedDescending() {
    List<BlendedScoreDoc> merged = new ArrayList<>();
    merged.add(doc(3, 0.3f));
    merged.add(doc(1, 1.0f));
    merged.add(doc(2, 0.5f));

    TopDocs result = BlenderOperation.sortAndPaginate(merged, 0, 10, 0f, false);

    assertEquals(3, result.scoreDocs.length);
    assertEquals(1, result.scoreDocs[0].doc); // score 1.0
    assertEquals(2, result.scoreDocs[1].doc); // score 0.5
    assertEquals(3, result.scoreDocs[2].doc); // score 0.3
  }

  @Test
  public void testTopHitsLimit() {
    List<BlendedScoreDoc> merged = new ArrayList<>();
    merged.add(doc(1, 1.0f));
    merged.add(doc(2, 0.8f));
    merged.add(doc(3, 0.6f));
    merged.add(doc(4, 0.4f));

    TopDocs result = BlenderOperation.sortAndPaginate(merged, 0, 2, 0f, false);

    assertEquals(2, result.scoreDocs.length);
    assertEquals(4, result.totalHits.value()); // total is deduplicated count, not page size
    assertEquals(1, result.scoreDocs[0].doc);
    assertEquals(2, result.scoreDocs[1].doc);
  }

  @Test
  public void testStartHitPagination() {
    List<BlendedScoreDoc> merged = new ArrayList<>();
    merged.add(doc(1, 1.0f));
    merged.add(doc(2, 0.8f));
    merged.add(doc(3, 0.6f));

    TopDocs result = BlenderOperation.sortAndPaginate(merged, 1, 10, 0f, false);

    assertEquals(2, result.scoreDocs.length);
    assertEquals(2, result.scoreDocs[0].doc); // doc 1 was skipped
    assertEquals(3, result.scoreDocs[1].doc);
  }

  @Test
  public void testStartHitBeyondResults() {
    List<BlendedScoreDoc> merged = List.of(doc(1, 1.0f), doc(2, 0.5f));
    TopDocs result = BlenderOperation.sortAndPaginate(merged, 5, 10, 0f, false);
    assertEquals(0, result.scoreDocs.length);
    assertEquals(2, result.totalHits.value());
  }

  @Test
  public void testMinScoreInclusive() {
    List<BlendedScoreDoc> merged = List.of(doc(1, 1.0f), doc(2, 0.5f), doc(3, 0.3f));

    TopDocs result = BlenderOperation.sortAndPaginate(merged, 0, 10, 0.5f, false);

    assertEquals(2, result.scoreDocs.length);
    assertEquals(1, result.scoreDocs[0].doc);
    assertEquals(2, result.scoreDocs[1].doc); // score == minScore is kept
    assertEquals(2, result.totalHits.value()); // total reflects the filtered count
  }

  @Test
  public void testMinScoreExclusive() {
    List<BlendedScoreDoc> merged = List.of(doc(1, 1.0f), doc(2, 0.5f), doc(3, 0.3f));

    TopDocs result = BlenderOperation.sortAndPaginate(merged, 0, 10, 0.5f, true);

    assertEquals(1, result.scoreDocs.length);
    assertEquals(1, result.scoreDocs[0].doc); // score == minScore is dropped
    assertEquals(1, result.totalHits.value());
  }

  @Test
  public void testMinScoreZeroExcludedDropsZeroScores() {
    List<BlendedScoreDoc> merged = List.of(doc(1, 0.0f), doc(2, 0.2f), doc(3, 0.0f));

    TopDocs result = BlenderOperation.sortAndPaginate(merged, 0, 10, 0f, true);

    assertEquals(1, result.scoreDocs.length);
    assertEquals(2, result.scoreDocs[0].doc);
    assertEquals(1, result.totalHits.value());
  }

  @Test
  public void testMinScoreDropsAll() {
    List<BlendedScoreDoc> merged = List.of(doc(1, 0.4f), doc(2, 0.2f));

    TopDocs result = BlenderOperation.sortAndPaginate(merged, 0, 10, 0.5f, false);

    assertEquals(0, result.scoreDocs.length);
    assertEquals(0, result.totalHits.value());
  }

  @Test
  public void testMinScoreWithPagination() {
    List<BlendedScoreDoc> merged = new ArrayList<>();
    merged.add(doc(1, 1.0f));
    merged.add(doc(2, 0.8f));
    merged.add(doc(3, 0.6f));
    merged.add(doc(4, 0.4f));
    merged.add(doc(5, 0.2f));

    // Threshold keeps docs 1-3; page of 2 starting at offset 1 within the filtered ranking.
    TopDocs result = BlenderOperation.sortAndPaginate(merged, 1, 3, 0.5f, false);

    assertEquals(2, result.scoreDocs.length);
    assertEquals(2, result.scoreDocs[0].doc);
    assertEquals(3, result.scoreDocs[1].doc);
    assertEquals(3, result.totalHits.value());
  }

  @Test
  public void testPassesMinScore() {
    assertTrue(BlenderOperation.passesMinScore(0.6f, 0.5f, false));
    assertTrue(BlenderOperation.passesMinScore(0.6f, 0.5f, true));
    assertTrue(BlenderOperation.passesMinScore(0.5f, 0.5f, false));
    assertFalse(BlenderOperation.passesMinScore(0.5f, 0.5f, true));
    assertFalse(BlenderOperation.passesMinScore(0.4f, 0.5f, false));
    assertFalse(BlenderOperation.passesMinScore(0.4f, 0.5f, true));
    // Defaults keep everything, including zero scores.
    assertTrue(BlenderOperation.passesMinScore(0f, 0f, false));
  }
}
