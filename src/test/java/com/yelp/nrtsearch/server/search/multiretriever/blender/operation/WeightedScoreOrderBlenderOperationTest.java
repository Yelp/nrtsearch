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
package com.yelp.nrtsearch.server.search.multiretriever.blender.operation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.yelp.nrtsearch.server.grpc.WeightedScoreOrderBlender;
import com.yelp.nrtsearch.server.search.multiretriever.RetrieverContext;
import com.yelp.nrtsearch.server.search.multiretriever.blender.score.BlendedScoreDoc;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.junit.Test;

public class WeightedScoreOrderBlenderOperationTest {

  private static final float DELTA = 1e-6f;

  private static WeightedScoreOrderBlenderOperation operation(
      WeightedScoreOrderBlender.ScoreMode mode) {
    return new WeightedScoreOrderBlenderOperation(
        WeightedScoreOrderBlender.newBuilder().setScoreMode(mode).build());
  }

  private static WeightedScoreOrderBlenderOperation defaultOperation() {
    // Default proto enum value (MAX) is 0, so an unset scoreMode also yields MAX
    return new WeightedScoreOrderBlenderOperation(WeightedScoreOrderBlender.newBuilder().build());
  }

  private static TopDocs topDocs(int[] docIds, float[] scores) {
    ScoreDoc[] scoreDocs = new ScoreDoc[docIds.length];
    for (int i = 0; i < docIds.length; i++) {
      scoreDocs[i] = new ScoreDoc(docIds[i], scores[i], 0);
    }
    return new TopDocs(new TotalHits(docIds.length, TotalHits.Relation.EQUAL_TO), scoreDocs);
  }

  private static RetrieverContext retriever(String name, float boost) {
    return RetrieverContext.newBuilder(name).boost(boost).build();
  }

  private static Map<Integer, Float> toScoreMap(Collection<BlendedScoreDoc> docs) {
    return docs.stream().collect(Collectors.toMap(d -> d.doc, d -> d.score));
  }

  // ---- single retriever ----

  @Test
  public void testSingleRetriever() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(new int[] {10, 20}, new float[] {3.0f, 1.5f}));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 2.0f));

    Collection<BlendedScoreDoc> merged = defaultOperation().mergeHits(results, contexts);

    assertEquals(2, merged.size());
    Map<Integer, Float> scores = toScoreMap(merged);
    assertEquals(6.0f, scores.get(10), DELTA); // 3.0 * 2.0
    assertEquals(3.0f, scores.get(20), DELTA); // 1.5 * 2.0
  }

  // ---- two retrievers, no overlap ----

  @Test
  public void testTwoRetrieversNoDuplicate() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(new int[] {1}, new float[] {4.0f}));
    results.put("knn", topDocs(new int[] {2}, new float[] {2.0f}));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 1.0f));
    contexts.put("knn", retriever("knn", 1.0f));

    Map<Integer, Float> scores = toScoreMap(defaultOperation().mergeHits(results, contexts));
    assertEquals(4.0f, scores.get(1), DELTA);
    assertEquals(2.0f, scores.get(2), DELTA);
  }

  // ---- deduplication: MAX mode ----

  @Test
  public void testDeduplicationMax() {
    // doc 5 appears in both retrievers; MAX picks the higher weighted score
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(new int[] {5}, new float[] {3.0f}));
    results.put("knn", topDocs(new int[] {5}, new float[] {5.0f}));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 1.0f)); // weighted = 3.0
    contexts.put("knn", retriever("knn", 1.0f)); // weighted = 5.0

    Collection<BlendedScoreDoc> merged =
        operation(WeightedScoreOrderBlender.ScoreMode.MAX).mergeHits(results, contexts);

    assertEquals(1, merged.size());
    BlendedScoreDoc doc = merged.iterator().next();
    assertEquals(5, doc.doc);
    assertEquals(5.0f, doc.score, DELTA);
    assertEquals(2, doc.getScoreDocs().size());
  }

  // ---- deduplication: SUM mode ----

  @Test
  public void testDeduplicationSum() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(new int[] {5}, new float[] {3.0f}));
    results.put("knn", topDocs(new int[] {5}, new float[] {2.0f}));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 2.0f)); // weighted = 6.0
    contexts.put("knn", retriever("knn", 1.0f)); // weighted = 2.0

    Collection<BlendedScoreDoc> merged =
        operation(WeightedScoreOrderBlender.ScoreMode.SUM).mergeHits(results, contexts);

    assertEquals(1, merged.size());
    assertEquals(8.0f, merged.iterator().next().score, DELTA); // 6.0 + 2.0
  }

  // ---- deduplication: AVG mode ----

  @Test
  public void testDeduplicationAvg() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(new int[] {5}, new float[] {4.0f}));
    results.put("knn", topDocs(new int[] {5}, new float[] {2.0f}));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 1.0f)); // weighted = 4.0
    contexts.put("knn", retriever("knn", 1.0f)); // weighted = 2.0

    Collection<BlendedScoreDoc> merged =
        operation(WeightedScoreOrderBlender.ScoreMode.AVG).mergeHits(results, contexts);

    assertEquals(1, merged.size());
    assertEquals(3.0f, merged.iterator().next().score, DELTA); // (4.0 + 2.0) / 2
  }

  // ---- boost scaling ----

  @Test
  public void testBoostScalesScore() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(new int[] {1}, new float[] {2.0f}));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 3.0f));

    Map<Integer, Float> scores = toScoreMap(defaultOperation().mergeHits(results, contexts));
    assertEquals(6.0f, scores.get(1), DELTA); // 2.0 * 3.0
  }

  // ---- default score mode (unset enum → MAX) ----

  @Test
  public void testDefaultScoreModeIsMax() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(new int[] {7}, new float[] {1.0f}));
    results.put("knn", topDocs(new int[] {7}, new float[] {5.0f}));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 1.0f));
    contexts.put("knn", retriever("knn", 1.0f));

    Collection<BlendedScoreDoc> merged = defaultOperation().mergeHits(results, contexts);
    assertEquals(5.0f, merged.iterator().next().score, DELTA);
  }

  // ---- unknown score mode ----

  @Test
  public void testUnrecognizedScoreModeThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new WeightedScoreOrderBlenderOperation(
                WeightedScoreOrderBlender.newBuilder()
                    .setScoreModeValue(99) // unknown proto enum value → UNRECOGNIZED
                    .build()));
  }

  // ---- empty results ----

  @Test
  public void testEmptyRetrieverResults() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(new int[] {}, new float[] {}));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 1.0f));

    assertEquals(0, defaultOperation().mergeHits(results, contexts).size());
  }
}
