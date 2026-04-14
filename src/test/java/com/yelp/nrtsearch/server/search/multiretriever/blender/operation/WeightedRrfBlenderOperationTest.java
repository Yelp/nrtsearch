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

import com.yelp.nrtsearch.server.grpc.Blender;
import com.yelp.nrtsearch.server.grpc.WeightedRrfBlender;
import com.yelp.nrtsearch.server.search.multiretriever.RetrieverContext;
import com.yelp.nrtsearch.server.search.multiretriever.blender.score.BlendedScoreDoc;
import com.yelp.nrtsearch.server.search.multiretriever.blender.score.WeightedRRFScoreDoc;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.junit.Test;

public class WeightedRrfBlenderOperationTest {

  private static final float DELTA = 1e-6f;

  private static final WeightedRrfBlenderOperation OPERATION = new WeightedRrfBlenderOperation();

  private static Blender blenderWithK(int k) {
    return Blender.newBuilder()
        .setWeightedRrf(WeightedRrfBlender.newBuilder().setRankConstant(k).build())
        .build();
  }

  private static Blender defaultBlender() {
    return blenderWithK(0); // 0 → falls back to DEFAULT_K
  }

  private static TopDocs topDocs(int... docIds) {
    ScoreDoc[] scoreDocs = new ScoreDoc[docIds.length];
    for (int i = 0; i < docIds.length; i++) {
      scoreDocs[i] = new ScoreDoc(docIds[i], 1.0f, 0);
    }
    return new TopDocs(new TotalHits(docIds.length, TotalHits.Relation.EQUAL_TO), scoreDocs);
  }

  private static RetrieverContext retriever(String name, float boost) {
    return RetrieverContext.newBuilder(name).boost(boost).build();
  }

  private static Map<Integer, Float> toScoreMap(Collection<BlendedScoreDoc> docs) {
    return docs.stream().collect(Collectors.toMap(d -> d.doc, d -> d.score));
  }

  @Test
  public void testSingleRetriever() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(10, 20, 30));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 1.0f));

    Collection<BlendedScoreDoc> merged = OPERATION.mergeHits(results, blenderWithK(60), contexts);

    assertEquals(3, merged.size());
    Map<Integer, Float> scores = toScoreMap(merged);
    assertEquals(1.0f / 61, scores.get(10), DELTA); // rank 1
    assertEquals(1.0f / 62, scores.get(20), DELTA); // rank 2
    assertEquals(1.0f / 63, scores.get(30), DELTA); // rank 3
  }

  @Test
  public void testTwoRetrieversNoDuplicate() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(1));
    results.put("knn", topDocs(2));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 1.0f));
    contexts.put("knn", retriever("knn", 1.0f));

    Collection<BlendedScoreDoc> merged = OPERATION.mergeHits(results, blenderWithK(60), contexts);

    assertEquals(2, merged.size());
    Map<Integer, Float> scores = toScoreMap(merged);
    assertEquals(1.0f / 61, scores.get(1), DELTA);
    assertEquals(1.0f / 61, scores.get(2), DELTA);
  }

  @Test
  public void testDeduplication() {
    // doc 5 appears in both retrievers at rank 1 — scores should be summed
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(5));
    results.put("knn", topDocs(5));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 1.0f));
    contexts.put("knn", retriever("knn", 1.0f));

    Collection<BlendedScoreDoc> merged = OPERATION.mergeHits(results, blenderWithK(60), contexts);

    assertEquals(1, merged.size());
    BlendedScoreDoc doc = merged.iterator().next();
    assertEquals(5, doc.doc);
    assertEquals(1.0f / 61 + 1.0f / 61, doc.score, DELTA);
    assertEquals(2, doc.scoreDocs.size()); // baseDoc + second hit appended via add()
  }

  @Test
  public void testDefaultKFallbackWhenZero() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(1));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 1.0f));

    Collection<BlendedScoreDoc> merged = OPERATION.mergeHits(results, defaultBlender(), contexts);

    float expected = 1.0f / (WeightedRRFScoreDoc.DEFAULT_K + 1);
    assertEquals(expected, merged.iterator().next().score, DELTA);
  }

  @Test
  public void testCustomK() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(1));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 1.0f));

    Collection<BlendedScoreDoc> merged = OPERATION.mergeHits(results, blenderWithK(10), contexts);

    assertEquals(1.0f / 11, merged.iterator().next().score, DELTA);
  }

  @Test
  public void testBoostScalesScore() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(1));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 2.0f));

    Collection<BlendedScoreDoc> merged = OPERATION.mergeHits(results, blenderWithK(60), contexts);

    assertEquals(2.0f / 61, merged.iterator().next().score, DELTA);
  }

  @Test
  public void testEmptyRetrieverResults() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs());

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text", 1.0f));

    Collection<BlendedScoreDoc> merged = OPERATION.mergeHits(results, defaultBlender(), contexts);
    assertEquals(0, merged.size());
  }
}
