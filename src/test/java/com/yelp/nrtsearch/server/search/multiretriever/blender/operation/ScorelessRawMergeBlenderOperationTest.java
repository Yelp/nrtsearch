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

import com.yelp.nrtsearch.server.search.multiretriever.RetrieverContext;
import com.yelp.nrtsearch.server.search.multiretriever.blender.score.BlendedScoreDoc;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.junit.Test;

public class ScorelessRawMergeBlenderOperationTest {

  private static final float DELTA = 1e-6f;

  private static TopDocs topDocs(int[] docIds, float[] scores) {
    ScoreDoc[] scoreDocs = new ScoreDoc[docIds.length];
    for (int i = 0; i < docIds.length; i++) {
      scoreDocs[i] = new ScoreDoc(docIds[i], scores[i], 0);
    }
    return new TopDocs(new TotalHits(docIds.length, TotalHits.Relation.EQUAL_TO), scoreDocs);
  }

  private static RetrieverContext retriever(String name) {
    return RetrieverContext.newBuilder(name).build();
  }

  private static Future<TopDocs> future(TopDocs td) {
    return CompletableFuture.completedFuture(td);
  }

  private static Map<Integer, BlendedScoreDoc> toDocMap(Collection<BlendedScoreDoc> docs) {
    return docs.stream().collect(Collectors.toMap(d -> d.doc, d -> d));
  }

  // ---- mergeHits ----

  @Test
  public void testAllScoresAreZero() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(new int[] {1, 2}, new float[] {3.0f, 5.0f}));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text"));

    Collection<BlendedScoreDoc> merged =
        new ScorelessRawMergeBlenderOperation().mergeHits(results, contexts);

    assertEquals(2, merged.size());
    for (BlendedScoreDoc doc : merged) {
      assertEquals(0f, doc.score, DELTA);
    }
  }

  @Test
  public void testDeduplicationKeepsAllRetrieverHits() {
    // doc 5 in both retrievers — deduped to one entry, both raw hits preserved
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(new int[] {5}, new float[] {2.0f}));
    results.put("knn", topDocs(new int[] {5}, new float[] {4.0f}));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text"));
    contexts.put("knn", retriever("knn"));

    Collection<BlendedScoreDoc> merged =
        new ScorelessRawMergeBlenderOperation().mergeHits(results, contexts);

    assertEquals(1, merged.size());
    BlendedScoreDoc doc = merged.iterator().next();
    assertEquals(5, doc.doc);
    assertEquals(0f, doc.score, DELTA);
    assertEquals(2, doc.getScoreDocs().size());
  }

  @Test
  public void testRawScoresPreservedInScoreDocs() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(new int[] {5}, new float[] {2.0f}));
    results.put("knn", topDocs(new int[] {5}, new float[] {4.0f}));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text"));
    contexts.put("knn", retriever("knn"));

    BlendedScoreDoc doc =
        new ScorelessRawMergeBlenderOperation().mergeHits(results, contexts).iterator().next();

    assertEquals(2.0f, doc.getScoreDocs().get("text").score, DELTA);
    assertEquals(4.0f, doc.getScoreDocs().get("knn").score, DELTA);
  }

  @Test
  public void testNonOverlappingRetrievers() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(new int[] {1}, new float[] {1.0f}));
    results.put("knn", topDocs(new int[] {2}, new float[] {1.0f}));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text"));
    contexts.put("knn", retriever("knn"));

    Collection<BlendedScoreDoc> merged =
        new ScorelessRawMergeBlenderOperation().mergeHits(results, contexts);

    assertEquals(2, merged.size());
    Map<Integer, BlendedScoreDoc> byDoc = toDocMap(merged);
    assertEquals(1, byDoc.get(1).getScoreDocs().size());
    assertEquals(1, byDoc.get(2).getScoreDocs().size());
  }

  @Test
  public void testEmptyResults() {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    results.put("text", topDocs(new int[] {}, new float[] {}));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text"));

    assertEquals(0, new ScorelessRawMergeBlenderOperation().mergeHits(results, contexts).size());
  }

  // ---- blend: no sort, no pagination ----

  @Test
  public void testBlendIgnoresPagination() throws InterruptedException {
    LinkedHashMap<String, Future<TopDocs>> futures = new LinkedHashMap<>();
    futures.put(
        "text", future(topDocs(new int[] {1, 2, 3, 4, 5}, new float[] {1f, 1f, 1f, 1f, 1f})));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text"));

    TopDocs result = new ScorelessRawMergeBlenderOperation().blend(futures, contexts, 2, 2);

    // All 5 hits returned despite startHit=2 topHits=2
    assertEquals(5, result.scoreDocs.length);
  }

  @Test
  public void testBlendDeduplicates() throws InterruptedException {
    LinkedHashMap<String, Future<TopDocs>> futures = new LinkedHashMap<>();
    futures.put("text", future(topDocs(new int[] {7}, new float[] {1f})));
    futures.put("knn", future(topDocs(new int[] {7}, new float[] {2f})));

    LinkedHashMap<String, RetrieverContext> contexts = new LinkedHashMap<>();
    contexts.put("text", retriever("text"));
    contexts.put("knn", retriever("knn"));

    TopDocs result = new ScorelessRawMergeBlenderOperation().blend(futures, contexts, 0, 10);

    assertEquals(1, result.scoreDocs.length);
    assertEquals(0f, result.scoreDocs[0].score, DELTA);
  }
}
