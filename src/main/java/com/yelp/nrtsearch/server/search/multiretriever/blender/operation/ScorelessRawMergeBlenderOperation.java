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

import com.yelp.nrtsearch.server.search.multiretriever.RetrieverContext;
import com.yelp.nrtsearch.server.search.multiretriever.blender.BlenderOperation;
import com.yelp.nrtsearch.server.search.multiretriever.blender.score.BlendedScoreDoc;
import com.yelp.nrtsearch.server.search.multiretriever.blender.score.WeightedScoreDoc;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * {@link BlenderOperation} that deduplicates hits across retrievers (accumulating per-retriever
 * {@link ScoreDoc}s in a single {@link WeightedScoreDoc}) but assigns score 0 to every result and
 * skips sorting and pagination. Intended for callers that handle ranking and pagination on the
 * client side.
 */
public class ScorelessRawMergeBlenderOperation implements BlenderOperation {

  /**
   * Deduplicates hits by doc ID across retrievers. Each unique document is represented by a {@link
   * WeightedScoreDoc} with weight 0, so {@link BlendedScoreDoc#score} remains 0 regardless of how
   * many retrievers contributed. All per-retriever hits for the same document are preserved in
   * {@link BlendedScoreDoc#getScoreDocs()}.
   */
  @Override
  public Collection<BlendedScoreDoc> mergeHits(
      LinkedHashMap<String, TopDocs> retrieverResults,
      LinkedHashMap<String, RetrieverContext> retrieverContexts) {

    Map<Integer, BlendedScoreDoc> merged = new HashMap<>();

    for (Map.Entry<String, TopDocs> entry : retrieverResults.entrySet()) {
      for (ScoreDoc scoreDoc : entry.getValue().scoreDocs) {
        BlendedScoreDoc existing = merged.get(scoreDoc.doc);
        if (existing == null) {
          merged.put(
              scoreDoc.doc,
              new WeightedScoreDoc(entry.getKey(), scoreDoc, 0f, WeightedScoreDoc.ScoreMode.MAX));
        } else {
          existing.add(entry.getKey(), 0, 0f, scoreDoc);
        }
      }
    }

    return merged.values();
  }

  /**
   * Collects results via {@link #mergeHits}, then returns them as-is without sorting or pagination.
   * The {@code startHit} and {@code topHits} parameters are ignored.
   */
  @Override
  public TopDocs blend(
      LinkedHashMap<String, Future<TopDocs>> retrieverFutures,
      LinkedHashMap<String, RetrieverContext> retrieverContexts,
      int startHit,
      int topHits)
      throws InterruptedException {
    LinkedHashMap<String, TopDocs> results = new LinkedHashMap<>();
    for (Map.Entry<String, Future<TopDocs>> entry : retrieverFutures.entrySet()) {
      String name = entry.getKey();
      try {
        results.put(name, entry.getValue().get());
      } catch (ExecutionException e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        throw new RuntimeException("Retriever '" + name + "' failed: " + cause.getMessage(), cause);
      }
    }

    Collection<BlendedScoreDoc> merged = mergeHits(results, retrieverContexts);
    return new TopDocs(
        new TotalHits(merged.size(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
        merged.toArray(ScoreDoc[]::new));
  }
}
