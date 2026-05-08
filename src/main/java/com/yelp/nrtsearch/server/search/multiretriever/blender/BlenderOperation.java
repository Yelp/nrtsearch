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

import com.yelp.nrtsearch.server.plugins.BlenderPlugin;
import com.yelp.nrtsearch.server.search.multiretriever.RetrieverContext;
import com.yelp.nrtsearch.server.search.multiretriever.blender.score.BlendedScoreDoc;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * Interface for blending results from multiple retrievers into a single ranked {@link TopDocs}.
 * Implementations are registered by name via {@link BlenderPlugin} and instantiated through {@link
 * BlenderCreator}.
 *
 * <p>The entry point is {@link #blend}, which accepts already-submitted per-retriever {@link
 * Future}s (keyed by name), collects their results, then merges, sorts, and paginates. Callers are
 * responsible for submitting retriever searches to an executor before calling {@code blend}; the
 * futures run concurrently and this method blocks until all complete.
 *
 * <p>Implementations define the merge logic in {@link #mergeHits}, which operates on the raw
 * per-retriever hits before sorting and pagination are applied by the framework.
 */
public interface BlenderOperation {

  /**
   * Merge per-retriever hits into an unsorted, unpaginated flat list. Implementations deduplicate
   * hits that appear in multiple retrievers and assign each a combined score (e.g. via RRF or a
   * score-mode fold). The returned collection will be sorted by {@link BlendedScoreDoc#score} and
   * paginated by the caller.
   *
   * @param retrieverResults per-retriever {@link TopDocs} in declaration order, keyed by name
   * @param retrieverContexts per-retriever contexts in declaration order, keyed by name
   * @return merged hits, unsorted and unpaginated
   */
  Collection<BlendedScoreDoc> mergeHits(
      LinkedHashMap<String, TopDocs> retrieverResults,
      LinkedHashMap<String, RetrieverContext> retrieverContexts);

  /**
   * Collects results from already-submitted retriever futures, then merges, sorts, and paginates.
   * Blocks until all futures complete. Callers must submit retriever searches to an executor before
   * calling this method so that the futures run concurrently.
   *
   * @param retrieverFutures per-retriever futures in declaration order, keyed by retriever name
   * @param retrieverContexts per-retriever contexts in declaration order, keyed by retriever name
   * @param startHit 0-based offset of the first blended hit to include in the result
   * @param topHits maximum number of blended hits to return; {@code 0} returns empty; must be >= 0
   * @return blended, sorted, paginated {@link TopDocs}
   * @throws RuntimeException wrapping any retriever {@link ExecutionException}, identified by name
   * @throws InterruptedException if the calling thread is interrupted while waiting
   */
  default TopDocs blend(
      LinkedHashMap<String, Future<TopDocs>> retrieverFutures,
      LinkedHashMap<String, RetrieverContext> retrieverContexts,
      int startHit,
      int topHits)
      throws InterruptedException {
    if (topHits == 0 || startHit > topHits) {
      return new TopDocs(
          new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), new ScoreDoc[0]);
    }
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
    return sortAndPaginate(merged, startHit, topHits);
  }

  /**
   * Select and return the requested pagination window from merged hits without sorting the full
   * list. Uses a min-heap of size {@code k = topHits} to find the top-k hits in O(n log k) time and
   * O(k) space. The heap entries are then sorted in O(k log k) to produce the final page order.
   *
   * @param merged unsorted merged hits from {@link #mergeHits}
   * @param startHit 0-based offset of the first hit to include in the returned page
   * @param topHits maximum number of hits to return; {@code 0} returns empty; must be >= 0
   * @return paginated {@link TopDocs}
   */
  static TopDocs sortAndPaginate(Collection<BlendedScoreDoc> merged, int startHit, int topHits) {
    int total = merged.size();
    // Heap capacity is bounded by topHits to avoid materializing docs outside the window.
    // `total` is preserved as the true deduplicated hit count for TotalHits reporting.
    int capacity = Math.min(topHits, total);

    // Min-heap of size capacity ordered by score ascending: the root is always the
    // smallest score in the heap, so any incoming doc that beats the root displaces it.
    PriorityQueue<BlendedScoreDoc> heap =
        new PriorityQueue<>(capacity + 1, (a, b) -> Float.compare(a.score, b.score));

    for (BlendedScoreDoc doc : merged) {
      if (heap.size() < capacity) {
        heap.offer(doc);
      } else if (doc.score > heap.peek().score) {
        heap.poll();
        heap.offer(doc);
      }
    }

    // Drain via poll() — each call removes the current minimum — filling the array from the end
    // so the result is in descending score order without a separate sort pass.
    int heapSize = heap.size();
    BlendedScoreDoc[] topK = new BlendedScoreDoc[heapSize];
    for (int i = heapSize - 1; i >= 0; i--) {
      topK[i] = heap.poll();
    }

    // Slice out the requested page window.
    int from = Math.min(startHit, heapSize);
    ScoreDoc[] page = Arrays.copyOfRange(topK, from, heapSize, ScoreDoc[].class);

    return new TopDocs(new TotalHits(total, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), page);
  }
}
