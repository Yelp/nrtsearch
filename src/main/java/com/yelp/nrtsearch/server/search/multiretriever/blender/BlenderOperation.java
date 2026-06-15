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
import java.util.PriorityQueue;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * Interface for blending results from multiple retrievers into a single ranked {@link TopDocs}.
 * Implementations are registered by name via {@link BlenderPlugin} and instantiated through {@link
 * BlenderCreator}.
 *
 * <p>The pipeline is:
 *
 * <ol>
 *   <li>{@link #mergeHits} — implementation-defined: deduplicate across retrievers and assign a
 *       blended {@link BlendedScoreDoc#score}.
 *   <li>{@link #blend} — framework default: calls {@link #mergeHits}, then {@link
 *       #sortAndPaginate}. Implementations may override {@link #blend} to skip sorting/pagination
 *       (e.g. {@link
 *       com.yelp.nrtsearch.server.search.multiretriever.blender.operation.ScorelessRawMergeBlenderOperation}).
 * </ol>
 *
 * <p>Each {@link BlendedScoreDoc} in the returned {@link TopDocs} preserves the raw per-retriever
 * hits in {@link BlendedScoreDoc#getScoreDocs()}, which the caller (SearchHandler) uses to build
 * retriever-score side values for downstream L2 rescorers.
 */
public interface BlenderOperation {

  /**
   * Merge per-retriever hits into an unsorted, unpaginated flat list. Implementations deduplicate
   * hits that appear in multiple retrievers and assign each a combined {@link
   * BlendedScoreDoc#score} (e.g. via RRF or a score-mode fold). The raw per-retriever hits must be
   * preserved in {@link BlendedScoreDoc#getScoreDocs()} for diagnostics and side-value extraction.
   *
   * @param retrieverResults per-retriever {@link TopDocs} in declaration order, keyed by name
   * @param retrieverContexts per-retriever contexts in declaration order, keyed by name
   * @return merged hits, unsorted and unpaginated
   */
  Collection<BlendedScoreDoc> mergeHits(
      LinkedHashMap<String, TopDocs> retrieverResults,
      LinkedHashMap<String, RetrieverContext> retrieverContexts);

  /**
   * Merges, sorts, and paginates resolved per-retriever results into a final ranked {@link
   * TopDocs}. The returned hits are {@link BlendedScoreDoc} instances carrying per-retriever score
   * breakdowns; the caller extracts these for response diagnostics and L2 rescore side values.
   *
   * @param retrieverResults per-retriever {@link TopDocs} in declaration order, keyed by name
   * @param retrieverContexts per-retriever contexts in declaration order, keyed by retriever name
   * @param startHit 0-based offset of the first blended hit to include in the result
   * @param topHits maximum number of blended hits to return; {@code 0} returns empty; must be >= 0
   * @return blended, sorted, paginated {@link TopDocs}
   */
  default TopDocs blend(
      LinkedHashMap<String, TopDocs> retrieverResults,
      LinkedHashMap<String, RetrieverContext> retrieverContexts,
      int startHit,
      int topHits) {
    if (topHits == 0 || startHit > topHits) {
      return new TopDocs(
          new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), new ScoreDoc[0]);
    }
    Collection<BlendedScoreDoc> merged = mergeHits(retrieverResults, retrieverContexts);
    return sortAndPaginate(merged, startHit, topHits);
  }

  /**
   * Selects the top-k window from merged hits in O(n log k) time using a min-heap, then returns
   * them in descending score order. The heap entries are drained in O(k log k) to produce the final
   * page without a full sort of all merged hits.
   *
   * @param merged unsorted merged hits from {@link #mergeHits}
   * @param startHit 0-based offset of the first hit to include in the returned page
   * @param topHits maximum number of hits to return; must be >= 0
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
