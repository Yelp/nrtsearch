/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.search.retriever;

import com.yelp.nrtsearch.server.grpc.Blender;
import com.yelp.nrtsearch.server.grpc.FlatMapBlender;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * Built-in {@link BlenderOperation} implementing flat-map blending. Merges results from multiple
 * retrievers according to the {@link FlatMapBlender.MergeMode}:
 *
 * <ul>
 *   <li>{@code APPEND} — concatenate in retriever declaration order, no deduplication
 *   <li>{@code SCORE_ORDER_DEDUP} — deduplicate (keep highest score), sort by score descending
 *       (default when merge_mode is unset)
 *   <li>{@code RANK_ORDER_DEDUP} — deduplicate (keep best rank), sort by rank ascending
 *   <li>{@code PRIORITY_ORDER_DEDUP} — deduplicate (highest-priority retriever wins), group by
 *       retriever priority order
 * </ul>
 *
 * <p>The blended score for each hit is the score from whichever retriever's occurrence is kept.
 * After merging, {@link Blender#getTopHits()} and {@link Blender#getStartHit()} are applied to
 * produce the final page.
 */
public class FlatMapBlenderOperation implements BlenderOperation {

  @Override
  public TopDocs blend(
      LinkedHashMap<String, TopDocs> retrieverResults,
      Blender blender,
      LinkedHashMap<String, RetrieverContext> retrieverContexts) {
    FlatMapBlender config = blender.getFlatMap();
    FlatMapBlender.MergeMode mergeMode = config.getMergeMode();

    List<ScoreDoc> merged =
        switch (mergeMode) {
          case APPEND -> append(retrieverResults);
          case SCORE_ORDER_DEDUP -> scoreOrderDedup(retrieverResults);
          case RANK_ORDER_DEDUP -> rankOrderDedup(retrieverResults);
          case PRIORITY_ORDER_DEDUP ->
              priorityOrderDedup(retrieverResults, config, retrieverContexts);
          default -> throw new IllegalArgumentException("Unknown merge mode: " + mergeMode);
        };

    return paginate(merged, blender.getStartHit(), blender.getTopHits());
  }

  /** Concatenate all hits in retriever declaration order without deduplication. */
  private static List<ScoreDoc> append(Map<String, TopDocs> retrieverResults) {
    List<ScoreDoc> result = new ArrayList<>();
    for (TopDocs topDocs : retrieverResults.values()) {
      for (ScoreDoc sd : topDocs.scoreDocs) {
        result.add(new ScoreDoc(sd.doc, sd.score, sd.shardIndex));
      }
    }
    return result;
  }

  /**
   * Deduplicate by doc id keeping the highest score, then sort by score descending. Ties broken by
   * original rank (lower rank wins).
   */
  private static List<ScoreDoc> scoreOrderDedup(Map<String, TopDocs> retrieverResults) {
    // doc id -> best ScoreDoc seen so far
    Map<Integer, ScoreDoc> best = new LinkedHashMap<>();
    for (TopDocs topDocs : retrieverResults.values()) {
      for (ScoreDoc sd : topDocs.scoreDocs) {
        best.merge(
            sd.doc,
            new ScoreDoc(sd.doc, sd.score, sd.shardIndex),
            (existing, incoming) -> incoming.score > existing.score ? incoming : existing);
      }
    }
    List<ScoreDoc> result = new ArrayList<>(best.values());
    result.sort(Comparator.comparingDouble((ScoreDoc sd) -> sd.score).reversed());
    return result;
  }

  /**
   * Deduplicate by doc id keeping the occurrence with the best rank (first seen in iteration
   * order). Result order preserves rank order across all retrievers.
   */
  private static List<ScoreDoc> rankOrderDedup(Map<String, TopDocs> retrieverResults) {
    Map<Integer, ScoreDoc> seen = new LinkedHashMap<>();
    for (TopDocs topDocs : retrieverResults.values()) {
      for (ScoreDoc sd : topDocs.scoreDocs) {
        // putIfAbsent preserves the first (best-rank) occurrence
        seen.putIfAbsent(sd.doc, new ScoreDoc(sd.doc, sd.score, sd.shardIndex));
      }
    }
    return new ArrayList<>(seen.values());
  }

  /**
   * Deduplicate by doc id with priority given to the retriever highest in the priority list. Groups
   * results by retriever in priority order: all hits from the highest-priority retriever first,
   * then the next, etc. Retrievers not listed in {@code retriever_priority_order} are appended in
   * declaration order after the listed ones.
   */
  private static List<ScoreDoc> priorityOrderDedup(
      Map<String, TopDocs> retrieverResults,
      FlatMapBlender config,
      Map<String, RetrieverContext> retrieverContexts) {
    // Build the ordered list of retriever names: listed first, then unlisted in declaration order
    List<String> orderedNames = new ArrayList<>(config.getRetrieverPriorityOrderList());
    for (String name : retrieverResults.keySet()) {
      if (!orderedNames.contains(name)) {
        orderedNames.add(name);
      }
    }

    // Assign each doc to the highest-priority retriever that returned it
    Map<Integer, ScoreDoc> seen = new LinkedHashMap<>();
    List<ScoreDoc> result = new ArrayList<>();
    for (String name : orderedNames) {
      TopDocs topDocs = retrieverResults.get(name);
      if (topDocs == null) {
        continue;
      }
      for (ScoreDoc sd : topDocs.scoreDocs) {
        if (seen.putIfAbsent(sd.doc, new ScoreDoc(sd.doc, sd.score, sd.shardIndex)) == null) {
          result.add(seen.get(sd.doc));
        }
      }
    }
    return result;
  }

  /** Apply startHit/topHits pagination and wrap in a {@link TopDocs}. */
  private static TopDocs paginate(List<ScoreDoc> docs, int startHit, int topHits) {
    int total = docs.size(); // Extremely rough totalHits
    int from = Math.min(startHit, total);
    int to = topHits > 0 ? Math.min(from + topHits, total) : total;
    List<ScoreDoc> page = docs.subList(from, to);
    return new TopDocs(
        new TotalHits(total, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
        page.toArray(new ScoreDoc[0]));
  }
}
