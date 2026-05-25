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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * {@link BlenderOperation} that deduplicates hits across retrievers and assigns score {@code 0} to
 * every result without sorting or paginating. This is intended for use with an L2 {@link
 * com.yelp.nrtsearch.server.rescore.ScriptRescore} that takes full ownership of ranking via
 * retriever side values (e.g. {@code _side_retriever_text}, {@code _side_retriever_knn}).
 */
public class ScorelessRawMergeBlenderOperation implements BlenderOperation {

  /**
   * Deduplicates hits by doc ID. Each unique document is represented by a {@link WeightedScoreDoc}
   * with weight {@code 0}, so {@link BlendedScoreDoc#score} stays {@code 0} regardless of how many
   * retrievers contributed. All per-retriever raw hits are preserved in {@link
   * BlendedScoreDoc#getScoreDocs()} for side-value extraction and response diagnostics.
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
   * Returns all merged hits without sorting or pagination. The {@code startHit} and {@code topHits}
   * parameters are ignored; an L2 rescorer is expected to produce the final ordering.
   */
  @Override
  public TopDocs blend(
      LinkedHashMap<String, TopDocs> retrieverResults,
      LinkedHashMap<String, RetrieverContext> retrieverContexts,
      int startHit,
      int topHits) {
    Collection<BlendedScoreDoc> merged = mergeHits(retrieverResults, retrieverContexts);
    return new TopDocs(
        new TotalHits(merged.size(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
        merged.toArray(ScoreDoc[]::new));
  }
}
