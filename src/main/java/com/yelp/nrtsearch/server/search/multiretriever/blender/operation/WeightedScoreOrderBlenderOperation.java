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

import com.yelp.nrtsearch.server.grpc.WeightedScoreOrderBlender;
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

/**
 * {@link BlenderOperation} that blends raw retriever scores weighted by each retriever's boost.
 *
 * <p>Each document's blended score is computed from {@code score * boost} contributions across all
 * retrievers, combined according to the configured {@link WeightedScoreDoc.ScoreMode}:
 *
 * <ul>
 *   <li>{@link WeightedScoreDoc.ScoreMode#MAX} — highest weighted score wins (default).
 *   <li>{@link WeightedScoreDoc.ScoreMode#SUM} — sum of all weighted scores.
 *   <li>{@link WeightedScoreDoc.ScoreMode#AVG} — average of all weighted scores.
 * </ul>
 */
public class WeightedScoreOrderBlenderOperation implements BlenderOperation {

  private final WeightedScoreDoc.ScoreMode scoreMode;

  public WeightedScoreOrderBlenderOperation(WeightedScoreOrderBlender config) {
    this.scoreMode = toScoreMode(config.getScoreMode());
  }

  @Override
  public Collection<BlendedScoreDoc> mergeHits(
      LinkedHashMap<String, TopDocs> retrieverResults,
      LinkedHashMap<String, RetrieverContext> retrieverContexts) {

    Map<Integer, BlendedScoreDoc> merged = new HashMap<>();

    for (Map.Entry<String, TopDocs> entry : retrieverResults.entrySet()) {
      ScoreDoc[] scoreDocs = entry.getValue().scoreDocs;
      float boost = retrieverContexts.get(entry.getKey()).getBoost();

      for (ScoreDoc scoreDoc : scoreDocs) {
        BlendedScoreDoc existing = merged.get(scoreDoc.doc);
        if (existing == null) {
          merged.put(
              scoreDoc.doc, new WeightedScoreDoc(entry.getKey(), scoreDoc, boost, scoreMode));
        } else {
          existing.add(entry.getKey(), 0, boost, scoreDoc);
        }
      }
    }

    return merged.values();
  }

  private static WeightedScoreDoc.ScoreMode toScoreMode(
      WeightedScoreOrderBlender.ScoreMode grpcMode) {
    return switch (grpcMode) {
      case MAX -> WeightedScoreDoc.ScoreMode.MAX;
      case SUM -> WeightedScoreDoc.ScoreMode.SUM;
      case AVG -> WeightedScoreDoc.ScoreMode.AVG;
      case UNRECOGNIZED ->
          throw new IllegalArgumentException("Unsupported score mode: " + grpcMode);
    };
  }
}
