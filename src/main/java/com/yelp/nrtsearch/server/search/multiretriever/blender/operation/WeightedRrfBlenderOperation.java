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
package com.yelp.nrtsearch.server.search.multiretriever.blender.operation;

import com.yelp.nrtsearch.server.grpc.Blender;
import com.yelp.nrtsearch.server.grpc.WeightedRrfBlender;
import com.yelp.nrtsearch.server.search.multiretriever.RetrieverContext;
import com.yelp.nrtsearch.server.search.multiretriever.blender.BlenderOperation;
import com.yelp.nrtsearch.server.search.multiretriever.blender.score.BlendedScoreDoc;
import com.yelp.nrtsearch.server.search.multiretriever.blender.score.WeightedRRFScoreDoc;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/**
 * {@link BlenderOperation} implementing Reciprocal Rank Fusion (RRF).
 *
 * <p>Each retriever contributes {@code boost / (k + rank)} to the fused score, where:
 *
 * <ul>
 *   <li>{@code rank} is the 1-based position in that retriever's result list,
 *   <li>{@code k} is the smoothing constant from {@link WeightedRrfBlender#getRankConstant()}
 *       (defaults to {@link WeightedRRFScoreDoc#DEFAULT_K} when 0),
 *   <li>{@code boost} is taken from {@link RetrieverContext#getBoost()}.
 * </ul>
 */
public class WeightedRrfBlenderOperation implements BlenderOperation {

  @Override
  public Collection<BlendedScoreDoc> mergeHits(
      LinkedHashMap<String, TopDocs> retrieverResults,
      Blender blender,
      LinkedHashMap<String, RetrieverContext> retrieverContexts) {

    WeightedRrfBlender rrfConfig = blender.getWeightedRrf();
    int k =
        rrfConfig.getRankConstant() > 0
            ? rrfConfig.getRankConstant()
            : WeightedRRFScoreDoc.DEFAULT_K;

    Map<Integer, BlendedScoreDoc> merged = new HashMap<>();

    for (Map.Entry<String, TopDocs> entry : retrieverResults.entrySet()) {
      ScoreDoc[] scoreDocs = entry.getValue().scoreDocs;
      float boost = retrieverContexts.get(entry.getKey()).getBoost();

      for (int i = 0; i < scoreDocs.length; i++) {
        ScoreDoc scoreDoc = scoreDocs[i];
        int rank = i + 1; // 1-based

        BlendedScoreDoc existing = merged.get(scoreDoc.doc);
        if (existing == null) {
          merged.put(
              scoreDoc.doc, new WeightedRRFScoreDoc(entry.getKey(), scoreDoc, rank, boost, k));
        } else {
          existing.add(entry.getKey(), rank, boost, scoreDoc);
        }
      }
    }

    return merged.values();
  }
}
