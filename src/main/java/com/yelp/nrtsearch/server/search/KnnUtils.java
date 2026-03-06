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
package com.yelp.nrtsearch.server.search;

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.query.MinThresholdQuery;
import com.yelp.nrtsearch.server.query.vector.WithVectorTotalHits;
import java.io.IOException;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;

public class KnnUtils {
  /**
   * Resolve (execute) the knn query and apply the boost. Resolving the query produces a new query
   * that matches the vector top hits. The boost is applied to this new query.
   *
   * @param knnQuery lucene knn query
   * @param boost boost to apply to the query
   * @param indexSearcher index searcher
   * @param vectorDiagnosticsBuilder diagnostics builder for vector search
   * @return vector search results query with boost applied
   * @throws IOException
   */
  public static Query resolveKnnQueryAndBoost(
      Query knnQuery,
      float boost,
      IndexSearcher indexSearcher,
      SearchResponse.Diagnostics.VectorDiagnostics.Builder vectorDiagnosticsBuilder)
      throws IOException {

    long vectorSearchStart = System.nanoTime();
    // Rewriting the query executes the vector search using the executor from the index searcher
    Query rewrittenQuery = knnQuery.rewrite(indexSearcher);

    // fill diagnostic info
    vectorDiagnosticsBuilder.setSearchTimeMs(((System.nanoTime() - vectorSearchStart) / 1000000.0));
    setVectorTotalHits(knnQuery, vectorDiagnosticsBuilder);

    if (boost != 1.0f) {
      rewrittenQuery = new BoostQuery(rewrittenQuery, boost);
    }
    return rewrittenQuery;
  }

  private static void setVectorTotalHits(
      Query knnQuery,
      SearchResponse.Diagnostics.VectorDiagnostics.Builder vectorDiagnosticsBuilder) {
    Query vectorQuery = knnQuery;
    if (vectorQuery instanceof MinThresholdQuery minThresholdQuery) {
      vectorQuery = minThresholdQuery.getWrapped();
    }
    if (vectorQuery instanceof WithVectorTotalHits withVectorTotalHits) {
      TotalHits vectorTotalHits = withVectorTotalHits.getTotalHits();
      vectorDiagnosticsBuilder.setTotalHits(
          com.yelp.nrtsearch.server.grpc.TotalHits.newBuilder()
              .setRelation(
                  com.yelp.nrtsearch.server.grpc.TotalHits.Relation.valueOf(
                      vectorTotalHits.relation().name()))
              .setValue(vectorTotalHits.value())
              .build());
    }
  }
}
