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

import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.KnnRetriever;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.index.IndexState;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;

/**
 * Executes a single {@link KnnRetriever} and returns a {@link KnnRetrieverResult} containing the
 * top documents, execution time, and vector diagnostics.
 */
public class KnnRetrieverExecutor implements Callable<KnnRetrieverResult> {

  private static final float DEFAULT_BOOST = 1.0f;

  private final String name;
  private final KnnRetriever knnRetriever;
  private final float boost;
  private final IndexState indexState;
  private final IndexSearcher indexSearcher;

  public KnnRetrieverExecutor(
      Retriever retriever, IndexState indexState, IndexSearcher indexSearcher) {
    this.name = retriever.getName();
    this.knnRetriever = retriever.getKnnRetriever();
    this.boost = retriever.hasBoost() ? retriever.getBoost() : DEFAULT_BOOST;
    this.indexState = indexState;
    this.indexSearcher = indexSearcher;
  }

  @Override
  public KnnRetrieverResult call() throws Exception {
    long startNs = System.nanoTime();

    KnnQuery knnQuery = knnRetriever.getKnnQuery();

    // Build the Lucene KNN query (handles nested fields, filters)
    Query luceneKnnQuery = KnnUtils.buildKnnQuery(knnQuery, indexState);

    // Resolve the KNN query via rewrite and apply boost
    SearchResponse.Diagnostics.VectorDiagnostics.Builder vectorDiagnosticsBuilder =
        SearchResponse.Diagnostics.VectorDiagnostics.newBuilder();
    Query resolvedQuery =
        KnnUtils.resolveKnnQueryAndBoost(
            luceneKnnQuery, boost, indexSearcher, vectorDiagnosticsBuilder);

    // Collect the pre-computed KNN hits into TopDocs
    int k = knnQuery.getK();
    TopDocs topDocs =
        indexSearcher.search(
            resolvedQuery, new TopScoreDocCollectorManager(k, null, Integer.MAX_VALUE));

    double timeTakenMs = (System.nanoTime() - startNs) / 1_000_000.0;
    return new KnnRetrieverResult(
        name, topDocs, timeTakenMs, List.of(vectorDiagnosticsBuilder.build()));
  }
}
