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

import com.yelp.nrtsearch.server.grpc.KnnRetriever;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.search.collectors.CollectorCreatorContext;
import com.yelp.nrtsearch.server.search.collectors.RelevanceCollector;
import java.util.Collections;
import java.util.concurrent.Callable;
import org.apache.lucene.search.Query;

/**
 * Executes a single {@link KnnRetriever} and returns a {@link SearcherResult} containing the top
 * documents. The caller is responsible for building the Lucene KNN query via {@link
 * KnnUtils#buildKnnQuery} before constructing this executor.
 */
public class KnnRetrieverExecutor implements Callable<SearcherResult> {

  private static final float DEFAULT_BOOST = 1.0f;

  private final float boost;
  private final Query luceneKnnQuery;
  private final CollectorCreatorContext collectorCreatorContext;
  private final SearchResponse.Diagnostics.VectorDiagnostics.Builder vectorDiagnosticsBuilder;

  public KnnRetrieverExecutor(
      Retriever retriever,
      Query luceneKnnQuery,
      CollectorCreatorContext collectorCreatorContext,
      SearchResponse.Diagnostics.VectorDiagnostics.Builder vectorDiagnosticsBuilder) {
    this.boost = retriever.hasBoost() ? retriever.getBoost() : DEFAULT_BOOST;
    this.luceneKnnQuery = luceneKnnQuery;
    this.collectorCreatorContext = collectorCreatorContext;
    this.vectorDiagnosticsBuilder = vectorDiagnosticsBuilder;
  }

  @Override
  public SearcherResult call() throws Exception {
    Query resolvedQuery =
        KnnUtils.resolveKnnQueryAndBoost(
            luceneKnnQuery,
            boost,
            collectorCreatorContext.getSearcherAndTaxonomy().searcher(),
            vectorDiagnosticsBuilder);

    RelevanceCollector collector =
        new RelevanceCollector(collectorCreatorContext, Collections.emptyList());
    return collectorCreatorContext
        .getSearcherAndTaxonomy()
        .searcher()
        .search(resolvedQuery, collector.getWrappedManager());
  }
}
