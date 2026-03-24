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
import java.util.List;
import org.apache.lucene.search.TopDocs;

/**
 * Result from executing a single {@link com.yelp.nrtsearch.server.grpc.KnnRetriever}. Extends
 * {@link RetrieverResult} with the {@link SearchResponse.Diagnostics.VectorDiagnostics} produced
 * during KNN query execution.
 */
public class KnnRetrieverResult extends RetrieverResult {

  private final List<SearchResponse.Diagnostics.VectorDiagnostics> vectorDiagnostics;

  public KnnRetrieverResult(
      String name,
      TopDocs topDocs,
      double timeTakenMs,
      List<SearchResponse.Diagnostics.VectorDiagnostics> vectorDiagnostics) {
    super(name, topDocs, timeTakenMs);
    this.vectorDiagnostics = List.copyOf(vectorDiagnostics);
  }

  /** Get the vector diagnostics produced during KNN query execution. */
  public List<SearchResponse.Diagnostics.VectorDiagnostics> getVectorDiagnostics() {
    return vectorDiagnostics;
  }
}
