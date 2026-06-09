/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.query.vector;

import org.apache.lucene.search.*;
import org.apache.lucene.search.knn.KnnSearchStrategy;

/**
 * A {@link KnnByteVectorQuery} that has its functionality slightly modified. The {@link TotalHits}
 * from the vector search are available after the query has been rewritten using the {@link
 * WithVectorTotalHits} interface. The results merging has also been modified to produce the top k
 * hits from the top numCandidates hits from each leaf.
 */
public class NrtKnnByteVectorQuery extends KnnByteVectorQuery implements WithVectorTotalHits {
  private final int topHits;
  private TotalHits totalHits;

  /**
   * Constructor.
   *
   * @param field field name
   * @param target query vector
   * @param k number of top hits to return
   * @param filter filter to use for vector search, or null
   * @param numCandidates number of candidates to retrieve from each leaf
   */
  public NrtKnnByteVectorQuery(
      String field,
      byte[] target,
      int k,
      Query filter,
      int numCandidates,
      KnnSearchStrategy searchStrategy) {
    super(field, target, numCandidates, filter, searchStrategy);
    this.topHits = k;
  }

  @Override
  public TotalHits getTotalHits() {
    return totalHits;
  }

  @Override
  protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
    TopDocs topDocs = TopDocs.merge(topHits, perLeafResults);
    totalHits = topDocs.totalHits;
    return topDocs;
  }
}
