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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;

/**
 * A {@link DiversifyingChildrenFloatKnnVectorQuery} that has its functionality slightly modified.
 * The {@link TotalHits} from the vector search are available after the query has been rewritten
 * using the {@link WithVectorTotalHits} interface. The results merging has also been modified to
 * produce the top k hits from the top numCandidates hits from each leaf.
 */
public class NrtDiversifyingChildrenFloatKnnVectorQuery
    extends DiversifyingChildrenFloatKnnVectorQuery implements WithVectorTotalHits {
  private final int topHits;
  private TotalHits totalHits;

  public NrtDiversifyingChildrenFloatKnnVectorQuery(
      String field,
      float[] target,
      Query filter,
      int k,
      int numCandidates,
      BitSetProducer parentsFilter) {
    super(field, target, filter, numCandidates, parentsFilter);
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
