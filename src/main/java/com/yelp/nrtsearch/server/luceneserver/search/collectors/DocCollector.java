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
package com.yelp.nrtsearch.server.luceneserver.search.collectors;

import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/** Interface for classes to manage the collection of documents when executing queries. */
public interface DocCollector {

  /**
   * Get a lucene level {@link CollectorManager}. This will be passed to {@link
   * org.apache.lucene.search.IndexSearcher#search(Query, CollectorManager)}.
   */
  CollectorManager<? extends Collector, ? extends TopDocs> getManager();

  /**
   * Fill the response hit for the given {@link ScoreDoc}. This method is expected to fill the
   * ranking information (Score or Sort info) for the response {@link SearchResponse.Hit}. It is
   * expected that the lucene doc id is already set in the Hit.
   *
   * @param hitResponse hit response message
   * @param scoreDoc doc from lucene query
   */
  void fillHitRanking(SearchResponse.Hit.Builder hitResponse, ScoreDoc scoreDoc);

  /**
   * Add information on the last hit into the search response.
   *
   * @param stateBuilder state message returned in response
   * @param lastHit last hit document
   */
  void fillLastHit(SearchResponse.SearchState.Builder stateBuilder, ScoreDoc lastHit);

  /**
   * Get the maximum number of hits that should be collected during ranking. This value will at
   * least be as large as the query specified top hits, and may be larger if doing a facet sample
   * aggregation requiring a greater number of top docs.
   *
   * @param request search request
   * @return total top docs needed for query response and sample facets
   */
  default int getNumHitsToCollect(SearchRequest request) {
    int collectHits = request.getTopHits();
    for (Facet facet : request.getFacetsList()) {
      int facetSample = facet.getSampleTopDocs();
      if (facetSample > 0 && facetSample > collectHits) {
        collectHits = facetSample;
      }
    }
    return collectHits;
  }
}
