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

import static com.yelp.nrtsearch.server.luceneserver.search.SearchRequestProcessor.TOTAL_HITS_THRESHOLD;

import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;

/** Collector for getting documents ranked by score. */
public class ScoreCollector implements DocCollector {

  private final CollectorManager<TopScoreDocCollector, TopDocs> manager;

  public ScoreCollector(SearchContext context, SearchRequest searchRequest) {
    FieldDoc searchAfter = null;
    int topHits = searchRequest.getTopHits();
    int totalHitsThreshold = TOTAL_HITS_THRESHOLD;
    if (searchRequest.getTotalHitsThreshold() != 0) {
      totalHitsThreshold = searchRequest.getTotalHitsThreshold();
    }
    manager = TopScoreDocCollector.createSharedManager(topHits, searchAfter, totalHitsThreshold);
  }

  @Override
  public CollectorManager<? extends Collector, ? extends TopDocs> getManager() {
    return manager;
  }

  @Override
  public void fillLastHit(SearchResponse.SearchState.Builder stateBuilder, ScoreDoc lastHit) {
    stateBuilder.setLastScore(lastHit.score);
  }
}
