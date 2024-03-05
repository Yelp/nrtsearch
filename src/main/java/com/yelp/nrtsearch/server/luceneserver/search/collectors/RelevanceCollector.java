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

import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import java.util.List;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Collector for getting documents ranked by relevance score. */
public class RelevanceCollector extends DocCollector {
  private static final Logger logger = LoggerFactory.getLogger(RelevanceCollector.class);

  private final CollectorManager<TopScoreDocCollector, TopDocs> manager;

  public RelevanceCollector(
      CollectorCreatorContext context,
      List<AdditionalCollectorManager<? extends Collector, ? extends CollectorResult>>
          additionalCollectors) {
    super(context, additionalCollectors);
    FieldDoc searchAfter = null;
    int topHits = getNumHitsToCollect();
    int totalHitsThreshold = TOTAL_HITS_THRESHOLD;
    // if there are additional collectors, we cannot skip any recalled docs
    if (!additionalCollectors.isEmpty()) {
      totalHitsThreshold = Integer.MAX_VALUE;
      if (context.getRequest().getTotalHitsThreshold() != 0) {
        logger.warn("Query totalHitsThreshold ignored when using additional collectors");
      }
    } else if (context.getRequest().getTotalHitsThreshold() != 0) {
      totalHitsThreshold = context.getRequest().getTotalHitsThreshold();
    }
    manager = TopScoreDocCollector.createSharedManager(topHits, searchAfter, totalHitsThreshold);
  }

  @Override
  public CollectorManager<? extends Collector, ? extends TopDocs> getManager() {
    return manager;
  }

  @Override
  public void fillHitRanking(SearchResponse.Hit.Builder hitResponse, ScoreDoc scoreDoc) {
    if (!Float.isNaN(scoreDoc.score)) {
      hitResponse.setScore(scoreDoc.score);
    }
  }

  @Override
  public void fillLastHit(SearchResponse.SearchState.Builder stateBuilder, ScoreDoc lastHit) {
    stateBuilder.setLastScore(lastHit.score);
  }
}
