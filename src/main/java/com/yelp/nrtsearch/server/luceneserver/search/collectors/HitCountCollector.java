/*
 * Copyright 2022 Yelp Inc.
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

import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder;
import com.yelp.nrtsearch.server.grpc.SearchResponse.SearchState;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;

/**
 * Document collector that only tracks the count of recalled documents. Used when no hits need to be
 * recalled, such as when only using additional collectors.
 */
public class HitCountCollector extends DocCollector {
  private final HitCountCollectorManager manager;
  /**
   * Constructor
   *
   * @param context collector creation context
   * @param additionalCollectors additional collector implementations
   */
  public HitCountCollector(
      CollectorCreatorContext context,
      List<AdditionalCollectorManager<? extends Collector, ? extends CollectorResult>>
          additionalCollectors) {
    super(context, additionalCollectors);
    manager = new HitCountCollectorManager();
  }

  @Override
  public CollectorManager<? extends Collector, ? extends TopDocs> getManager() {
    return manager;
  }

  @Override
  public void fillHitRanking(Builder hitResponse, ScoreDoc scoreDoc) {}

  @Override
  public void fillLastHit(SearchState.Builder stateBuilder, ScoreDoc lastHit) {}

  /** Collector manager to do parallel collection of hit count. */
  public static class HitCountCollectorManager
      implements CollectorManager<TotalHitCountCollector, TopDocs> {

    @Override
    public TotalHitCountCollector newCollector() throws IOException {
      return new TotalHitCountCollector();
    }

    @Override
    public TopDocs reduce(Collection<TotalHitCountCollector> collectors) throws IOException {
      long count = 0;
      for (TotalHitCountCollector collector : collectors) {
        count += collector.getTotalHits();
      }
      return new TopDocs(new TotalHits(count, Relation.EQUAL_TO), new ScoreDoc[0]);
    }
  }
}
