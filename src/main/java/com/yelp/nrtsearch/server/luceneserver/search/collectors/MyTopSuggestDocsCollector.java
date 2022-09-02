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
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.suggest.document.TopSuggestDocs;
import org.apache.lucene.search.suggest.document.TopSuggestDocsCollector;

public class MyTopSuggestDocsCollector extends DocCollector {
  private final MyTopSuggestDocsCollectorManager manager;

  public MyTopSuggestDocsCollector(
      CollectorCreatorContext context,
      List<AdditionalCollectorManager<? extends Collector, ? extends CollectorResult>>
          additionalCollectors) {
    super(context, additionalCollectors);
    manager = new MyTopSuggestDocsCollectorManager(getNumHitsToCollect());
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

  public static class MyTopSuggestDocsCollectorManager
      implements CollectorManager<TopSuggestDocsCollector, TopSuggestDocs> {
    private final int numHitsToCollect;

    public MyTopSuggestDocsCollectorManager(int numHitsToCollect) {
      this.numHitsToCollect = numHitsToCollect;
    }

    @Override
    public TopSuggestDocsCollector newCollector() throws IOException {
      return new TopSuggestDocsCollector(this.numHitsToCollect, true);
    }

    @Override
    public TopSuggestDocs reduce(Collection<TopSuggestDocsCollector> collectors)
        throws IOException {
      final TopSuggestDocs[] topDocs = new TopSuggestDocs[collectors.size()];
      int i = 0;
      for (TopSuggestDocsCollector collector : collectors) {
        topDocs[i++] = collector.get();
      }
      return TopSuggestDocs.merge(numHitsToCollect, topDocs);
    }
  }
}
