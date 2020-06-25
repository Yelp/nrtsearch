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
package com.yelp.nrtsearch.server.luceneserver;

import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LargeNumHitsTopDocsCollector;
import org.apache.lucene.search.TopDocs;

public class LargeNumHitsTopDocsCollectorManagerCreator {
  /**
   * Create a CollectorManager of LargeNumHitsTopDocsCollectors to facilitate parallel segment
   * search on LargeNumHitsTopDocsCollector
   */
  public static CollectorManager<LargeNumHitsTopDocsCollector, TopDocs> createSharedManager(
      int numHits) {
    return new CollectorManager<LargeNumHitsTopDocsCollector, TopDocs>() {

      @Override
      public LargeNumHitsTopDocsCollector newCollector() throws IOException {
        return new LargeNumHitsTopDocsCollector(numHits);
      }

      @Override
      public TopDocs reduce(Collection<LargeNumHitsTopDocsCollector> collectors)
          throws IOException {
        final TopDocs[] topDocs = new TopDocs[collectors.size()];
        int i = 0;
        for (LargeNumHitsTopDocsCollector collector : collectors) {
          topDocs[i++] = collector.topDocs();
        }
        return TopDocs.merge(numHits, topDocs);
      }
    };
  }
}
