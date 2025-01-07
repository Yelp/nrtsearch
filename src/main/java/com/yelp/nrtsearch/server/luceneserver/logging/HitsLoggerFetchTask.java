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
package com.yelp.nrtsearch.server.luceneserver.logging;

import com.yelp.nrtsearch.server.grpc.LoggingHits;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.search.FetchTasks.FetchTask;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.plugins.HitsLoggerPlugin;
import java.util.List;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * Implementation of {@link FetchTask} which holds the required context to be able to log hits for a
 * search request.
 */
public class HitsLoggerFetchTask implements FetchTask {
  private static final double TEN_TO_THE_POWER_SIX = Math.pow(10, 6);
  private final HitsLogger hitsLogger;
  private final int hitsToLog;
  private final DoubleAdder timeTakenMs = new DoubleAdder();

  public HitsLoggerFetchTask(LoggingHits loggingHits) {
    this.hitsLogger = HitsLoggerCreator.getInstance().createHitsLogger(loggingHits);
    this.hitsToLog = loggingHits.getHitsToLog();
  }

  /**
   * Calls {@link HitsLogger} that logs hits. The logic for logging is implemented via {@link
   * HitsLoggerPlugin}
   *
   * @param searchContext search context
   * @param hits list of hits for query response
   */
  @Override
  public void processAllHits(SearchContext searchContext, List<SearchResponse.Hit.Builder> hits) {
    long startTime = System.nanoTime();

    // hits list can contain extra hits that don't need to be logged, otherwise, pass all hits that
    // can be logged
    if (searchContext.getHitsToLog() < hits.size()) {
      hitsLogger.log(searchContext, hits.subList(0, searchContext.getHitsToLog()));
    } else {
      hitsLogger.log(searchContext, hits);
    }

    timeTakenMs.add(((System.nanoTime() - startTime) / TEN_TO_THE_POWER_SIX));
  }

  /**
   * Get the total time taken so far to logging hits.
   *
   * @return Total time taken to logging hits in ms.
   */
  public double getTimeTakenMs() {
    return timeTakenMs.doubleValue();
  }

  /**
   * Get the total number of hits to log.
   *
   * @return Total number of hits to log.
   */
  public int getHitsToLog() {
    return hitsToLog;
  }
}
