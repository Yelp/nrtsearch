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
package com.yelp.nrtsearch.server.logging;

import com.yelp.nrtsearch.server.grpc.LoggingHits;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.plugins.HitsLoggerPlugin;
import com.yelp.nrtsearch.server.search.FetchTasks.FetchTask;
import com.yelp.nrtsearch.server.search.SearchContext;
import java.util.List;

/**
 * Implementation of {@link FetchTask} which holds the required context to be able to log hits for a
 * search request.
 */
public class HitsLoggerFetchTask implements FetchTask {
  private final HitsLogger hitsLogger;
  private final int hitsToLog;

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
    // hits list can contain extra hits that don't need to be logged, otherwise, pass all hits that can be logged
    if (searchContext.getHitsToLog() < hits.size()) {
      hitsLogger.log(searchContext, hits.subList(0, searchContext.getHitsToLog()));
    } else {
      hitsLogger.log(searchContext, hits);
    }
  }

  public int getHitsToLog() {
    return hitsToLog;
  }
}
