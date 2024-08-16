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

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import java.util.List;

/**
 * This is the hits logger interface to provide the new logging hits feature with different
 * implementations. A single HitsLogger is supposed to be initiated once at the startup time and
 * registered in the {@link HitsLoggerCreator}.
 */
public interface HitsLogger {
  /**
   * This will be invoked once as the last fetch task of the search request.
   *
   * @param context the {@link SearchContext} to keep the contexts for this search request
   * @param hits query hits
   */
  void log(SearchContext context, List<SearchResponse.Hit.Builder> hits);
}
