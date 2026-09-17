/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.handler.stream;

import com.yelp.nrtsearch.server.search.SearchContext;
import org.apache.lucene.search.TopDocs;

/**
 * What one stream carries from its ranking phase into its fetch phase, held in the session's {@link
 * io.grpc.Context} rather than in fields of the stream observer.
 *
 * <p>Only these few things need to be here: everything else the fetch phase wants is reachable from
 * the {@link SearchContext} — the index and shard state, the searcher, the response builder and its
 * diagnostics, and the fetch tasks.
 */
class SearchStreamState {
  SearchContext searchContext;
  TopDocs currentHits;

  /**
   * Hits to log from the request, kept because the context is built with an unbounded logging
   * limit, so {@link SearchContext#getHitsToLog()} no longer reports the configured value.
   */
  int requestHitsToLog;

  boolean metricsRecorded;
}
