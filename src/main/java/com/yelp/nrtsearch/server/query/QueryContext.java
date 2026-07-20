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
package com.yelp.nrtsearch.server.query;

import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.doc.SharedDocContext;
import com.yelp.nrtsearch.server.search.FetchTasks.FetchTask;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Context for query building. Bundles {@link DocLookup} (for field resolution and doc values) with
 * optional {@link GlobalState} (needed for cross-index queries like {@link
 * com.yelp.nrtsearch.server.grpc.CrossIndexQuery}) and optional {@link SharedDocContext} (for
 * scripts that access per-document shared values).
 *
 * <p>Also accumulates deferred {@link FetchTask}s created during query building (e.g. for
 * cross-index field retrieval). These should be drained via {@link #getDeferredFetchTasks()} and
 * registered with the search context's {@link com.yelp.nrtsearch.server.search.FetchTasks} before
 * the fetch phase runs.
 */
public final class QueryContext {
  private final DocLookup docLookup;
  @Nullable private final GlobalState globalState;
  @Nullable private final SharedDocContext sharedDocContext;
  private List<FetchTask> deferredFetchTasks;

  public QueryContext(
      DocLookup docLookup,
      @Nullable GlobalState globalState,
      @Nullable SharedDocContext sharedDocContext) {
    this.docLookup = docLookup;
    this.globalState = globalState;
    this.sharedDocContext = sharedDocContext;
  }

  public DocLookup docLookup() {
    return docLookup;
  }

  @Nullable
  public GlobalState globalState() {
    return globalState;
  }

  @Nullable
  public SharedDocContext sharedDocContext() {
    return sharedDocContext;
  }

  /** Register a fetch task to be executed during the fetch phase. */
  public void addDeferredFetchTask(FetchTask task) {
    if (deferredFetchTasks == null) {
      deferredFetchTasks = new ArrayList<>();
    }
    deferredFetchTasks.add(task);
  }

  /** Get all deferred fetch tasks accumulated during query building. */
  public List<FetchTask> getDeferredFetchTasks() {
    return deferredFetchTasks == null ? Collections.emptyList() : deferredFetchTasks;
  }
}
