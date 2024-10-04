/*
 * Copyright 2021 Yelp Inc.
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
package com.yelp.nrtsearch.server.rescore;

import com.yelp.nrtsearch.server.search.SearchContext;

/** Class containing additional information a {@link RescoreOperation} can use for rescoring. */
public class RescoreContext {
  private final int windowSize;
  private final SearchContext searchContext;

  /**
   * Constructor.
   *
   * @param windowSize rescore window size
   * @param searchContext search context
   */
  public RescoreContext(int windowSize, SearchContext searchContext) {
    this.windowSize = windowSize;
    this.searchContext = searchContext;
  }

  /** Get rescore window size. */
  public int getWindowSize() {
    return windowSize;
  }

  /** Get query search context. */
  public SearchContext getSearchContext() {
    return searchContext;
  }
}
