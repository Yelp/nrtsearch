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
package com.yelp.nrtsearch.server.luceneserver.highlights;

import java.util.HashMap;
import java.util.Map;

/**
 * This is a shared context holder class for the {@link HighlightFetchTask}. Caches and other
 * contexts can be stored and passed between fields. All objects must be lazily initialized.
 *
 * <p>As we do not support multi-highlightFetchTask, we do not have the need to access to the
 * context between multiple tasks. Therefore, this class is set at a lower level, and won't be
 * accessible from the {@link com.yelp.nrtsearch.server.luceneserver.search.SearchContext}.</>
 */
public class SharedHighlightContext {
  private Map<String, Object> cache = null;

  public Map<String, Object> getCache() {
    if (cache == null) {
      cache = new HashMap<>();
    }
    return cache;
  }
}
