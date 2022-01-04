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
package com.yelp.nrtsearch.server.luceneserver.search.cache;

import java.util.function.Predicate;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.Query;

/**
 * Query cache implementation that extends the default {@link LRUQueryCache} and exposes additional
 * metrics.
 */
public class NrtQueryCache extends LRUQueryCache {

  // these variables are volatile so that we do not need to sync reads
  // but increments need to be performed under the lock
  private volatile long cacheQueryCount;
  private volatile long cacheQuerySize;

  public NrtQueryCache(
      int maxSize,
      long maxRamBytesUsed,
      Predicate<LeafReaderContext> leavesToCache,
      float skipCacheFactor) {
    super(maxSize, maxRamBytesUsed, leavesToCache, skipCacheFactor);
  }

  public NrtQueryCache(int maxSize, long maxRamBytesUsed) {
    super(maxSize, maxRamBytesUsed);
  }

  /** Get count of all queries added to cache. */
  public long getCacheQueryCount() {
    return cacheQueryCount;
  }

  /** Get current number of queries in the cache. */
  public long getCacheQuerySize() {
    return cacheQuerySize;
  }

  @Override
  protected void onQueryCache(Query query, long ramBytesUsed) {
    // super method asserts thread holds cache lock
    super.onQueryCache(query, ramBytesUsed);
    cacheQueryCount += 1;
    cacheQuerySize += 1;
  }

  @Override
  protected void onQueryEviction(Query query, long ramBytesUsed) {
    // super method asserts thread holds cache lock
    super.onQueryEviction(query, ramBytesUsed);
    cacheQuerySize -= 1;
  }
}
