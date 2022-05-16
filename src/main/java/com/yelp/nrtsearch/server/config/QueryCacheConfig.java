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
package com.yelp.nrtsearch.server.config;

import java.util.function.Predicate;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;

/** Class containing configuration for the global lucene query cache */
public class QueryCacheConfig {
  private static final String CONFIG_PREFIX = "queryCache.";
  static final int DEFAULT_MAX_QUERIES = 1000;
  static final String DEFAULT_MAX_MEMORY = "32MB";
  static final int DEFAULT_MIN_DOCS = 10000;
  static final float DEFAULT_MIN_SIZE_RATIO = 0.03f;
  static final float DEFAULT_SKIP_CACHE_FACTOR = 250.0f;

  private final boolean enabled;
  private final int maxQueries;
  private final long maxMemoryBytes;
  private final Predicate<LeafReaderContext> leafPredicate;
  private final float skipCacheFactor;

  /**
   * Create instance from provided configuration reader.
   *
   * @param configReader config reader
   * @return class instance
   */
  public static QueryCacheConfig fromConfig(YamlConfigReader configReader) {
    boolean enabled = configReader.getBoolean(CONFIG_PREFIX + "enabled", true);
    int maxQueries = configReader.getInteger(CONFIG_PREFIX + "maxQueries", DEFAULT_MAX_QUERIES);
    String maxMemory = configReader.getString(CONFIG_PREFIX + "maxMemory", DEFAULT_MAX_MEMORY);
    long maxMemoryBytes = sizeStrToBytes(maxMemory);
    int minDocs = configReader.getInteger(CONFIG_PREFIX + "minDocs", DEFAULT_MIN_DOCS);
    float minSizeRatio =
        configReader.getFloat(CONFIG_PREFIX + "minSizeRatio", DEFAULT_MIN_SIZE_RATIO);
    float skipCacheFactor =
        configReader.getFloat(CONFIG_PREFIX + "skipCacheFactor", DEFAULT_SKIP_CACHE_FACTOR);
    return new QueryCacheConfig(
        enabled, maxQueries, maxMemoryBytes, minDocs, minSizeRatio, skipCacheFactor);
  }

  /**
   * Convert a size string into an absolute number of bytes. The string may be specified with a
   * units label, or as a percentage of the heap. For example ('100mb', '50kb', '2gb', '10%').
   * Unlabeled values are assumed to be bytes.
   */
  static long sizeStrToBytes(String sizeStr) {
    if (sizeStr.endsWith("%")) {
      String percentStr = sizeStr.substring(0, sizeStr.length() - 1);
      if (percentStr.isEmpty()) {
        throw new IllegalArgumentException("Cannot convert empty percentage");
      }
      double percentValue = Double.parseDouble(percentStr);
      return (long) (Runtime.getRuntime().maxMemory() * (percentValue / 100.0));
    }
    String baseStr = sizeStr;
    long multiplier = 1;
    if (baseStr.length() > 2) {
      String suffix = baseStr.substring(baseStr.length() - 2).toLowerCase();
      if ("kb".equals(suffix)) {
        baseStr = baseStr.substring(0, baseStr.length() - 2);
        multiplier = 1024;
      } else if ("mb".equals(suffix)) {
        baseStr = baseStr.substring(0, baseStr.length() - 2);
        multiplier = 1024 * 1024;
      } else if ("gb".equals(suffix)) {
        baseStr = baseStr.substring(0, baseStr.length() - 2);
        multiplier = 1024 * 1024 * 1024;
      }
    }
    if (baseStr.isEmpty()) {
      throw new IllegalArgumentException("Cannot convert size string: " + sizeStr);
    }
    double baseValue = Double.parseDouble(baseStr);
    return (long) (baseValue * multiplier);
  }

  /**
   * Constructor.
   *
   * @param enabled toggle for enabling query cache
   * @param maxQueries max queries the cache will hold
   * @param maxMemoryBytes maximum cache memory size
   * @param minDocs min docs needed to consider caching for a segment
   * @param minSizeRatio min portion of index a segment must have to use caching
   * @param skipCacheFactor skip caching clauses this many times as expensive as top-level query
   */
  public QueryCacheConfig(
      boolean enabled,
      int maxQueries,
      long maxMemoryBytes,
      int minDocs,
      float minSizeRatio,
      float skipCacheFactor) {
    this.enabled = enabled;
    this.maxQueries = maxQueries;
    this.maxMemoryBytes = maxMemoryBytes;
    this.leafPredicate = new MinSegmentSizePredicate(minDocs, minSizeRatio);
    this.skipCacheFactor = skipCacheFactor;
  }

  /** Get if query cache is enabled. */
  public boolean getEnabled() {
    return enabled;
  }

  /** Get maximum queries to cache. */
  public int getMaxQueries() {
    return maxQueries;
  }

  /** Get maximum memory to use for cache. */
  public long getMaxMemoryBytes() {
    return maxMemoryBytes;
  }

  /** Get predicate to determine if caching should be used for a segment. */
  public Predicate<LeafReaderContext> getLeafPredicate() {
    return leafPredicate;
  }

  /** Get skip cache factor. */
  public float getSkipCacheFactor() {
    return skipCacheFactor;
  }

  /**
   * Predicate that accepts segments that have more that minSize docs, and contain at least
   * minSizeRatio portion of the index docs. This was copied out or {@link
   * org.apache.lucene.search.LRUQueryCache}, since it was package private.
   */
  static class MinSegmentSizePredicate implements Predicate<LeafReaderContext> {
    final int minSize;
    final float minSizeRatio;

    MinSegmentSizePredicate(int minSize, float minSizeRatio) {
      this.minSize = minSize;
      this.minSizeRatio = minSizeRatio;
    }

    @Override
    public boolean test(LeafReaderContext context) {
      final int maxDoc = context.reader().maxDoc();
      if (maxDoc < minSize) {
        return false;
      }
      final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
      final float sizeRatio = (float) context.reader().maxDoc() / topLevelContext.reader().maxDoc();
      return sizeRatio >= minSizeRatio;
    }
  }
}
