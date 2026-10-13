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
package com.yelp.nrtsearch.server.warming;

import com.yelp.nrtsearch.server.config.YamlConfigReader;

public class WarmerConfig {
  private static final String CONFIG_PREFIX = "warmer.";
  private static final int DEFAULT_MAX_WARMING_QUERIES = 0;
  private static final int DEFAULT_WARMING_PARALLELISM = 1;
  private static final int DEFAULT_WARM_BASIC_QUERY_ONLY_PERC = 0;
  private static final boolean DEFAULT_WARM_ON_STARTUP = false;
  private static final boolean DEFAULT_PAD_WITH_DOWNLOADED_REQUESTS = false;
  private static final boolean DEFAULT_FILTER_MATCH_ALL_QUERIES = false;
  private static final int DEFAULT_MAX_QUERY_TOP_HITS = 0;
  private static final int DEFAULT_MAX_QUERY_LATENCY_MS = 0;
  private static final int DEFAULT_QUERY_TIMEOUT_MS = 0;
  private static final int DEFAULT_MAX_WARMING_DURATION_MS = 0;

  private final int maxWarmingQueries;
  private final int warmingParallelism;
  private final int warmBasicQueryOnlyPerc;
  private final boolean warmOnStartup;
  private final boolean padWithDownloadedRequests;
  private final boolean filterMatchAllQueries;
  private final int maxQueryTopHits;
  private final int maxQueryLatencyMs;
  private final int queryTimeoutMs;
  private final int maxWarmingDurationMs;

  /**
   * Configuration for warmer.
   *
   * @param maxWarmingQueries maximum queries to store for warming
   * @param warmingParallelism number of parallel queries while warming on startup
   * @param warmBasicQueryOnlyPerc percentage of warming queries that should be basic queries for
   *     the fast boostrap
   * @param warmOnStartup if true will try to download queries from S3 and use them to warm
   * @param padWithDownloadedRequests if true, backup will be padded with the most recent downloaded
   *     queries when fewer than maxWarmingQueries have been observed
   */
  public WarmerConfig(
      int maxWarmingQueries,
      int warmingParallelism,
      int warmBasicQueryOnlyPerc,
      boolean warmOnStartup,
      boolean padWithDownloadedRequests) {
    this(
        maxWarmingQueries,
        warmingParallelism,
        warmBasicQueryOnlyPerc,
        warmOnStartup,
        padWithDownloadedRequests,
        DEFAULT_FILTER_MATCH_ALL_QUERIES,
        DEFAULT_MAX_QUERY_TOP_HITS,
        DEFAULT_MAX_QUERY_LATENCY_MS,
        DEFAULT_QUERY_TIMEOUT_MS,
        DEFAULT_MAX_WARMING_DURATION_MS);
  }

  /**
   * Configuration for warmer, including warming query guardrails.
   *
   * @param maxWarmingQueries maximum queries to store for warming
   * @param warmingParallelism number of parallel queries while warming on startup
   * @param warmBasicQueryOnlyPerc percentage of warming queries that should be basic queries for
   *     the fast boostrap
   * @param warmOnStartup if true will try to download queries from S3 and use them to warm
   * @param padWithDownloadedRequests if true, backup will be padded with the most recent downloaded
   *     queries when fewer than maxWarmingQueries have been observed
   * @param filterMatchAllQueries if true, match all queries are not collected or backed up
   * @param maxQueryTopHits queries with startHit + topHits above this are not collected or backed
   *     up, 0 to disable
   * @param maxQueryLatencyMs queries that took longer than this are not collected, 0 to disable
   * @param queryTimeoutMs timeout for each query replayed during warming, 0 to disable
   * @param maxWarmingDurationMs maximum total time to spend warming on startup, 0 to disable
   */
  public WarmerConfig(
      int maxWarmingQueries,
      int warmingParallelism,
      int warmBasicQueryOnlyPerc,
      boolean warmOnStartup,
      boolean padWithDownloadedRequests,
      boolean filterMatchAllQueries,
      int maxQueryTopHits,
      int maxQueryLatencyMs,
      int queryTimeoutMs,
      int maxWarmingDurationMs) {
    this.maxWarmingQueries = maxWarmingQueries;
    this.warmingParallelism = warmingParallelism;
    this.warmBasicQueryOnlyPerc = warmBasicQueryOnlyPerc;
    this.warmOnStartup = warmOnStartup;
    this.padWithDownloadedRequests = padWithDownloadedRequests;
    this.filterMatchAllQueries = filterMatchAllQueries;
    this.maxQueryTopHits = maxQueryTopHits;
    this.maxQueryLatencyMs = maxQueryLatencyMs;
    this.queryTimeoutMs = queryTimeoutMs;
    this.maxWarmingDurationMs = maxWarmingDurationMs;
  }

  public static WarmerConfig fromConfig(YamlConfigReader configReader) {
    int maxWarmingQueries =
        configReader.getInteger(CONFIG_PREFIX + "maxWarmingQueries", DEFAULT_MAX_WARMING_QUERIES);
    int warmingParallelism =
        configReader.getInteger(CONFIG_PREFIX + "warmingParallelism", DEFAULT_WARMING_PARALLELISM);
    int warmBasicQueryOnlyPerc =
        configReader.getInteger(
            CONFIG_PREFIX + "warmBasicQueryOnlyPerc", DEFAULT_WARM_BASIC_QUERY_ONLY_PERC);
    boolean warmOnStartup =
        configReader.getBoolean(CONFIG_PREFIX + "warmOnStartup", DEFAULT_WARM_ON_STARTUP);
    boolean padWithDownloadedRequests =
        configReader.getBoolean(
            CONFIG_PREFIX + "padWithDownloadedRequests", DEFAULT_PAD_WITH_DOWNLOADED_REQUESTS);
    boolean filterMatchAllQueries =
        configReader.getBoolean(
            CONFIG_PREFIX + "filterMatchAllQueries", DEFAULT_FILTER_MATCH_ALL_QUERIES);
    int maxQueryTopHits =
        configReader.getInteger(CONFIG_PREFIX + "maxQueryTopHits", DEFAULT_MAX_QUERY_TOP_HITS);
    int maxQueryLatencyMs =
        configReader.getInteger(CONFIG_PREFIX + "maxQueryLatencyMs", DEFAULT_MAX_QUERY_LATENCY_MS);
    int queryTimeoutMs =
        configReader.getInteger(CONFIG_PREFIX + "queryTimeoutMs", DEFAULT_QUERY_TIMEOUT_MS);
    int maxWarmingDurationMs =
        configReader.getInteger(
            CONFIG_PREFIX + "maxWarmingDurationMs", DEFAULT_MAX_WARMING_DURATION_MS);

    return new WarmerConfig(
        maxWarmingQueries,
        warmingParallelism,
        warmBasicQueryOnlyPerc,
        warmOnStartup,
        padWithDownloadedRequests,
        filterMatchAllQueries,
        maxQueryTopHits,
        maxQueryLatencyMs,
        queryTimeoutMs,
        maxWarmingDurationMs);
  }

  public int getMaxWarmingQueries() {
    return maxWarmingQueries;
  }

  public int getWarmingParallelism() {
    return warmingParallelism;
  }

  public int getWarmBasicQueryOnlyPerc() {
    return warmBasicQueryOnlyPerc;
  }

  public boolean isWarmOnStartup() {
    return warmOnStartup;
  }

  public boolean isPadWithDownloadedRequests() {
    return padWithDownloadedRequests;
  }

  public boolean isFilterMatchAllQueries() {
    return filterMatchAllQueries;
  }

  public int getMaxQueryTopHits() {
    return maxQueryTopHits;
  }

  public int getMaxQueryLatencyMs() {
    return maxQueryLatencyMs;
  }

  public int getQueryTimeoutMs() {
    return queryTimeoutMs;
  }

  public int getMaxWarmingDurationMs() {
    return maxWarmingDurationMs;
  }
}
