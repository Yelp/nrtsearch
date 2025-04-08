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

  private final int maxWarmingQueries;
  private final int warmingParallelism;
  private final int warmBasicQueryOnlyPerc;
  private final boolean warmOnStartup;

  /**
   * Configuration for warmer.
   *
   * @param maxWarmingQueries maximum queries to store for warming
   * @param warmingParallelism number of parallel queries while warming on startup
   * @param warmBasicQueryOnlyPerc percentage of warming queries that should be basic queries for
   *     the fast boostrap
   * @param warmOnStartup if true will try to download queries from S3 and use them to warm
   */
  public WarmerConfig(
      int maxWarmingQueries,
      int warmingParallelism,
      int warmBasicQueryOnlyPerc,
      boolean warmOnStartup) {
    this.maxWarmingQueries = maxWarmingQueries;
    this.warmingParallelism = warmingParallelism;
    this.warmBasicQueryOnlyPerc = warmBasicQueryOnlyPerc;
    this.warmOnStartup = warmOnStartup;
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

    return new WarmerConfig(
        maxWarmingQueries, warmingParallelism, warmBasicQueryOnlyPerc, warmOnStartup);
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
}
