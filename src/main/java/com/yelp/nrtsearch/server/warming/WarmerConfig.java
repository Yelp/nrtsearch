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
  private static final String WARMING_QUERY_STRIPPING_CONFIG_PREFIX = "warmingQueriesStrippingPercs.";
  private static final int DEFAULT_MAX_WARMING_QUERIES = 0;
  private static final int DEFAULT_WARMING_PARALLELISM = 1;
  private static final boolean DEFAULT_WARM_ON_STARTUP = false;
  private static final int DEFAULT_MAX_STRIPPING_PERC = 0;

  private final int maxWarmingQueries;
  private final int warmingParallelism;
  private final boolean warmOnStartup;
  private final int maxRescorerStrippingPerc;
  private final int maxFunctionScoreScriptStrippingPerc;
  private final int maxVirtualFieldsStrippingPerc;
  private final int maxFacetsStripping;

  /**
   * Configuration for warmer.
   *
   * @param maxWarmingQueries maximum queries to store for warming
   * @param warmingParallelism number of parallel queries while warming on startup
   * @param warmOnStartup if true will try to download queries from S3 and use them to warm
   */
  public WarmerConfig(
          int maxWarmingQueries,
          int warmingParallelism,
          boolean warmOnStartup,
          int maxRescorerStrippingPerc,
          int maxFunctionScoreScriptStrippingPerc,
          int maxVirtualFieldsStrippingPerc,
          int maxFacetsStripping) {
    this.maxWarmingQueries = maxWarmingQueries;
    this.warmingParallelism = warmingParallelism;
    this.warmOnStartup = warmOnStartup;
    this.maxRescorerStrippingPerc = maxRescorerStrippingPerc;
    this.maxFunctionScoreScriptStrippingPerc = maxFunctionScoreScriptStrippingPerc;
    this.maxVirtualFieldsStrippingPerc = maxVirtualFieldsStrippingPerc;
    this.maxFacetsStripping = maxFacetsStripping;
  }

  public static WarmerConfig fromConfig(YamlConfigReader configReader) {
    int maxWarmingQueries =
        configReader.getInteger(CONFIG_PREFIX + "maxWarmingQueries", DEFAULT_MAX_WARMING_QUERIES);
    int warmingParallelism =
        configReader.getInteger(CONFIG_PREFIX + "warmingParallelism", DEFAULT_WARMING_PARALLELISM);
    boolean warmOnStartup =
        configReader.getBoolean(CONFIG_PREFIX + "warmOnStartup", DEFAULT_WARM_ON_STARTUP);

    int maxRescorerStrippingPerc =
            configReader.getInteger(CONFIG_PREFIX + WARMING_QUERY_STRIPPING_CONFIG_PREFIX + "maxRescorerStripping", DEFAULT_MAX_STRIPPING_PERC);
    int maxFunctionScoreScriptStrippingPerc =
            configReader.getInteger(CONFIG_PREFIX + WARMING_QUERY_STRIPPING_CONFIG_PREFIX + "maxFunctionScoreScriptStripping", DEFAULT_MAX_STRIPPING_PERC);
    int maxVirtualFieldsStrippingPerc =
            configReader.getInteger(CONFIG_PREFIX + WARMING_QUERY_STRIPPING_CONFIG_PREFIX + "maxVirtualFieldsStripping", DEFAULT_MAX_STRIPPING_PERC);
    int maxFacetsStripping =
            configReader.getInteger(CONFIG_PREFIX + WARMING_QUERY_STRIPPING_CONFIG_PREFIX + "maxFacetsStripping", DEFAULT_MAX_STRIPPING_PERC);

    return new WarmerConfig(
            maxWarmingQueries,
            warmingParallelism,
            warmOnStartup,
            maxRescorerStrippingPerc,
            maxFunctionScoreScriptStrippingPerc,
            maxVirtualFieldsStrippingPerc,
            maxFacetsStripping);
  }

  public int getMaxWarmingQueries() {
    return maxWarmingQueries;
  }

  public int getWarmingParallelism() {
    return warmingParallelism;
  }

  public boolean isWarmOnStartup() {
    return warmOnStartup;
  }

  public int getMaxRescorerStrippingPerc() { return maxRescorerStrippingPerc; }

  public int getMaxFunctionScoreScriptStrippingPerc() { return maxFunctionScoreScriptStrippingPerc; }

  public int getMaxVirtualFieldsStrippingPerc() { return maxVirtualFieldsStrippingPerc; }

  public int getMaxFacetsStripping() { return maxFacetsStripping; }
}
