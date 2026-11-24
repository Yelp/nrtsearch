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
  /** Source of the queries replayed when warming on startup. */
  public enum WarmingStrategy {
    /** Replay the query corpus backed up to S3 by backupWarmingQueries. */
    S3,
    /** Generate queries from the index schema and values, without downloading from S3. */
    SCHEMA,
    /**
     * Replay the warming corpus of a running sibling replica, fetched over the replication port.
     * Falls back to {@link WarmerConfig#getPeerFallbackStrategy()} if no peer qualifies.
     */
    PEER,
    /**
     * Generate queries from the templates of a few real queries, with values from the documents
     * they return. Seeds come from a running peer, or the S3 corpus. Falls back to SCHEMA.
     */
    TEMPLATE,
    /** Do not warm. Only valid as the PEER fallback strategy. */
    NONE
  }

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
  private static final int DEFAULT_MAX_VECTOR_CANDIDATES = 0;
  private static final WarmingStrategy DEFAULT_WARMING_STRATEGY = WarmingStrategy.S3;
  private static final int DEFAULT_SCHEMA_WARMING_QUERIES = 1000;
  private static final WarmingStrategy DEFAULT_PEER_FALLBACK_STRATEGY = WarmingStrategy.S3;
  private static final int DEFAULT_PEER_MIN_QUERIES = 100;
  private static final int DEFAULT_PEER_MIN_UPTIME_MINUTES = 10;
  private static final long DEFAULT_PEER_MAX_BYTES = 64L * 1024 * 1024;
  private static final int DEFAULT_PEER_TIMEOUT_MS = 30_000;
  private static final int DEFAULT_PEER_MAX_CANDIDATES = 3;
  private static final int DEFAULT_TEMPLATE_SEED_QUERIES = 100;
  private static final int DEFAULT_TEMPLATE_SCHEMA_QUERIES = 0;

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
  private final int maxVectorCandidates;
  private final WarmingStrategy warmingStrategy;
  private final int schemaWarmingQueries;
  private final WarmingStrategy peerFallbackStrategy;
  private final int peerMinQueries;
  private final int peerMinUptimeMinutes;
  private final long peerMaxBytes;
  private final int peerTimeoutMs;
  private final int peerMaxCandidates;
  private final int templateSeedQueries;
  private TemplateLibrarySettings templateLibrary = TemplateLibrarySettings.DISABLED;
  private int templateSchemaQueries = DEFAULT_TEMPLATE_SCHEMA_QUERIES;
  private SchemaQuerySettings schemaQuerySettings = SchemaQuerySettings.DEFAULT;

  /**
   * Template library settings of the TEMPLATE strategy.
   *
   * @param enabled store the learned templates in the remote backend, load them on startup instead
   *     of learning from seed queries, and relearn when live queries stop matching them
   * @param relearnMatchThreshold relearn when the share of checked live queries matching a template
   *     falls below this
   * @param driftCheckEvery check one in every this many collected live queries
   * @param driftWindow number of checks the match share is computed over
   * @param relearnCooldownMinutes minimum time between two relearns started by this replica
   * @param decay weight multiplier of existing templates at each relearn, so templates that stop
   *     occurring fade out
   * @param minWeight templates whose weight falls below this are dropped
   * @param relearnMinQueries minimum collected queries to relearn from
   */
  public record TemplateLibrarySettings(
      boolean enabled,
      double relearnMatchThreshold,
      int driftCheckEvery,
      int driftWindow,
      int relearnCooldownMinutes,
      double decay,
      double minWeight,
      int relearnMinQueries) {
    public static final TemplateLibrarySettings DISABLED =
        new TemplateLibrarySettings(false, 0.95, 100, 1000, 360, 0.5, 0.5, 100);

    public TemplateLibrarySettings {
      if (relearnMatchThreshold <= 0 || relearnMatchThreshold > 1) {
        throw new IllegalArgumentException("templateRelearnMatchThreshold must be in (0, 1]");
      }
      if (driftCheckEvery <= 0 || driftWindow <= 0 || relearnMinQueries <= 0) {
        throw new IllegalArgumentException(
            "templateDriftCheckEvery, templateDriftWindow and templateRelearnMinQueries must be"
                + " positive");
      }
      if (relearnCooldownMinutes < 0 || decay <= 0 || decay > 1 || minWeight < 0) {
        throw new IllegalArgumentException(
            "templateRelearnCooldownMinutes must be >= 0, templateLibraryDecay in (0, 1] and"
                + " templateLibraryMinWeight >= 0");
      }
    }
  }

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
        DEFAULT_MAX_WARMING_DURATION_MS,
        DEFAULT_MAX_VECTOR_CANDIDATES,
        DEFAULT_WARMING_STRATEGY,
        DEFAULT_SCHEMA_WARMING_QUERIES);
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
   * @param filterMatchAllQueries if true, match all queries are not collected, backed up or
   *     replayed
   * @param maxQueryTopHits queries with topHits or startHit above this are not collected, backed up
   *     or replayed, 0 to disable
   * @param maxQueryLatencyMs queries that took longer than this are not collected, 0 to disable
   * @param queryTimeoutMs timeout for each query replayed during warming, 0 to disable
   * @param maxWarmingDurationMs maximum total time to spend warming on startup, 0 to disable
   * @param maxVectorCandidates queries with a knn query that has numCandidates above this are not
   *     collected, backed up or replayed, 0 to disable
   * @param warmingStrategy source of the queries replayed when warming on startup
   * @param schemaWarmingQueries number of term and range queries to generate with the SCHEMA
   *     strategy, defaults to maxWarmingQueries (or 1000 when that is 0)
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
      int maxWarmingDurationMs,
      int maxVectorCandidates,
      WarmingStrategy warmingStrategy,
      int schemaWarmingQueries) {
    this(
        maxWarmingQueries,
        warmingParallelism,
        warmBasicQueryOnlyPerc,
        warmOnStartup,
        padWithDownloadedRequests,
        filterMatchAllQueries,
        maxQueryTopHits,
        maxQueryLatencyMs,
        queryTimeoutMs,
        maxWarmingDurationMs,
        maxVectorCandidates,
        warmingStrategy,
        schemaWarmingQueries,
        DEFAULT_PEER_FALLBACK_STRATEGY,
        DEFAULT_PEER_MIN_QUERIES,
        DEFAULT_PEER_MIN_UPTIME_MINUTES,
        DEFAULT_PEER_MAX_BYTES,
        DEFAULT_PEER_TIMEOUT_MS,
        DEFAULT_PEER_MAX_CANDIDATES);
  }

  /**
   * Configuration for warmer, including the PEER strategy settings. See the constructor above for
   * the other parameters.
   *
   * @param peerFallbackStrategy strategy used when no peer qualifies: S3, SCHEMA or NONE
   * @param peerMinQueries a peer must have sampled at least this many queries, 0 to disable
   * @param peerMinUptimeMinutes a peer must have been up at least this long, 0 to disable
   * @param peerMaxBytes maximum bytes of queries to fetch from a peer, 0 for no limit
   * @param peerTimeoutMs deadline for fetching from one peer, 0 for none
   * @param peerMaxCandidates maximum number of peers to try
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
      int maxWarmingDurationMs,
      int maxVectorCandidates,
      WarmingStrategy warmingStrategy,
      int schemaWarmingQueries,
      WarmingStrategy peerFallbackStrategy,
      int peerMinQueries,
      int peerMinUptimeMinutes,
      long peerMaxBytes,
      int peerTimeoutMs,
      int peerMaxCandidates) {
    this(
        maxWarmingQueries,
        warmingParallelism,
        warmBasicQueryOnlyPerc,
        warmOnStartup,
        padWithDownloadedRequests,
        filterMatchAllQueries,
        maxQueryTopHits,
        maxQueryLatencyMs,
        queryTimeoutMs,
        maxWarmingDurationMs,
        maxVectorCandidates,
        warmingStrategy,
        schemaWarmingQueries,
        peerFallbackStrategy,
        peerMinQueries,
        peerMinUptimeMinutes,
        peerMaxBytes,
        peerTimeoutMs,
        peerMaxCandidates,
        DEFAULT_TEMPLATE_SEED_QUERIES);
  }

  /**
   * Configuration for warmer, including the TEMPLATE strategy settings. See the constructors above
   * for the other parameters.
   *
   * @param templateSeedQueries number of real queries the TEMPLATE strategy learns templates from
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
      int maxWarmingDurationMs,
      int maxVectorCandidates,
      WarmingStrategy warmingStrategy,
      int schemaWarmingQueries,
      WarmingStrategy peerFallbackStrategy,
      int peerMinQueries,
      int peerMinUptimeMinutes,
      long peerMaxBytes,
      int peerTimeoutMs,
      int peerMaxCandidates,
      int templateSeedQueries) {
    if (warmingStrategy == WarmingStrategy.NONE) {
      throw new IllegalArgumentException(
          "warmingStrategy NONE is only valid as peerFallbackStrategy, disable warmOnStartup instead");
    }
    if (peerFallbackStrategy == WarmingStrategy.PEER
        || peerFallbackStrategy == WarmingStrategy.TEMPLATE) {
      throw new IllegalArgumentException("peerFallbackStrategy cannot be " + peerFallbackStrategy);
    }
    if (templateSeedQueries <= 0) {
      throw new IllegalArgumentException("templateSeedQueries must be positive");
    }
    this.templateSeedQueries = templateSeedQueries;
    this.peerFallbackStrategy = peerFallbackStrategy;
    this.peerMinQueries = peerMinQueries;
    this.peerMinUptimeMinutes = peerMinUptimeMinutes;
    this.peerMaxBytes = peerMaxBytes;
    this.peerTimeoutMs = peerTimeoutMs;
    this.peerMaxCandidates = peerMaxCandidates;
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
    this.maxVectorCandidates = maxVectorCandidates;
    this.warmingStrategy = warmingStrategy;
    this.schemaWarmingQueries = schemaWarmingQueries;
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
    int maxVectorCandidates =
        configReader.getInteger(
            CONFIG_PREFIX + "maxVectorCandidates", DEFAULT_MAX_VECTOR_CANDIDATES);
    WarmingStrategy warmingStrategy =
        WarmingStrategy.valueOf(
            configReader
                .getString(CONFIG_PREFIX + "warmingStrategy", DEFAULT_WARMING_STRATEGY.name())
                .toUpperCase());
    // by default generate as many queries as a stored corpus would replay
    int schemaWarmingQueries =
        configReader.getInteger(
            CONFIG_PREFIX + "schemaWarmingQueries",
            maxWarmingQueries > 0 ? maxWarmingQueries : DEFAULT_SCHEMA_WARMING_QUERIES);
    WarmingStrategy peerFallbackStrategy =
        WarmingStrategy.valueOf(
            configReader
                .getString(
                    CONFIG_PREFIX + "peerFallbackStrategy", DEFAULT_PEER_FALLBACK_STRATEGY.name())
                .toUpperCase());
    int peerMinQueries =
        configReader.getInteger(CONFIG_PREFIX + "peerMinQueries", DEFAULT_PEER_MIN_QUERIES);
    int peerMinUptimeMinutes =
        configReader.getInteger(
            CONFIG_PREFIX + "peerMinUptimeMinutes", DEFAULT_PEER_MIN_UPTIME_MINUTES);
    long peerMaxBytes =
        configReader.getLong(CONFIG_PREFIX + "peerMaxBytes", DEFAULT_PEER_MAX_BYTES);
    int peerTimeoutMs =
        configReader.getInteger(CONFIG_PREFIX + "peerTimeoutMs", DEFAULT_PEER_TIMEOUT_MS);
    int peerMaxCandidates =
        configReader.getInteger(CONFIG_PREFIX + "peerMaxCandidates", DEFAULT_PEER_MAX_CANDIDATES);
    int templateSeedQueries =
        configReader.getInteger(
            CONFIG_PREFIX + "templateSeedQueries", DEFAULT_TEMPLATE_SEED_QUERIES);
    TemplateLibrarySettings d = TemplateLibrarySettings.DISABLED;
    TemplateLibrarySettings templateLibrary =
        new TemplateLibrarySettings(
            configReader.getBoolean(CONFIG_PREFIX + "templateLibrary", false),
            configReader.getDouble(
                CONFIG_PREFIX + "templateRelearnMatchThreshold", d.relearnMatchThreshold()),
            configReader.getInteger(CONFIG_PREFIX + "templateDriftCheckEvery", d.driftCheckEvery()),
            configReader.getInteger(CONFIG_PREFIX + "templateDriftWindow", d.driftWindow()),
            configReader.getInteger(
                CONFIG_PREFIX + "templateRelearnCooldownMinutes", d.relearnCooldownMinutes()),
            configReader.getDouble(CONFIG_PREFIX + "templateLibraryDecay", d.decay()),
            configReader.getDouble(CONFIG_PREFIX + "templateLibraryMinWeight", d.minWeight()),
            configReader.getInteger(
                CONFIG_PREFIX + "templateRelearnMinQueries", d.relearnMinQueries()));

    WarmerConfig config =
        new WarmerConfig(
            maxWarmingQueries,
            warmingParallelism,
            warmBasicQueryOnlyPerc,
            warmOnStartup,
            padWithDownloadedRequests,
            filterMatchAllQueries,
            maxQueryTopHits,
            maxQueryLatencyMs,
            queryTimeoutMs,
            maxWarmingDurationMs,
            maxVectorCandidates,
            warmingStrategy,
            schemaWarmingQueries,
            peerFallbackStrategy,
            peerMinQueries,
            peerMinUptimeMinutes,
            peerMaxBytes,
            peerTimeoutMs,
            peerMaxCandidates,
            templateSeedQueries);
    config.templateLibrary = templateLibrary;
    config.setTemplateSchemaQueries(
        configReader.getInteger(
            CONFIG_PREFIX + "templateSchemaQueries", DEFAULT_TEMPLATE_SCHEMA_QUERIES));
    SchemaWarmingQueryGenerator.Limits l = SchemaWarmingQueryGenerator.Limits.DEFAULT;
    config.schemaQuerySettings =
        new SchemaQuerySettings(
            configReader.getInteger(CONFIG_PREFIX + "schemaWarmingParallelism", 0),
            configReader.getInteger(CONFIG_PREFIX + "schemaQueryTimeoutMs", 0),
            new SchemaWarmingQueryGenerator.Limits(
                configReader.getInteger(CONFIG_PREFIX + "schemaTerminateAfter", l.terminateAfter()),
                configReader.getInteger(
                    CONFIG_PREFIX + "schemaMaxRetrieveFields", l.maxRetrieveFields()),
                configReader.getInteger(
                    CONFIG_PREFIX + "schemaMaxQueriesPerTarget", l.maxQueriesPerTarget()),
                configReader.getDouble(
                    CONFIG_PREFIX + "schemaSelectiveFraction", l.selectiveFraction())));
    return config;
  }

  /**
   * How schema generated queries are run, by the SCHEMA strategy and after template queries.
   *
   * @param parallelism queries run in parallel, 0 to use warmingParallelism. The replica serves no
   *     traffic while warming, so this can be higher than warmingParallelism, up to the CPUs.
   * @param queryTimeoutMs timeout of each schema query, 0 to use queryTimeoutMs
   * @param limits limits on the cost of each generated query
   */
  public record SchemaQuerySettings(
      int parallelism, int queryTimeoutMs, SchemaWarmingQueryGenerator.Limits limits) {
    public static final SchemaQuerySettings DEFAULT =
        new SchemaQuerySettings(0, 0, SchemaWarmingQueryGenerator.Limits.DEFAULT);

    public SchemaQuerySettings {
      if (parallelism < 0 || queryTimeoutMs < 0) {
        throw new IllegalArgumentException(
            "schemaWarmingParallelism and schemaQueryTimeoutMs must be >= 0");
      }
    }
  }

  public SchemaQuerySettings getSchemaQuerySettings() {
    return schemaQuerySettings;
  }

  /** Set the schema query settings, for configs built with a constructor. */
  public WarmerConfig withSchemaQuerySettings(SchemaQuerySettings schemaQuerySettings) {
    this.schemaQuerySettings = schemaQuerySettings;
    return this;
  }

  public int getTemplateSeedQueries() {
    return templateSeedQueries;
  }

  /**
   * Number of schema generated queries the TEMPLATE strategy replays after its template queries, 0
   * to replay only template queries.
   */
  public int getTemplateSchemaQueries() {
    return templateSchemaQueries;
  }

  /**
   * Set the schema queries replayed after template queries, for configs built with a constructor.
   */
  public WarmerConfig withTemplateSchemaQueries(int templateSchemaQueries) {
    setTemplateSchemaQueries(templateSchemaQueries);
    return this;
  }

  private void setTemplateSchemaQueries(int templateSchemaQueries) {
    if (templateSchemaQueries < 0) {
      throw new IllegalArgumentException("templateSchemaQueries must be >= 0");
    }
    this.templateSchemaQueries = templateSchemaQueries;
  }

  public TemplateLibrarySettings getTemplateLibrary() {
    return templateLibrary;
  }

  /** Set the template library settings, for configs built with a constructor. */
  public WarmerConfig withTemplateLibrary(TemplateLibrarySettings templateLibrary) {
    this.templateLibrary = templateLibrary;
    return this;
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

  public int getMaxVectorCandidates() {
    return maxVectorCandidates;
  }

  public WarmingStrategy getWarmingStrategy() {
    return warmingStrategy;
  }

  public int getSchemaWarmingQueries() {
    return schemaWarmingQueries;
  }

  public WarmingStrategy getPeerFallbackStrategy() {
    return peerFallbackStrategy;
  }

  public int getPeerMinQueries() {
    return peerMinQueries;
  }

  public int getPeerMinUptimeMinutes() {
    return peerMinUptimeMinutes;
  }

  public long getPeerMaxBytes() {
    return peerMaxBytes;
  }

  public int getPeerTimeoutMs() {
    return peerTimeoutMs;
  }

  public int getPeerMaxCandidates() {
    return peerMaxCandidates;
  }
}
