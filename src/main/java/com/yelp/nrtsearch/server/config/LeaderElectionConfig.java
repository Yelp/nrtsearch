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
package com.yelp.nrtsearch.server.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Config for multi-primary leader election. When the type is {@code none} (the default), the server
 * behaves as a single primary cluster with no election.
 *
 * <pre>
 * leaderElection:
 *   type: s3_lease        # none, s3_lease, or a type registered by a PrimaryElectorPlugin
 *   claimConcurrency: 32  # parallelism when claiming index state on promotion
 *   refreshIntervalMs: 30000  # periodic refresh of remote global state, 0 to disable
 *   reloadWaitMs: 10000   # max time ReloadState waits for its refresh
 *   params:               # elector specific parameters
 *     ttlMs: 15000
 * </pre>
 */
public class LeaderElectionConfig {
  public static final String CONFIG_PREFIX = "leaderElection.";
  public static final String NONE = "none";
  public static final int DEFAULT_CLAIM_CONCURRENCY = 32;
  public static final long DEFAULT_REFRESH_INTERVAL_MS = 30_000;
  public static final long DEFAULT_RELOAD_WAIT_MS = 10_000;

  private final String type;
  private final int claimConcurrency;
  private final long refreshIntervalMs;
  private final long reloadWaitMs;
  private final Map<String, Object> params;

  public static LeaderElectionConfig fromConfig(YamlConfigReader configReader) {
    Objects.requireNonNull(configReader);
    String type = configReader.getString(CONFIG_PREFIX + "type", NONE);
    int claimConcurrency =
        configReader.getInteger(CONFIG_PREFIX + "claimConcurrency", DEFAULT_CLAIM_CONCURRENCY);
    long refreshIntervalMs =
        configReader.getLong(CONFIG_PREFIX + "refreshIntervalMs", DEFAULT_REFRESH_INTERVAL_MS);
    long reloadWaitMs =
        configReader.getLong(CONFIG_PREFIX + "reloadWaitMs", DEFAULT_RELOAD_WAIT_MS);
    Map<String, Object> params =
        configReader.get(CONFIG_PREFIX + "params", LeaderElectionConfig::readParams, Map.of());
    return new LeaderElectionConfig(
        type, claimConcurrency, refreshIntervalMs, reloadWaitMs, params);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> readParams(Object obj) {
    if (!(obj instanceof Map)) {
      throw new IllegalStateException("leaderElection.params must be a map");
    }
    Map<String, Object> result = new HashMap<>();
    ((Map<Object, Object>) obj).forEach((k, v) -> result.put(k.toString(), v));
    return result;
  }

  public LeaderElectionConfig(
      String type,
      int claimConcurrency,
      long refreshIntervalMs,
      long reloadWaitMs,
      Map<String, Object> params) {
    this.type = Objects.requireNonNull(type);
    if (claimConcurrency <= 0) {
      throw new IllegalArgumentException("claimConcurrency must be > 0");
    }
    if (refreshIntervalMs < 0) {
      throw new IllegalArgumentException("refreshIntervalMs must be >= 0");
    }
    this.claimConcurrency = claimConcurrency;
    this.refreshIntervalMs = refreshIntervalMs;
    if (reloadWaitMs <= 0) {
      throw new IllegalArgumentException("reloadWaitMs must be > 0");
    }
    this.reloadWaitMs = reloadWaitMs;
    this.params = Collections.unmodifiableMap(new HashMap<>(params));
  }

  /** Elector type name. */
  public String getType() {
    return type;
  }

  /** If leader election is enabled. */
  public boolean isEnabled() {
    return !NONE.equals(type);
  }

  public int getClaimConcurrency() {
    return claimConcurrency;
  }

  /**
   * Interval between periodic refreshes of remote global state, so standbys pick up created,
   * recreated, or removed indices; 0 disables periodic refresh (ReloadState still requests one).
   */
  public long getRefreshIntervalMs() {
    return refreshIntervalMs;
  }

  /**
   * How long ReloadState waits for the refresh it requests; on timeout it returns DEADLINE_EXCEEDED
   * and the refresh continues in the background.
   */
  public long getReloadWaitMs() {
    return reloadWaitMs;
  }

  /** Elector specific params. */
  public Map<String, Object> getParams() {
    return params;
  }
}
