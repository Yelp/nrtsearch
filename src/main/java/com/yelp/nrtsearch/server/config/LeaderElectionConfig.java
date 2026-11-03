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
 *   params:               # elector specific parameters
 *     ttlMs: 15000
 * </pre>
 */
public class LeaderElectionConfig {
  public static final String CONFIG_PREFIX = "leaderElection.";
  public static final String NONE = "none";
  public static final int DEFAULT_CLAIM_CONCURRENCY = 32;

  private final String type;
  private final int claimConcurrency;
  private final Map<String, Object> params;

  public static LeaderElectionConfig fromConfig(YamlConfigReader configReader) {
    Objects.requireNonNull(configReader);
    String type = configReader.getString(CONFIG_PREFIX + "type", NONE);
    int claimConcurrency =
        configReader.getInteger(CONFIG_PREFIX + "claimConcurrency", DEFAULT_CLAIM_CONCURRENCY);
    Map<String, Object> params =
        configReader.get(CONFIG_PREFIX + "params", LeaderElectionConfig::readParams, Map.of());
    return new LeaderElectionConfig(type, claimConcurrency, params);
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

  public LeaderElectionConfig(String type, int claimConcurrency, Map<String, Object> params) {
    this.type = Objects.requireNonNull(type);
    if (claimConcurrency <= 0) {
      throw new IllegalArgumentException("claimConcurrency must be > 0");
    }
    this.claimConcurrency = claimConcurrency;
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

  /** Elector specific params. */
  public Map<String, Object> getParams() {
    return params;
  }
}
