/*
 * Copyright 2025 Yelp Inc.
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

import java.util.Objects;

/** Config class for the isolated replica feature that decouples replicas from the primary. */
public class IsolatedReplicaConfig {
  private static final int MAX_FRESHNESS_TARGET_SECONDS = 60 * 60 * 24; // 1 day
  public static final String CONFIG_PREFIX = "isolatedReplicaConfig.";

  private final boolean enabled;
  private final int pollingIntervalSeconds;
  private final int freshnessTargetSeconds;
  private final int freshnessTargetOffsetSeconds;

  /**
   * Create instance from provided configuration reader.
   *
   * @param configReader config reader
   * @return class instance
   */
  public static IsolatedReplicaConfig fromConfig(YamlConfigReader configReader) {
    Objects.requireNonNull(configReader);
    boolean enabled = configReader.getBoolean(CONFIG_PREFIX + "enabled", false);
    int pollingIntervalSeconds =
        configReader.getInteger(CONFIG_PREFIX + "pollingIntervalSeconds", 120);
    int freshnessTargetSeconds =
        configReader.getInteger(CONFIG_PREFIX + "freshnessTargetSeconds", 0);
    int freshnessTargetOffsetSeconds =
        configReader.getInteger(CONFIG_PREFIX + "freshnessTargetOffsetSeconds", 0);
    return new IsolatedReplicaConfig(
        enabled, pollingIntervalSeconds, freshnessTargetSeconds, freshnessTargetOffsetSeconds);
  }

  /**
   * Constructor.
   *
   * @param enabled if isolated replica is enabled
   * @param pollingIntervalSeconds interval in seconds to poll for new index versions, must be > 0
   * @param freshnessTargetSeconds target for how fresh the replica index data should be in seconds,
   *     must be >= 0 and <= 1 day, 0 means as fresh as possible
   * @param freshnessTargetOffsetSeconds offset to apply to the freshness target in seconds, must be
   *     >= 0 and <= 1 day
   * @throws IllegalArgumentException if pollingIntervalSeconds is not positive or
   *     freshnessTargetSeconds is negative or too large
   * @throws IllegalArgumentException if freshnessTargetOffsetSeconds is negative or too large
   */
  public IsolatedReplicaConfig(
      boolean enabled,
      int pollingIntervalSeconds,
      int freshnessTargetSeconds,
      int freshnessTargetOffsetSeconds) {
    this.enabled = enabled;
    this.pollingIntervalSeconds = pollingIntervalSeconds;
    if (pollingIntervalSeconds <= 0) {
      throw new IllegalArgumentException(
          String.format(
              "Polling interval seconds must be positive, got: %d", pollingIntervalSeconds));
    }
    this.freshnessTargetSeconds = freshnessTargetSeconds;
    if (freshnessTargetSeconds < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Freshness target seconds must be non-negative, got: %d", freshnessTargetSeconds));
    } else if (freshnessTargetSeconds > MAX_FRESHNESS_TARGET_SECONDS) {
      throw new IllegalArgumentException(
          String.format(
              "Freshness target seconds must be <= %d, got: %d",
              MAX_FRESHNESS_TARGET_SECONDS, freshnessTargetSeconds));
    }
    this.freshnessTargetOffsetSeconds = freshnessTargetOffsetSeconds;
    if (freshnessTargetOffsetSeconds < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Freshness target offset seconds must be non-negative, got: %d",
              freshnessTargetOffsetSeconds));
    } else if (freshnessTargetOffsetSeconds > MAX_FRESHNESS_TARGET_SECONDS) {
      throw new IllegalArgumentException(
          String.format(
              "Freshness target offset seconds must be <= %d, got: %d",
              MAX_FRESHNESS_TARGET_SECONDS, freshnessTargetOffsetSeconds));
    }
  }

  /**
   * @return if isolated replica is enabled
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * @return interval in seconds to poll for new index versions
   */
  public int getPollingIntervalSeconds() {
    return pollingIntervalSeconds;
  }

  /**
   * @return target for how fresh the replica index should be in seconds, 0 means as fresh as
   *     possible
   */
  public int getFreshnessTargetSeconds() {
    return freshnessTargetSeconds;
  }

  /**
   * @return offset to apply to the freshness target in seconds
   */
  public int getFreshnessTargetOffsetSeconds() {
    return freshnessTargetOffsetSeconds;
  }
}
