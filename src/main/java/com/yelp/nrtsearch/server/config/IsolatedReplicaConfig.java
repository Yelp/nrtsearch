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
  public static final String CONFIG_PREFIX = "isolatedReplicaConfig.";

  private final boolean enabled;
  private final int pollingIntervalSeconds;

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
    return new IsolatedReplicaConfig(enabled, pollingIntervalSeconds);
  }

  /**
   * Constructor.
   *
   * @param enabled if isolated replica is enabled
   * @param pollingIntervalSeconds interval in seconds to poll for new index versions, must be > 0
   */
  public IsolatedReplicaConfig(boolean enabled, int pollingIntervalSeconds) {
    this.enabled = enabled;
    this.pollingIntervalSeconds = pollingIntervalSeconds;
    if (pollingIntervalSeconds <= 0) {
      throw new IllegalArgumentException(
          String.format(
              "Polling interval seconds must be positive, got: %d", pollingIntervalSeconds));
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
}
