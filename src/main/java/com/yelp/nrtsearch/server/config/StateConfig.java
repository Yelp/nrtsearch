/*
 * Copyright 2022 Yelp Inc.
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

/** Configuration class for state managment. */
public class StateConfig {
  public static final String CONFIG_PREFIX = "stateConfig.";

  public enum StateBackendType {
    LOCAL,
    REMOTE
  }

  private final StateBackendType backendType;

  /**
   * Create instance from provided configuration reader.
   *
   * @param configReader config reader
   * @return class instance
   */
  public static StateConfig fromConfig(YamlConfigReader configReader) {
    Objects.requireNonNull(configReader);
    StateBackendType backendType =
        StateBackendType.valueOf(configReader.getString(CONFIG_PREFIX + "backendType", "LOCAL"));
    return new StateConfig(backendType);
  }

  /**
   * Constructor.
   *
   * @param backendType type of backend to used for storing/loading state
   */
  public StateConfig(StateBackendType backendType) {
    Objects.requireNonNull(backendType);
    this.backendType = backendType;
  }

  /** Get the type of backend that should be used for storing/loading server state. */
  public StateBackendType getBackendType() {
    return backendType;
  }
}
