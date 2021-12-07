/*
 * Copyright 2021 Yelp Inc.
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

/** Configuration class for script cache. */
public class ScriptCacheConfig {
  static final int DEFAULT_CONCURRENCY_LEVEL = 4;
  static final int DEFAULT_SIZE = 1000;
  static final int DEFAULT_EXPIRATION_TIME = 1;

  private final int concurrencyLevel;
  private final int maximumSize;
  private final int expirationTime;

  /**
   * Create instance from provided configuration reader.
   *
   * @param configReader config reader
   * @return class instance
   */
  public static ScriptCacheConfig fromConfig(YamlConfigReader configReader) {
    int concurrencyLevel =
        configReader.getInteger("ScriptCacheConfig.concurrencyLevel", DEFAULT_CONCURRENCY_LEVEL);
    int maximumSize = configReader.getInteger("ScriptCacheConfig.maximumSize", DEFAULT_SIZE);
    int expirationTime =
        configReader.getInteger("ScriptCacheConfig.expirationTime", DEFAULT_EXPIRATION_TIME);
    return new ScriptCacheConfig(concurrencyLevel, maximumSize, expirationTime);
  }

  /**
   * Constructor.
   *
   * @param concurrencyLevel cache concurrency level
   * @param maximumSize cache size
   * @param expirationTime cache expiration time config
   */
  public ScriptCacheConfig(int concurrencyLevel, int maximumSize, int expirationTime) {
    this.concurrencyLevel = concurrencyLevel;
    this.maximumSize = maximumSize;
    this.expirationTime = expirationTime;
  }

  /** Get cache concurrency level. */
  public int getConcurrencyLevel() {
    return concurrencyLevel;
  }

  /** Get maximum size */
  public int getMaximumSize() {
    return maximumSize;
  }

  /** Get expiration time. */
  public int getExpirationTime() {
    return expirationTime;
  }
}
