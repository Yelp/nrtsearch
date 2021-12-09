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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Configuration class for script cache. */
public class ScriptCacheConfig {
  static final int DEFAULT_CONCURRENCY_LEVEL = 4;
  static final int DEFAULT_SIZE = 1000;
  static final long DEFAULT_EXPIRATION_TIME = 1;
  static final String DEFAULT_TIME_UNIT = "DAYS";

  private final int concurrencyLevel;
  private final int maximumSize;
  private final long expirationTime;
  private final TimeUnit timeUnit;

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
    long expirationTime =
        configReader.getLong("ScriptCacheConfig.expirationTime", DEFAULT_EXPIRATION_TIME);
    TimeUnit timeUnit =
        getTimeUnitFromString(
            configReader.getString("ScriptCacheConfig.timeUnit", DEFAULT_TIME_UNIT));
    return new ScriptCacheConfig(concurrencyLevel, maximumSize, expirationTime, timeUnit);
  }

  /**
   * @param timeUnitStr time unit string
   * @return expiration time unit from config
   * @throws IllegalArgumentException if timeUnitStr is invalid and NullPointerException if it is
   *     null
   */
  public static TimeUnit getTimeUnitFromString(String timeUnitStr) {
    TimeUnit timeUnit =
        TimeUnit.valueOf(
            Objects.requireNonNull(
                timeUnitStr, "script cache expiration time unit cannot be null"));
    return timeUnit;
  }

  /**
   * Constructor.
   *
   * @param concurrencyLevel cache concurrency level
   * @param maximumSize cache size
   * @param expirationTime cache expiration time
   * @param timeUnit cache expiration time unit
   */
  public ScriptCacheConfig(
      int concurrencyLevel, int maximumSize, long expirationTime, TimeUnit timeUnit) {
    this.concurrencyLevel = concurrencyLevel;
    this.maximumSize = maximumSize;
    this.expirationTime = expirationTime;
    this.timeUnit = timeUnit;
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
  public long getExpirationTime() {
    return expirationTime;
  }

  /** Get time unit for expiration. */
  public TimeUnit getTimeUnit() {
    return timeUnit;
  }
}
