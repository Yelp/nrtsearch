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

import static com.yelp.nrtsearch.server.config.ScriptCacheConfig.getTimeUnitFromString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.ByteArrayInputStream;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class ScriptCacheConfigTest {
  private static ScriptCacheConfig getConfig(String configFile) {
    return ScriptCacheConfig.fromConfig(
        new YamlConfigReader(new ByteArrayInputStream(configFile.getBytes())));
  }

  @Test
  public void testDefault() {
    String configFile = "nodeName: \"lucene_server_foo\"";
    ScriptCacheConfig config = getConfig(configFile);
    assertEquals(ScriptCacheConfig.DEFAULT_CONCURRENCY_LEVEL, config.getConcurrencyLevel());
    assertEquals(ScriptCacheConfig.DEFAULT_SIZE, config.getMaximumSize());
    assertEquals(ScriptCacheConfig.DEFAULT_EXPIRATION_TIME, config.getExpirationTime());
    assertEquals(TimeUnit.valueOf(ScriptCacheConfig.DEFAULT_TIME_UNIT), config.getTimeUnit());
  }

  @Test
  public void testValidConfig() {
    String configFile =
        String.join(
            "\n",
            "nodeName: \"lucene_server_foo\"",
            "ScriptCacheConfig:",
            "  concurrencyLevel: 2",
            "  maximumSize: 2000",
            "  expirationTime: 5000",
            "  timeUnit: \"MILLISECONDS\"");
    ScriptCacheConfig config = getConfig(configFile);
    assertEquals(2, config.getConcurrencyLevel());
    assertEquals(2000, config.getMaximumSize());
    assertEquals(5000, config.getExpirationTime());
    assertEquals(TimeUnit.MILLISECONDS, config.getTimeUnit());
  }

  @Test
  public void testInvalidTimeUnitConfig() {
    String configFile =
        String.join(
            "\n", "nodeName: \"lucene_server_foo\"", "ScriptCacheConfig:", "  timeUnit: \"ABCD\"");
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> getConfig(configFile));
    assertEquals(exception.getMessage(), "No enum constant java.util.concurrent.TimeUnit.ABCD");
  }

  @Test
  public void testNullTimeUnitConfig() {
    NullPointerException exception =
        assertThrows(NullPointerException.class, () -> getTimeUnitFromString(null));
    assertEquals(exception.getMessage(), "script cache expiration time unit cannot be null");
  }
}
