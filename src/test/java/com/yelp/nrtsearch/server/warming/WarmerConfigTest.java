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
package com.yelp.nrtsearch.server.warming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.yelp.nrtsearch.server.config.YamlConfigReader;
import com.yelp.nrtsearch.server.warming.WarmerConfig.WarmingStrategy;
import java.io.ByteArrayInputStream;
import org.junit.Test;

public class WarmerConfigTest {

  private static WarmerConfig getConfig(String configStr) {
    return WarmerConfig.fromConfig(
        new YamlConfigReader(new ByteArrayInputStream(configStr.getBytes())));
  }

  @Test
  public void testDefaultStrategy() {
    WarmerConfig config = getConfig("nodeName: \"server_foo\"");
    assertEquals(WarmingStrategy.S3, config.getWarmingStrategy());
    assertEquals(1000, config.getSchemaWarmingQueries());
  }

  @Test
  public void testSchemaWarmingQueriesDefaultsToMaxWarmingQueries() {
    WarmerConfig config =
        getConfig(
            String.join("\n", "warmer:", "  warmingStrategy: schema", "  maxWarmingQueries: 5000"));
    assertEquals(5000, config.getSchemaWarmingQueries());
  }

  @Test
  public void testSchemaStrategy() {
    WarmerConfig config =
        getConfig(
            String.join(
                "\n", "warmer:", "  warmingStrategy: schema", "  schemaWarmingQueries: 500"));
    assertEquals(WarmingStrategy.SCHEMA, config.getWarmingStrategy());
    assertEquals(500, config.getSchemaWarmingQueries());
  }

  @Test
  public void testInvalidStrategy() {
    assertThrows(
        IllegalArgumentException.class,
        () -> getConfig(String.join("\n", "warmer:", "  warmingStrategy: invalid")));
  }
}
