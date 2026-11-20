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
  public void testPeerDefaults() {
    WarmerConfig config = getConfig(String.join("\n", "warmer:", "  warmingStrategy: peer"));
    assertEquals(WarmingStrategy.PEER, config.getWarmingStrategy());
    assertEquals(WarmingStrategy.S3, config.getPeerFallbackStrategy());
    assertEquals(100, config.getPeerMinQueries());
    assertEquals(10, config.getPeerMinUptimeMinutes());
    assertEquals(64L * 1024 * 1024, config.getPeerMaxBytes());
    assertEquals(30_000, config.getPeerTimeoutMs());
    assertEquals(3, config.getPeerMaxCandidates());
  }

  @Test
  public void testPeerSettings() {
    WarmerConfig config =
        getConfig(
            String.join(
                "\n",
                "warmer:",
                "  warmingStrategy: PEER",
                "  peerFallbackStrategy: schema",
                "  peerMinQueries: 5",
                "  peerMinUptimeMinutes: 1",
                "  peerMaxBytes: 1000",
                "  peerTimeoutMs: 200",
                "  peerMaxCandidates: 1"));
    assertEquals(WarmingStrategy.SCHEMA, config.getPeerFallbackStrategy());
    assertEquals(5, config.getPeerMinQueries());
    assertEquals(1, config.getPeerMinUptimeMinutes());
    assertEquals(1000, config.getPeerMaxBytes());
    assertEquals(200, config.getPeerTimeoutMs());
    assertEquals(1, config.getPeerMaxCandidates());
  }

  @Test
  public void testPeerFallbackNone() {
    WarmerConfig config =
        getConfig(
            String.join(
                "\n", "warmer:", "  warmingStrategy: peer", "  peerFallbackStrategy: none"));
    assertEquals(WarmingStrategy.NONE, config.getPeerFallbackStrategy());
  }

  @Test
  public void testInvalidPeerFallback() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            getConfig(
                String.join(
                    "\n", "warmer:", "  warmingStrategy: peer", "  peerFallbackStrategy: peer")));
  }

  @Test
  public void testTemplateDefaults() {
    WarmerConfig config = getConfig(String.join("\n", "warmer:", "  warmingStrategy: template"));
    assertEquals(WarmingStrategy.TEMPLATE, config.getWarmingStrategy());
    assertEquals(100, config.getTemplateSeedQueries());
  }

  @Test
  public void testTemplateSeedQueries() {
    WarmerConfig config =
        getConfig(
            String.join(
                "\n", "warmer:", "  warmingStrategy: TEMPLATE", "  templateSeedQueries: 50"));
    assertEquals(50, config.getTemplateSeedQueries());
  }

  @Test
  public void testTemplateLibraryDefaults() {
    WarmerConfig config = getConfig(String.join("\n", "warmer:", "  warmingStrategy: template"));
    WarmerConfig.TemplateLibrarySettings library = config.getTemplateLibrary();
    assertEquals(false, library.enabled());
    assertEquals(0.95, library.relearnMatchThreshold(), 0);
    assertEquals(100, library.driftCheckEvery());
    assertEquals(1000, library.driftWindow());
    assertEquals(360, library.relearnCooldownMinutes());
    assertEquals(0.5, library.decay(), 0);
    assertEquals(0.5, library.minWeight(), 0);
    assertEquals(100, library.relearnMinQueries());
  }

  @Test
  public void testTemplateLibrarySettings() {
    WarmerConfig config =
        getConfig(
            String.join(
                "\n",
                "warmer:",
                "  warmingStrategy: template",
                "  templateLibrary: true",
                "  templateRelearnMatchThreshold: 0.9",
                "  templateDriftCheckEvery: 10",
                "  templateDriftWindow: 500",
                "  templateRelearnCooldownMinutes: 60",
                "  templateLibraryDecay: 0.8",
                "  templateLibraryMinWeight: 1.5",
                "  templateRelearnMinQueries: 50"));
    WarmerConfig.TemplateLibrarySettings library = config.getTemplateLibrary();
    assertEquals(true, library.enabled());
    assertEquals(0.9, library.relearnMatchThreshold(), 0);
    assertEquals(10, library.driftCheckEvery());
    assertEquals(500, library.driftWindow());
    assertEquals(60, library.relearnCooldownMinutes());
    assertEquals(0.8, library.decay(), 0);
    assertEquals(1.5, library.minWeight(), 0);
    assertEquals(50, library.relearnMinQueries());
  }

  @Test
  public void testInvalidTemplateLibrarySettings() {
    for (String bad :
        new String[] {
          "  templateRelearnMatchThreshold: 0",
          "  templateRelearnMatchThreshold: 1.5",
          "  templateDriftCheckEvery: 0",
          "  templateDriftWindow: 0",
          "  templateLibraryDecay: 0",
          "  templateRelearnMinQueries: 0"
        }) {
      assertThrows(
          IllegalArgumentException.class,
          () -> getConfig(String.join("\n", "warmer:", "  warmingStrategy: template", bad)));
    }
  }

  @Test
  public void testInvalidTemplateSettings() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            getConfig(
                String.join(
                    "\n", "warmer:", "  warmingStrategy: template", "  templateSeedQueries: 0")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            getConfig(
                String.join(
                    "\n",
                    "warmer:",
                    "  warmingStrategy: peer",
                    "  peerFallbackStrategy: template")));
  }

  @Test
  public void testNoneStrategyInvalid() {
    assertThrows(
        IllegalArgumentException.class,
        () -> getConfig(String.join("\n", "warmer:", "  warmingStrategy: none")));
  }

  @Test
  public void testInvalidStrategy() {
    assertThrows(
        IllegalArgumentException.class,
        () -> getConfig(String.join("\n", "warmer:", "  warmingStrategy: invalid")));
  }
}
