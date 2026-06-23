/*
 * Copyright 2020 Yelp Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.Set;
import org.junit.Test;

public class PageCacheEvictionConfigTest {

  private static PageCacheEvictionConfig getConfig(String yaml) {
    return PageCacheEvictionConfig.fromConfig(
        new YamlConfigReader(new ByteArrayInputStream(yaml.getBytes())));
  }

  @Test
  public void testDefault() {
    PageCacheEvictionConfig config = getConfig("nodeName: server_foo");
    assertFalse(config.isEnabled());
    assertEquals(Collections.emptySet(), config.getExtensions());
  }

  @Test
  public void testEnabledNoExtensions() {
    PageCacheEvictionConfig config = getConfig("evictOnWrite:\n  enabled: true");
    assertTrue(config.isEnabled());
    assertEquals(Collections.emptySet(), config.getExtensions());
  }

  @Test
  public void testEnabledWithExtensions() {
    String yaml =
        String.join(
            "\n", "evictOnWrite:", "  enabled: true", "  extensions:", "    - vec", "    - pos");
    PageCacheEvictionConfig config = getConfig(yaml);
    assertTrue(config.isEnabled());
    assertEquals(Set.of("vec", "pos"), config.getExtensions());
  }

  @Test
  public void testEnabledWithWildcard() {
    String yaml =
        String.join("\n", "evictOnWrite:", "  enabled: true", "  extensions:", "    - \"*\"");
    PageCacheEvictionConfig config = getConfig(yaml);
    assertTrue(config.isEnabled());
    assertEquals(
        Collections.singleton(PageCacheEvictionConfig.ALL_EXTENSIONS), config.getExtensions());
  }

  @Test
  public void testShouldEvict_disabled() {
    PageCacheEvictionConfig config = new PageCacheEvictionConfig(false, Set.of("vec", "pos"));
    assertFalse(config.shouldEvict("vec"));
    assertFalse(config.shouldEvict("pos"));
    assertFalse(config.shouldEvict("dvd"));
  }

  @Test
  public void testShouldEvict_specificExtensions() {
    PageCacheEvictionConfig config = new PageCacheEvictionConfig(true, Set.of("vec", "pos"));
    assertTrue(config.shouldEvict("vec"));
    assertTrue(config.shouldEvict("pos"));
    assertFalse(config.shouldEvict("dvd"));
    assertFalse(config.shouldEvict("tim"));
  }

  @Test
  public void testShouldEvict_wildcard() {
    PageCacheEvictionConfig config =
        new PageCacheEvictionConfig(true, Set.of(PageCacheEvictionConfig.ALL_EXTENSIONS));
    assertTrue(config.shouldEvict("vec"));
    assertTrue(config.shouldEvict("pos"));
    assertTrue(config.shouldEvict("dvd"));
    assertTrue(config.shouldEvict("anything"));
  }

  @Test
  public void testShouldEvict_emptyExtensions() {
    PageCacheEvictionConfig config = new PageCacheEvictionConfig(true, Collections.emptySet());
    assertFalse(config.shouldEvict("vec"));
    assertFalse(config.shouldEvict("pos"));
  }
}
