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
import org.apache.lucene.store.IOContext;
import org.junit.Test;

public class IndexPreloadConfigTest {

  private static IndexPreloadConfig getConfig(String configFile) {
    return IndexPreloadConfig.fromConfig(
        new YamlConfigReader(new ByteArrayInputStream(configFile.getBytes())));
  }

  @Test
  public void testDefault() {
    String configFile = "nodeName: \"lucene_server_foo\"";
    IndexPreloadConfig config = getConfig(configFile);
    assertFalse(config.getPreload());
    assertEquals(config.getExtensions(), Collections.singleton(IndexPreloadConfig.ALL_EXTENSIONS));
  }

  @Test
  public void testNoPreload() {
    String configFile = String.join("\n", "preload: ", "  enabled: true");
    IndexPreloadConfig config = getConfig(configFile);
    assertTrue(config.getPreload());
    assertEquals(config.getExtensions(), Collections.singleton(IndexPreloadConfig.ALL_EXTENSIONS));
  }

  @Test
  public void testPreloadExtensions() {
    String configFile =
        String.join("\n", "preload:", "  enabled: true", "  extensions:", "    - dvd", "    - tim");
    IndexPreloadConfig config = getConfig(configFile);
    assertTrue(config.getPreload());
    assertEquals(config.getExtensions(), Set.of("dvd", "tim"));
  }

  @Test
  public void testPreloadPredicate_all() {
    String configFile = "preload:\n enabled: true";
    IndexPreloadConfig config = getConfig(configFile);
    assertTrue(config.preloadPredicate().test("file.dvd", IOContext.DEFAULT));
    assertTrue(config.preloadPredicate().test("file.tim", IOContext.DEFAULT));
    assertTrue(config.preloadPredicate().test("file", IOContext.DEFAULT));
  }

  @Test
  public void testPreloadPredicate_specific() {
    String configFile =
        String.join("\n", "preload:", "  enabled: true", "  extensions:", "    - dvd", "    - tim");
    IndexPreloadConfig config = getConfig(configFile);
    assertTrue(config.preloadPredicate().test("file.dvd", IOContext.DEFAULT));
    assertTrue(config.preloadPredicate().test("file.tim", IOContext.DEFAULT));
    assertFalse(config.preloadPredicate().test("file", IOContext.DEFAULT));
    assertFalse(config.preloadPredicate().test("file.mp3", IOContext.DEFAULT));
  }

  @Test
  public void testPreloadPredicate_disabled() {
    String configFile = "preload:\n enabled: false";
    IndexPreloadConfig config = getConfig(configFile);
    assertFalse(config.preloadPredicate().test("file.dvd", IOContext.DEFAULT));
    assertFalse(config.preloadPredicate().test("file.tim", IOContext.DEFAULT));
    assertFalse(config.preloadPredicate().test("file", IOContext.DEFAULT));
  }
}
