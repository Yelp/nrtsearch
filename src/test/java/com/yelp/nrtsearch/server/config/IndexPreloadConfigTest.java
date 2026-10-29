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
import java.util.List;
import java.util.Set;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.IOContext;
import org.junit.Test;

public class IndexPreloadConfigTest {

  private static IndexPreloadConfig getConfig(String configFile) {
    return IndexPreloadConfig.fromConfig(
        new YamlConfigReader(new ByteArrayInputStream(configFile.getBytes())));
  }

  @Test
  public void testDefault() {
    String configFile = "nodeName: \"server_foo\"";
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

  private static final List<String> INDEX_FILE_NAMES =
      List.of(
          "_0.fdt",
          "_0.fdx",
          "_0.fdm",
          "_0.fnm",
          "_0.si",
          "_0.cfs",
          "_0.cfe",
          "_0.nvd",
          "_0.nvm",
          "_0_1.liv",
          "_0_2.fnm",
          "_0_2_Lucene90_0.dvd",
          "_0_Lucene90_0.dvd",
          "_0_Lucene90_0.dvm",
          "_0_Lucene104_0.tim",
          "_0_Lucene104_0.tip",
          "_0_Lucene104_0.tmd",
          "_0_Lucene104_0.doc",
          "_0_Lucene104_0.pos",
          "_0_Lucene104HnswBinaryQuantizedVectorsFormat_0.vec",
          "_0_Lucene104HnswBinaryQuantizedVectorsFormat_0.veq",
          "_0_Lucene104HnswBinaryQuantizedVectorsFormat_0.vex",
          "_1a.kdd",
          "_1a.kdi",
          "_1a.kdm",
          "segments_1",
          "pending_segments_2",
          "write.lock",
          "file",
          "_0.tmp.copy");

  private static void assertSameAsExtensionMatch(Set<String> extensions) {
    IndexPreloadConfig config = new IndexPreloadConfig(true, extensions);
    for (String fileName : INDEX_FILE_NAMES) {
      boolean expected =
          extensions.contains(IndexPreloadConfig.ALL_EXTENSIONS)
              || extensions.contains(FileSwitchDirectory.getExtension(fileName));
      assertEquals(
          "file: " + fileName + ", extensions: " + extensions,
          expected,
          config.preloadPredicate().test(fileName, IOContext.DEFAULT));
    }
  }

  @Test
  public void testPreloadPredicate_matchesExtensionBehavior() {
    assertSameAsExtensionMatch(Set.of(IndexPreloadConfig.ALL_EXTENSIONS));
    assertSameAsExtensionMatch(Set.of("dvd", "tim"));
    assertSameAsExtensionMatch(Set.of("vec", "veq", "vex", "cfs"));
    assertSameAsExtensionMatch(Set.of("liv", "fnm", "si", "lock", "copy"));
    assertSameAsExtensionMatch(Set.of("fdt"));
  }
}
