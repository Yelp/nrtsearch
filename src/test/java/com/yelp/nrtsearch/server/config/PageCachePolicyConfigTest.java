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
package com.yelp.nrtsearch.server.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.PageCachePolicyConfig.Action;
import com.yelp.nrtsearch.server.config.PageCachePolicyConfig.IndexPageCachePolicy;
import com.yelp.nrtsearch.server.config.PageCachePolicyConfig.Rule;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Test;

public class PageCachePolicyConfigTest {
  private static final String QUANTIZED_VEC = "_0_Lucene104HnswBinaryQuantizedVectorsFormat_0.vec";
  private static final String QUANTIZED_VEQ = "_0_Lucene104HnswBinaryQuantizedVectorsFormat_0.veq";
  private static final String FLOAT_VEC = "_0_Lucene99HnswVectorsFormat_0.vec";

  private static PageCachePolicyConfig getConfig(String... lines) {
    String configFile = String.join("\n", lines);
    return PageCachePolicyConfig.fromConfig(
        new YamlConfigReader(new ByteArrayInputStream(configFile.getBytes())));
  }

  private static IllegalArgumentException assertInvalid(String... lines) {
    return assertThrows(IllegalArgumentException.class, () -> getConfig(lines));
  }

  @Test
  public void testDefault() {
    PageCachePolicyConfig config = getConfig("nodeName: \"server_foo\"");
    assertEquals(List.of(), config.getGlobalRules());
    assertEquals(Map.of(), config.getIndexRules());
    IndexPageCachePolicy policy = config.forIndex("my_index");
    assertSame(config.getGlobalPolicy(), policy);
    assertEquals(Action.DEFAULT, policy.indexFileAction("_0.fdt"));
    assertFalse(policy.hasAction(Action.PRELOAD));
  }

  @Test
  public void testGlobalRules() {
    PageCachePolicyConfig config =
        getConfig(
            "pageCache:",
            "  rules:",
            "    - files: [\"*QuantizedVectorsFormat_*.vec\"]",
            "      action: EVICT",
            "    - files: [\"*.tip\", \"*.tmd\"]",
            "      action: preload");
    assertEquals(
        List.of(
            new Rule(List.of("*QuantizedVectorsFormat_*.vec"), Action.EVICT),
            new Rule(List.of("*.tip", "*.tmd"), Action.PRELOAD)),
        config.getGlobalRules());
    IndexPageCachePolicy policy = config.forIndex("my_index");
    assertEquals(Action.EVICT, policy.indexFileAction(QUANTIZED_VEC));
    assertEquals(Action.DEFAULT, policy.indexFileAction(QUANTIZED_VEQ));
    assertEquals(Action.DEFAULT, policy.indexFileAction(FLOAT_VEC));
    assertEquals(Action.PRELOAD, policy.indexFileAction("_0_Lucene104_0.tip"));
    assertEquals(Action.PRELOAD, policy.indexFileAction("_0_Lucene104_0.tmd"));
    assertEquals(Action.DEFAULT, policy.indexFileAction("_0.fdt"));
  }

  @Test
  public void testFirstMatchWins() {
    IndexPageCachePolicy policy =
        new PageCachePolicyConfig(
                List.of(
                    new Rule(List.of("*.fdt"), Action.DEFAULT),
                    new Rule(List.of("*"), Action.PRELOAD),
                    new Rule(List.of("*.vec"), Action.EVICT)),
                Map.of())
            .getGlobalPolicy();
    assertEquals(Action.DEFAULT, policy.indexFileAction("_0.fdt"));
    assertEquals(Action.PRELOAD, policy.indexFileAction("_0.tim"));
    assertEquals(Action.PRELOAD, policy.indexFileAction(FLOAT_VEC));
  }

  @Test
  public void testMatchStrippedNames() {
    IndexPageCachePolicy policy =
        new PageCachePolicyConfig(
                List.of(
                    new Rule(List.of("_Lucene104_*"), Action.PRELOAD),
                    new Rule(List.of(".fdt"), Action.EVICT),
                    new Rule(List.of("segments_*"), Action.PRELOAD)),
                Map.of())
            .getGlobalPolicy();
    // segment prefix is removed from segment files
    assertEquals(Action.PRELOAD, policy.indexFileAction("_0_Lucene104_0.tip"));
    assertEquals(Action.PRELOAD, policy.indexFileAction("_1a_Lucene104_2.doc"));
    assertEquals(Action.EVICT, policy.indexFileAction("_0.fdt"));
    assertEquals(Action.EVICT, policy.indexFileAction("_12.fdt"));
    // compound file sub-file names are already stripped
    assertEquals(Action.PRELOAD, policy.fileAction("_Lucene104_0.tip"));
    assertEquals(Action.EVICT, policy.fileAction(".fdt"));
    // non segment files are matched by full name
    assertEquals(Action.PRELOAD, policy.indexFileAction("segments_1"));
    assertEquals(Action.DEFAULT, policy.indexFileAction("write.lock"));
  }

  @Test
  public void testStripSegmentName() {
    assertEquals("_Lucene104_0.tip", IndexPageCachePolicy.stripSegmentName("_0_Lucene104_0.tip"));
    assertEquals(".fdt", IndexPageCachePolicy.stripSegmentName("_0.fdt"));
    assertEquals("_1.liv", IndexPageCachePolicy.stripSegmentName("_0_1.liv"));
    assertEquals("segments_1", IndexPageCachePolicy.stripSegmentName("segments_1"));
    assertEquals("pending_segments_2", IndexPageCachePolicy.stripSegmentName("pending_segments_2"));
    assertEquals("write.lock", IndexPageCachePolicy.stripSegmentName("write.lock"));
  }

  @Test
  public void testIndexRules() {
    PageCachePolicyConfig config =
        getConfig(
            "pageCache:",
            "  rules:",
            "    - files: [\"*.vec\"]",
            "      action: EVICT",
            "  indices:",
            "    hot_index:",
            "      rules:",
            "        - files: [\"*\"]",
            "          action: PRELOAD",
            "    stored_fields_index:",
            "      rules:",
            "        - files: [\"*.fdt\"]",
            "          action: EVICT");
    assertEquals(Set.of("hot_index", "stored_fields_index"), config.getIndexRules().keySet());

    // catch all index rule replaces the global rules
    IndexPageCachePolicy hotPolicy = config.forIndex("hot_index");
    assertEquals(Action.PRELOAD, hotPolicy.indexFileAction(FLOAT_VEC));
    assertEquals(Action.PRELOAD, hotPolicy.indexFileAction("_0.fdt"));

    // index rules are checked before global rules
    IndexPageCachePolicy storedPolicy = config.forIndex("stored_fields_index");
    assertEquals(Action.EVICT, storedPolicy.indexFileAction("_0.fdt"));
    assertEquals(Action.EVICT, storedPolicy.indexFileAction(FLOAT_VEC));
    assertEquals(Action.DEFAULT, storedPolicy.indexFileAction("_0.tim"));
    assertEquals(2, storedPolicy.getRules().size());

    // index without rules uses global rules
    IndexPageCachePolicy otherPolicy = config.forIndex("other_index");
    assertSame(config.getGlobalPolicy(), otherPolicy);
    assertEquals(Action.EVICT, otherPolicy.indexFileAction(FLOAT_VEC));
    assertEquals(Action.DEFAULT, otherPolicy.indexFileAction("_0.fdt"));
  }

  @Test
  public void testIndexRulesOnly() {
    PageCachePolicyConfig config =
        getConfig(
            "pageCache:",
            "  indices:",
            "    hot_index:",
            "      rules:",
            "        - files: [\"*\"]",
            "          action: PRELOAD");
    assertEquals(List.of(), config.getGlobalRules());
    assertEquals(Action.PRELOAD, config.forIndex("hot_index").indexFileAction("_0.fdt"));
    assertEquals(Action.DEFAULT, config.forIndex("other_index").indexFileAction("_0.fdt"));
  }

  @Test
  public void testPreloadPredicatePerIndex() {
    PageCachePolicyConfig config =
        getConfig(
            "pageCache:",
            "  indices:",
            "    hot_index:",
            "      rules:",
            "        - files: [\"*\"]",
            "          action: PRELOAD",
            "    tip_index:",
            "      rules:",
            "        - files: [\"*.tip\"]",
            "          action: PRELOAD");
    BiPredicate<String, IOContext> hot =
        new IndexPreloadConfig(config.forIndex("hot_index")).preloadPredicate();
    BiPredicate<String, IOContext> tip =
        new IndexPreloadConfig(config.forIndex("tip_index")).preloadPredicate();
    BiPredicate<String, IOContext> other =
        new IndexPreloadConfig(config.forIndex("other_index")).preloadPredicate();
    assertTrue(hot.test("_0.fdt", IOContext.DEFAULT));
    assertTrue(hot.test("_0_Lucene104_0.tip", IOContext.DEFAULT));
    assertFalse(tip.test("_0.fdt", IOContext.DEFAULT));
    assertTrue(tip.test("_0_Lucene104_0.tip", IOContext.DEFAULT));
    assertFalse(other.test("_0.fdt", IOContext.DEFAULT));
    assertFalse(other.test("_0_Lucene104_0.tip", IOContext.DEFAULT));
  }

  @Test
  public void testLegacyPreloadAll() {
    PageCachePolicyConfig config = getConfig("preload:", "  enabled: true");
    assertEquals(List.of(new Rule(List.of("*"), Action.PRELOAD)), config.getGlobalRules());
  }

  @Test
  public void testLegacyPreloadExtensions() {
    PageCachePolicyConfig config =
        getConfig("preload:", "  enabled: true", "  extensions:", "    - tim", "    - dvd");
    assertEquals(
        List.of(new Rule(List.of("*.dvd", "*.tim"), Action.PRELOAD)), config.getGlobalRules());
    IndexPageCachePolicy policy = config.getGlobalPolicy();
    assertEquals(Action.PRELOAD, policy.indexFileAction("_0_Lucene90_0.dvd"));
    assertEquals(Action.PRELOAD, policy.indexFileAction("_0_Lucene104_0.tim"));
    assertEquals(Action.DEFAULT, policy.indexFileAction("_0.fdt"));
  }

  @Test
  public void testLegacyPreloadDisabled() {
    PageCachePolicyConfig config =
        getConfig("preload:", "  enabled: false", "  extensions:", "    - tim");
    assertEquals(List.of(), config.getGlobalRules());
  }

  @Test
  public void testLegacyPreloadEscapesGlob() {
    List<Rule> rules = PageCachePolicyConfig.fromLegacyPreload(true, Set.of("a*b"));
    assertEquals(List.of(new Rule(List.of("*.a\\*b"), Action.PRELOAD)), rules);
    IndexPageCachePolicy policy = new PageCachePolicyConfig(rules, Map.of()).getGlobalPolicy();
    assertEquals(Action.PRELOAD, policy.indexFileAction("_0.a*b"));
    assertEquals(Action.DEFAULT, policy.indexFileAction("_0.axb"));
  }

  @Test
  public void testLegacyAndPolicyConflict() {
    IllegalArgumentException e =
        assertInvalid(
            "preload:",
            "  enabled: true",
            "pageCache:",
            "  rules:",
            "    - files: [\"*\"]",
            "      action: PRELOAD");
    assertEquals(
        "Only one of pageCache and preload may be set, preload is deprecated", e.getMessage());
  }

  @Test
  public void testInvalidAction() {
    IllegalArgumentException e =
        assertInvalid("pageCache:", "  rules:", "    - files: [\"*\"]", "      action: PIN_IT");
    assertEquals(
        "Invalid pageCache.rules[0].action: PIN_IT, must be one of [PRELOAD, EVICT, DEFAULT]",
        e.getMessage());
  }

  @Test
  public void testMissingAction() {
    IllegalArgumentException e = assertInvalid("pageCache:", "  rules:", "    - files: [\"*\"]");
    assertEquals("pageCache.rules[0].action must be set", e.getMessage());
  }

  @Test
  public void testMissingFiles() {
    IllegalArgumentException e = assertInvalid("pageCache:", "  rules:", "    - action: EVICT");
    assertEquals("pageCache.rules[0].files must be a non-empty list", e.getMessage());
  }

  @Test
  public void testEmptyFiles() {
    IllegalArgumentException e =
        assertInvalid("pageCache:", "  rules:", "    - files: []", "      action: EVICT");
    assertEquals("pageCache.rules[0].files must be a non-empty list", e.getMessage());
  }

  @Test
  public void testFilesNotList() {
    IllegalArgumentException e =
        assertInvalid("pageCache:", "  rules:", "    - files: \"*.vec\"", "      action: EVICT");
    assertEquals("pageCache.rules[0].files must be a non-empty list", e.getMessage());
  }

  @Test
  public void testInvalidGlob() {
    IllegalArgumentException e =
        assertInvalid("pageCache:", "  rules:", "    - files: [\"*.{vec\"]", "      action: EVICT");
    assertEquals("Invalid glob in pageCache.rules[0].files: *.{vec", e.getMessage());
  }

  @Test
  public void testRulesNotList() {
    IllegalArgumentException e = assertInvalid("pageCache:", "  rules:", "    files: [\"*\"]");
    assertEquals("pageCache.rules must be a list", e.getMessage());
  }

  @Test
  public void testUnknownKeys() {
    IllegalArgumentException e = assertInvalid("pageCache:", "  rule: []");
    assertTrue(e.getMessage(), e.getMessage().startsWith("Unknown key in pageCache: rule"));

    e =
        assertInvalid(
            "pageCache:",
            "  rules:",
            "    - files: [\"*\"]",
            "      action: EVICT",
            "      field: vector");
    assertTrue(
        e.getMessage(), e.getMessage().startsWith("Unknown key in pageCache.rules[0]: field"));

    e = assertInvalid("pageCache:", "  indices:", "    my_index:", "      fields: {}");
    assertTrue(
        e.getMessage(),
        e.getMessage().startsWith("Unknown key in pageCache.indices.my_index: fields"));
  }

  @Test
  public void testInvalidIndexRule() {
    IllegalArgumentException e =
        assertInvalid(
            "pageCache:",
            "  indices:",
            "    my_index:",
            "      rules:",
            "        - files: [\"*\"]",
            "          action: NOPE");
    assertTrue(
        e.getMessage(),
        e.getMessage().startsWith("Invalid pageCache.indices.my_index.rules[0].action: NOPE"));
  }

  @Test
  public void testRuleValidation() {
    assertThrows(IllegalArgumentException.class, () -> new Rule(List.of(), Action.EVICT));
    assertThrows(IllegalArgumentException.class, () -> new Rule(null, Action.EVICT));
    assertThrows(IllegalArgumentException.class, () -> new Rule(List.of("*"), null));
  }

  @Test
  public void testPreloadAllFilesPredicate() {
    PageCachePolicyConfig config =
        new PageCachePolicyConfig(
            List.of(new Rule(List.of("*"), Action.PRELOAD)),
            Map.of(
                "evict_index",
                List.of(new Rule(List.of("*.vec"), Action.EVICT)),
                "default_index",
                List.of(new Rule(List.of("*"), Action.DEFAULT))));
    assertTrue(config.getGlobalPolicy().preloadsAllFiles());
    assertSame(
        MMapDirectory.ALL_FILES,
        new IndexPreloadConfig(config.getGlobalPolicy()).preloadPredicate());

    // index rules before the catch all prevent the fast path
    IndexPageCachePolicy evictPolicy = config.forIndex("evict_index");
    assertFalse(evictPolicy.preloadsAllFiles());
    BiPredicate<String, IOContext> evictPredicate =
        new IndexPreloadConfig(evictPolicy).preloadPredicate();
    assertFalse(evictPredicate.test(FLOAT_VEC, IOContext.DEFAULT));
    assertTrue(evictPredicate.test("_0.fdt", IOContext.DEFAULT));

    // index catch all DEFAULT disables preloading
    IndexPageCachePolicy defaultPolicy = config.forIndex("default_index");
    assertFalse(defaultPolicy.preloadsAllFiles());
    BiPredicate<String, IOContext> defaultPredicate =
        new IndexPreloadConfig(defaultPolicy).preloadPredicate();
    assertFalse(defaultPredicate.test("_0.fdt", IOContext.DEFAULT));
  }

  @Test
  public void testPreloadPredicate_compoundFileRanges() {
    IndexPageCachePolicy policy =
        new PageCachePolicyConfig(List.of(new Rule(List.of("*"), Action.PRELOAD)), Map.of())
            .getGlobalPolicy();
    // whole compound files are preloaded by default, as used for the taxonomy directory
    IndexPreloadConfig wholeFiles = new IndexPreloadConfig(policy);
    assertSame(MMapDirectory.ALL_FILES, wholeFiles.preloadPredicate());

    // compound files are excluded, since their sub-file ranges are preloaded separately
    BiPredicate<String, IOContext> ranges = wholeFiles.withCompoundFileRanges().preloadPredicate();
    assertFalse(ranges.test("_0.cfs", IOContext.DEFAULT));
    assertFalse(ranges.test("_1a.cfs", IOContext.DEFAULT));
    assertTrue(ranges.test("_0.cfe", IOContext.DEFAULT));
    assertTrue(ranges.test("_0_Lucene104_0.tip", IOContext.DEFAULT));
    assertTrue(ranges.test("segments_1", IOContext.DEFAULT));
    assertSame(policy, wholeFiles.withCompoundFileRanges().getPolicy());
  }
}
