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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.util.concurrent.MoreExecutors;
import com.yelp.nrtsearch.server.grpc.MatchAllQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class TemplateDriftMonitorTest {
  private static final SearchRequest KNOWN =
      SearchRequest.newBuilder()
          .setIndexName("idx")
          .setTopHits(10)
          .setQuery(
              Query.newBuilder()
                  .setTermQuery(TermQuery.newBuilder().setField("f").setTextValue("a")))
          .build();
  // same template as KNOWN, different value
  private static final SearchRequest KNOWN_OTHER_VALUE =
      KNOWN.toBuilder()
          .setQuery(
              Query.newBuilder()
                  .setTermQuery(TermQuery.newBuilder().setField("f").setTextValue("b")))
          .build();
  private static final SearchRequest NEW_SHAPE =
      SearchRequest.newBuilder()
          .setIndexName("idx")
          .setTopHits(10)
          .setQuery(Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder()))
          .build();

  private final AtomicLong now = new AtomicLong(1_000_000);
  private final List<Double> relearnedAt = new ArrayList<>();

  private TemplateDriftMonitor monitor(int checkEvery, int window, long cooldownMs) {
    return new TemplateDriftMonitor(
        "idx",
        checkEvery,
        window,
        0.95,
        cooldownMs,
        ratio -> {
          relearnedAt.add(ratio);
          return Set.of(TemplateWarmingQueryGenerator.templateKey(NEW_SHAPE));
        },
        now::get,
        MoreExecutors.newDirectExecutorService());
  }

  @Test
  public void testTemplateKeyIgnoresValues() {
    assertEquals(
        TemplateWarmingQueryGenerator.templateKey(KNOWN),
        TemplateWarmingQueryGenerator.templateKey(KNOWN_OTHER_VALUE));
    assertFalse(
        TemplateWarmingQueryGenerator.templateKey(KNOWN)
            .equals(TemplateWarmingQueryGenerator.templateKey(NEW_SHAPE)));
  }

  @Test
  public void testMatchingTrafficDoesNotRelearn() {
    TemplateDriftMonitor m = monitor(1, 10, 0);
    m.setLibraryKeys(Set.of(TemplateWarmingQueryGenerator.templateKey(KNOWN)));
    for (int i = 0; i < 100; ++i) {
      m.observe(i % 2 == 0 ? KNOWN : KNOWN_OTHER_VALUE);
    }
    assertEquals(1.0, m.getLastMatchRatio(), 0);
    assertTrue(relearnedAt.isEmpty());
  }

  @Test
  public void testDriftTriggersRelearnAndAdoptsNewKeys() {
    TemplateDriftMonitor m = monitor(1, 10, 0);
    m.setLibraryKeys(Set.of(TemplateWarmingQueryGenerator.templateKey(KNOWN)));
    // 8 of 10 match: 0.8 < 0.95
    for (int i = 0; i < 10; ++i) {
      m.observe(i < 8 ? KNOWN : NEW_SHAPE);
    }
    assertEquals(List.of(0.8), relearnedAt);
    // the relearned keys are used from now on: new shape traffic matches
    for (int i = 0; i < 10; ++i) {
      m.observe(NEW_SHAPE);
    }
    assertEquals(1.0, m.getLastMatchRatio(), 0);
    assertEquals(1, relearnedAt.size());
    assertFalse(m.isRelearning());
  }

  @Test
  public void testCooldown() {
    TemplateDriftMonitor m =
        new TemplateDriftMonitor(
            "idx",
            1,
            5,
            0.95,
            60_000,
            ratio -> {
              relearnedAt.add(ratio);
              // relearn did not help: keep the old library
              return Set.of(TemplateWarmingQueryGenerator.templateKey(KNOWN));
            },
            now::get,
            MoreExecutors.newDirectExecutorService());
    m.setLibraryKeys(Set.of(TemplateWarmingQueryGenerator.templateKey(KNOWN)));
    for (int i = 0; i < 5; ++i) {
      m.observe(NEW_SHAPE);
    }
    assertEquals(1, relearnedAt.size());
    // still drifting, but within the cooldown
    now.addAndGet(30_000);
    for (int i = 0; i < 5; ++i) {
      m.observe(NEW_SHAPE);
    }
    assertEquals(1, relearnedAt.size());
    // after the cooldown
    now.addAndGet(31_000);
    for (int i = 0; i < 5; ++i) {
      m.observe(NEW_SHAPE);
    }
    assertEquals(2, relearnedAt.size());
  }

  @Test
  public void testSamplesOneInCheckEvery() {
    TemplateDriftMonitor m = monitor(10, 2, 0);
    m.setLibraryKeys(Set.of(TemplateWarmingQueryGenerator.templateKey(KNOWN)));
    // 19 queries: only the 10th is checked, the window of 2 is not full yet
    for (int i = 0; i < 19; ++i) {
      m.observe(NEW_SHAPE);
    }
    assertTrue(Double.isNaN(m.getLastMatchRatio()));
    m.observe(NEW_SHAPE);
    assertEquals(0.0, m.getLastMatchRatio(), 0);
    assertEquals(1, relearnedAt.size());
  }

  @Test
  public void testNoLibraryKeysNoChecks() {
    TemplateDriftMonitor m = monitor(1, 1, 0);
    m.setLibraryKeys(null);
    for (int i = 0; i < 10; ++i) {
      m.observe(NEW_SHAPE);
    }
    assertTrue(Double.isNaN(m.getLastMatchRatio()));
    assertTrue(relearnedAt.isEmpty());
  }

  @Test
  public void testEmptyLibraryLearnsFromTraffic() {
    TemplateDriftMonitor m = monitor(1, 3, 0);
    m.setLibraryKeys(Set.of());
    for (int i = 0; i < 3; ++i) {
      m.observe(NEW_SHAPE);
    }
    assertEquals(List.of(0.0), relearnedAt);
  }

  @Test
  public void testRelearnFailureIsContained() {
    TemplateDriftMonitor m =
        new TemplateDriftMonitor(
            "idx",
            1,
            1,
            0.95,
            0,
            ratio -> {
              throw new IllegalStateException("store failed");
            },
            now::get,
            MoreExecutors.newDirectExecutorService());
    m.setLibraryKeys(Set.of());
    m.observe(NEW_SHAPE);
    assertFalse(m.isRelearning());
  }

  @Test
  public void testInvalidSettings() {
    assertThrows(IllegalArgumentException.class, () -> monitor(0, 1, 0));
    assertThrows(IllegalArgumentException.class, () -> monitor(1, 0, 0));
  }
}
