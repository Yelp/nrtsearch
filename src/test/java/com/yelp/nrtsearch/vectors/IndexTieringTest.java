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
package com.yelp.nrtsearch.vectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class IndexTieringTest {

  @Test
  public void testIdleStopAndRestart() {
    List<String> started = new ArrayList<>();
    List<String> stopped = new ArrayList<>();
    AtomicLong now = new AtomicLong(1000);
    VectorService.Options options = new VectorService.Options();
    options.idleStopMs = 100;
    IndexTiering tiering = new IndexTiering(started::add, stopped::add, options);
    tiering.setClock(now::get);

    tiering.markStarted("a");
    tiering.markStarted("b");
    assertTrue(tiering.stopIdleIndexes().isEmpty());

    now.addAndGet(50);
    tiering.withIndex("b", () -> null);
    now.addAndGet(60);
    // a idle for 110ms, b for 60ms
    assertEquals(List.of("a"), tiering.stopIdleIndexes());
    assertFalse(tiering.isStarted("a"));
    assertTrue(tiering.isStarted("b"));

    // next request starts it again
    assertEquals("ok", tiering.withIndex("a", () -> "ok"));
    assertEquals(List.of("a"), started);
    assertTrue(tiering.isStarted("a"));
    assertEquals(List.of("a"), stopped);
  }

  @Test
  public void testDisabled() {
    List<String> stopped = new ArrayList<>();
    IndexTiering tiering = new IndexTiering(s -> {}, stopped::add, new VectorService.Options());
    tiering.markStarted("a");
    assertFalse(tiering.isEnabled());
    assertTrue(tiering.stopIdleIndexes().isEmpty());
    assertEquals("x", tiering.withIndex("a", () -> "x"));
  }
}
