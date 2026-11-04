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
package com.yelp.nrtsearch.server.leader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class EpochPointerFenceTest {
  private static final String KEY = "svc/idx/point_state/_current";
  private InMemoryConditionalObjectStore store;

  @Before
  public void setUp() {
    store = new InMemoryConditionalObjectStore();
  }

  private String value() throws Exception {
    return EpochPointerFence.decodeValue(
        new String(store.get(KEY).get().data(), StandardCharsets.UTF_8));
  }

  private long pointerEpoch() throws Exception {
    return EpochPointerFence.decodeEpoch(
        new String(store.get(KEY).get().data(), StandardCharsets.UTF_8));
  }

  @Test
  public void testInactiveRejectsWrites() {
    EpochPointerFence fence = new EpochPointerFence(store);
    assertThrows(FencedException.class, () -> fence.write(KEY, "v1"));
    assertThrows(FencedException.class, () -> fence.claim(KEY));
  }

  @Test
  public void testCreateAndUpdate() throws Exception {
    EpochPointerFence fence = new EpochPointerFence(store);
    fence.activate(1);
    assertEquals(Optional.empty(), fence.claim(KEY));
    fence.write(KEY, "v1");
    fence.write(KEY, "v2");
    assertEquals("v2", value());
    assertEquals(1, pointerEpoch());
  }

  @Test
  public void testClaimUnfencedPointer() throws Exception {
    store.put(KEY, "legacy".getBytes(), Map.of());
    EpochPointerFence fence = new EpochPointerFence(store);
    fence.activate(3);
    assertEquals(Optional.of("legacy"), fence.claim(KEY));
    assertEquals(3, pointerEpoch());
    assertEquals("legacy", value());
    fence.write(KEY, "v1");
    assertEquals("v1", value());
  }

  @Test
  public void testFirstWriteClaimsLegacyPointer() throws Exception {
    store.put(KEY, "legacy".getBytes(), Map.of());
    EpochPointerFence fence = new EpochPointerFence(store);
    fence.activate(1);
    fence.write(KEY, "v1");
    assertEquals("v1", value());
    assertEquals(1, pointerEpoch());
  }

  /** The GC pause scenario: old leader resumes after a new leader claimed the pointer. */
  @Test
  public void testStaleLeaderFencedAfterNewLeaderClaims() throws Exception {
    List<FencedException> fencedEvents = new ArrayList<>();
    EpochPointerFence oldLeader = new EpochPointerFence(store);
    oldLeader.registerFencedListener(fencedEvents::add);
    oldLeader.activate(1);
    oldLeader.claim(KEY);
    oldLeader.write(KEY, "a1");

    EpochPointerFence newLeader = new EpochPointerFence(store);
    newLeader.activate(2);
    assertEquals(Optional.of("a1"), newLeader.claim(KEY));

    // old leader still thinks it is active (paused), its write must fail
    FencedException e = assertThrows(FencedException.class, () -> oldLeader.write(KEY, "a2"));
    assertTrue(e.getMessage().contains("no longer leader"));
    assertEquals(1, fencedEvents.size());
    assertEquals("a1", value());

    newLeader.write(KEY, "b1");
    assertEquals("b1", value());
    assertEquals(2, pointerEpoch());
  }

  /** Old leader writes before the new leader claims: the new leader sees the write. */
  @Test
  public void testWriteBeforeClaimIsVisibleToNewLeader() throws Exception {
    EpochPointerFence oldLeader = new EpochPointerFence(store);
    oldLeader.activate(1);
    oldLeader.claim(KEY);
    oldLeader.write(KEY, "a1");
    oldLeader.write(KEY, "a2");

    EpochPointerFence newLeader = new EpochPointerFence(store);
    newLeader.activate(2);
    assertEquals(Optional.of("a2"), newLeader.claim(KEY));
    assertThrows(FencedException.class, () -> oldLeader.write(KEY, "a3"));
  }

  @Test
  public void testOlderEpochCannotClaimNewerPointer() throws Exception {
    EpochPointerFence newer = new EpochPointerFence(store);
    newer.activate(5);
    newer.claim(KEY);
    newer.write(KEY, "n1");

    List<FencedException> fencedEvents = new ArrayList<>();
    EpochPointerFence older = new EpochPointerFence(store);
    older.registerFencedListener(fencedEvents::add);
    older.activate(4);
    assertThrows(FencedException.class, () -> older.claim(KEY));
    assertEquals(1, fencedEvents.size());
    assertEquals("n1", value());
  }

  @Test
  public void testConcurrentCreateOnlyOneWins() throws Exception {
    EpochPointerFence a = new EpochPointerFence(store);
    a.activate(1);
    a.claim(KEY);
    EpochPointerFence b = new EpochPointerFence(store);
    b.activate(2);
    b.claim(KEY);
    // both saw no pointer; first create wins, second is fenced
    b.write(KEY, "b1");
    assertThrows(FencedException.class, () -> a.write(KEY, "a1"));
    assertEquals("b1", value());
  }

  @Test
  public void testDeactivateRejectsWrites() throws Exception {
    EpochPointerFence fence = new EpochPointerFence(store);
    fence.activate(1);
    fence.claim(KEY);
    fence.write(KEY, "v1");
    fence.deactivate();
    assertThrows(FencedException.class, () -> fence.write(KEY, "v2"));
    assertEquals("v1", value());
    // reactivated at a newer epoch, the first write re-claims the pointer
    fence.activate(2);
    fence.write(KEY, "v2");
    assertEquals("v2", value());
    assertEquals(2, pointerEpoch());
  }

  /**
   * S3 versions are content hashes. The new leader claims a pointer whose value it does not change,
   * and the old leader then writes the same value it wrote before; both must still be fenced.
   */
  @Test
  public void testIdenticalValuesStillFenced() throws Exception {
    EpochPointerFence oldLeader = new EpochPointerFence(store);
    oldLeader.activate(1);
    oldLeader.claim(KEY);
    oldLeader.write(KEY, "same");
    oldLeader.write(KEY, "same");

    EpochPointerFence newLeader = new EpochPointerFence(store);
    newLeader.activate(2);
    assertEquals(Optional.of("same"), newLeader.claim(KEY));
    assertThrows(FencedException.class, () -> oldLeader.write(KEY, "same"));
    assertEquals(2, pointerEpoch());
  }

  @Test
  public void testDecodeLegacyAndFencedBodies() {
    assertEquals("v1", EpochPointerFence.decodeValue("v1"));
    assertEquals(0, EpochPointerFence.decodeEpoch("v1"));
    String fenced = new String(EpochPointerFence.encode("v1", 7), StandardCharsets.UTF_8);
    assertEquals("v1", EpochPointerFence.decodeValue(fenced));
    assertEquals(7, EpochPointerFence.decodeEpoch(fenced));
  }

  @Test
  public void testMultiLineValueRejected() throws Exception {
    EpochPointerFence fence = new EpochPointerFence(store);
    fence.activate(1);
    assertThrows(IllegalArgumentException.class, () -> fence.write(KEY, "a\nb"));
  }

  /** A write that started in epoch N must not use the version claimed in epoch N+1. */
  @Test
  public void testWriteDoesNotUseVersionFromNewerEpoch() throws Exception {
    EpochPointerFence fence = new EpochPointerFence(store);
    fence.activate(1);
    fence.claim(KEY);
    fence.write(KEY, "v1");
    fence.activate(2);
    fence.claim(KEY);
    // simulate a write that read epoch 1 before activation: owned version is now tagged epoch 2
    java.lang.reflect.Field f = EpochPointerFence.class.getDeclaredField("epoch");
    f.setAccessible(true);
    f.setLong(fence, 1);
    FencedException e = assertThrows(FencedException.class, () -> fence.write(KEY, "stale"));
    assertEquals(1, e.getEpoch());
    f.setLong(fence, 2);
    assertEquals("v1", value());
    fence.write(KEY, "v2");
    assertEquals("v2", value());
  }

  @Test
  public void testFencedExceptionCarriesEpoch() throws Exception {
    EpochPointerFence a = new EpochPointerFence(store);
    a.activate(1);
    a.claim(KEY);
    a.write(KEY, "a");
    EpochPointerFence b = new EpochPointerFence(store);
    b.activate(2);
    b.claim(KEY);
    FencedException e = assertThrows(FencedException.class, () -> a.write(KEY, "a2"));
    assertEquals(1, e.getEpoch());
  }

  @Test
  public void testMalformedFenceLine() {
    assertThrows(
        IllegalStateException.class, () -> EpochPointerFence.decodeEpoch("v\n#nrt-fence epoch=x"));
    assertThrows(
        IllegalStateException.class, () -> EpochPointerFence.decodeEpoch("v\n#nrt-fence nonce=1"));
  }

  /**
   * The new leader claims a pointer that does not exist yet. An older leader then tries to create
   * it; that must fail, otherwise it could acknowledge a commit outside the new leader's lineage,
   * and the new leader's own first write must still succeed.
   */
  @Test
  public void testOldLeaderCannotCreatePointerAfterClaim() throws Exception {
    EpochPointerFence oldLeader = new EpochPointerFence(store);
    oldLeader.activate(1);
    assertEquals(Optional.empty(), oldLeader.claim(KEY));

    EpochPointerFence newLeader = new EpochPointerFence(store);
    newLeader.activate(2);
    assertEquals(Optional.empty(), newLeader.claim(KEY));

    assertThrows(FencedException.class, () -> oldLeader.write(KEY, "old"));
    newLeader.write(KEY, "new");
    assertEquals("new", value());
    assertEquals(2, pointerEpoch());
  }

  @Test
  public void testClaimOfAbsentCreatesTombstone() throws Exception {
    EpochPointerFence fence = new EpochPointerFence(store);
    fence.activate(3);
    assertEquals(Optional.empty(), fence.claim(KEY));
    String body = new String(store.get(KEY).get().data(), StandardCharsets.UTF_8);
    assertTrue(EpochPointerFence.isAbsent(body));
    assertEquals(3, EpochPointerFence.decodeEpoch(body));
    // a later claim (e.g. re-promotion) still reports it absent
    fence.activate(4);
    assertEquals(Optional.empty(), fence.claim(KEY));
  }

  /** The first write of an unclaimed pointer claims it, unless a newer epoch owns it. */
  @Test
  public void testWriteClaimsUnclaimedPointer() throws Exception {
    EpochPointerFence fence = new EpochPointerFence(store);
    fence.activate(1);
    fence.write(KEY, "v");
    assertEquals("v", value());

    EpochPointerFence newer = new EpochPointerFence(store);
    newer.activate(2);
    newer.write(KEY, "n");
    EpochPointerFence older = new EpochPointerFence(store);
    older.activate(1);
    assertThrows(FencedException.class, () -> older.write(KEY, "o"));
    assertEquals("n", value());
  }

  /** Creating a pointer that does not exist yet is a single request: the GET plus one create. */
  @Test
  public void testCreatePointerSingleWrite() throws Exception {
    java.util.concurrent.atomic.AtomicInteger writes =
        new java.util.concurrent.atomic.AtomicInteger();
    ConditionalObjectStore counting =
        new ConditionalObjectStore() {
          @Override
          public Optional<VersionedObject> get(String key) throws java.io.IOException {
            return store.get(key);
          }

          @Override
          public String putIfAbsent(String key, byte[] data, Map<String, String> metadata)
              throws java.io.IOException {
            writes.incrementAndGet();
            return store.putIfAbsent(key, data, metadata);
          }

          @Override
          public String putIfMatch(
              String key, byte[] data, Map<String, String> metadata, String expectedVersion)
              throws java.io.IOException {
            writes.incrementAndGet();
            return store.putIfMatch(key, data, metadata, expectedVersion);
          }
        };
    EpochPointerFence fence = new EpochPointerFence(counting);
    fence.activate(1);
    fence.write(KEY, "v1");
    assertEquals(1, writes.get());
    assertEquals("v1", value());
    fence.write(KEY, "v2");
    assertEquals(2, writes.get());
  }
}
