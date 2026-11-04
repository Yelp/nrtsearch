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

import com.yelp.nrtsearch.server.leader.ConditionalObjectStore.PreconditionFailedException;
import com.yelp.nrtsearch.server.leader.ConditionalObjectStore.VersionedObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fences writes to remote {@code _current} pointer objects using the leadership epoch.
 *
 * <p>Each pointer object carries the epoch of its writer in metadata. A node may only write a
 * pointer after claiming it for its epoch: reading the pointer, verifying its epoch is not newer,
 * and rewriting it with the local epoch using compare-and-set. All later writes are compare-and-set
 * against the version this node last wrote. Once a newer leader claims a pointer, every write by an
 * older leader fails, no matter how long the older leader was paused, so two leaders can never both
 * advance the same pointer.
 *
 * <p>While not active (standby), all writes are rejected.
 *
 * <p>Object stores like S3 derive the version (ETag) from the object content, so rewriting the same
 * bytes does not change the version. Every fenced write therefore encodes the epoch and a unique
 * nonce in the body, after the pointer value: {@code <value>\n#nrt-fence epoch=<E> nonce=<N>}. Use
 * {@link #decodeValue} to read the pointer value, which also accepts unfenced pointers containing
 * only the value.
 */
public class EpochPointerFence {
  private static final Logger logger = LoggerFactory.getLogger(EpochPointerFence.class);
  public static final String EPOCH_METADATA_KEY = "nrt-epoch";
  static final String FENCE_LINE_PREFIX = "\n#nrt-fence ";
  private static final int MAX_CLAIM_ATTEMPTS = 10;

  private final ConditionalObjectStore store;

  /** Version of a pointer this node owns, and the epoch it was written in. */
  private record Owned(String version, long epoch) {}

  // only versions written in the current epoch may be used as an If-Match condition
  private final ConcurrentHashMap<String, Owned> ownedVersions = new ConcurrentHashMap<>();
  private volatile long epoch = -1;
  private final AtomicReference<Consumer<FencedException>> fencedListener = new AtomicReference<>();

  public EpochPointerFence(ConditionalObjectStore store) {
    this.store = Objects.requireNonNull(store);
  }

  /**
   * Register the listener invoked when a write is fenced because a newer leader took over. The
   * fence is created before its owner (the leader manager), so the listener is registered after
   * construction; only one may be registered.
   *
   * @throws IllegalStateException if a listener is already registered
   */
  public void registerFencedListener(Consumer<FencedException> listener) {
    if (!fencedListener.compareAndSet(null, Objects.requireNonNull(listener))) {
      throw new IllegalStateException("Fenced listener already registered");
    }
  }

  /**
   * Activate the fence for an epoch. Pointers must be claimed before they can be written.
   *
   * @param newEpoch leadership epoch
   */
  public synchronized void activate(long newEpoch) {
    if (newEpoch <= 0) {
      throw new IllegalArgumentException("Epoch must be positive: " + newEpoch);
    }
    ownedVersions.clear();
    epoch = newEpoch;
    logger.info("Pointer fence activated for epoch {}", newEpoch);
  }

  /** Deactivate the fence, all writes are rejected until the next activation. */
  public synchronized void deactivate() {
    epoch = -1;
    ownedVersions.clear();
    logger.info("Pointer fence deactivated");
  }

  /** Current epoch, or -1 if not active. */
  public long getEpoch() {
    return epoch;
  }

  public boolean isActive() {
    return epoch > 0;
  }

  /**
   * Claim a pointer for the current epoch. If the pointer does not exist, a tombstone owned by the
   * current epoch is created, so an older leader can not create the pointer after this claim (its
   * create would succeed, acknowledging data outside this leader's lineage). Readers treat a
   * tombstone as absent, see {@link #isAbsent}.
   *
   * @param key pointer object key
   * @return current pointer value, or empty if it does not exist
   * @throws FencedException if not active, or the pointer has a newer epoch
   */
  public Optional<String> claim(String key) throws IOException {
    ClaimResult result = claimFor(key, requireActive(key), TOMBSTONE_VALUE);
    return result.created() ? Optional.empty() : result.previousValue();
  }

  /**
   * Result of claiming a pointer.
   *
   * @param created if the pointer did not exist and was created with the given value
   * @param previousValue value of the pointer before the claim, empty if absent or a tombstone
   */
  private record ClaimResult(boolean created, Optional<String> previousValue) {}

  /**
   * Claim a pointer for an epoch, creating it with {@code valueIfAbsent} if it does not exist.
   *
   * @param key pointer object key
   * @param claimEpoch epoch to claim for
   * @param valueIfAbsent value to create the pointer with if it does not exist
   * @throws FencedException if the pointer has a newer epoch
   */
  private ClaimResult claimFor(String key, long claimEpoch, String valueIfAbsent)
      throws IOException {
    for (int attempt = 0; attempt < MAX_CLAIM_ATTEMPTS; attempt++) {
      Optional<VersionedObject> current = store.get(key);
      if (current.isEmpty()) {
        try {
          String version =
              store.putIfAbsent(key, encode(valueIfAbsent, claimEpoch), epochMetadata(claimEpoch));
          recordOwned(key, version, claimEpoch);
          return new ClaimResult(true, Optional.empty());
        } catch (PreconditionFailedException e) {
          logger.info("Pointer {} created while claiming, retrying", key);
          continue;
        }
      }
      VersionedObject obj = current.get();
      String body = new String(obj.data(), StandardCharsets.UTF_8);
      long pointerEpoch = decodeEpoch(body);
      if (pointerEpoch > claimEpoch) {
        throw fenced(
            String.format(
                "Cannot claim %s for epoch %d, pointer has newer epoch %d",
                key, claimEpoch, pointerEpoch),
            claimEpoch);
      }
      String value = decodeValue(body);
      Optional<String> result = isAbsent(body) ? Optional.empty() : Optional.of(value);
      try {
        // the body changes (epoch, nonce), so the version changes and older owners are fenced
        String version =
            store.putIfMatch(
                key, encode(value, claimEpoch), epochMetadata(claimEpoch), obj.version());
        recordOwned(key, version, claimEpoch);
        return new ClaimResult(false, result);
      } catch (PreconditionFailedException e) {
        logger.info("Pointer {} changed while claiming, retrying", key);
      }
    }
    throw new IOException("Could not claim pointer " + key + " due to concurrent modification");
  }

  /**
   * Write a pointer value. The pointer must have been claimed (or not exist) for the current epoch.
   *
   * @param key pointer object key
   * @param value pointer value
   * @throws FencedException if not active, or another leader has modified the pointer
   */
  public void write(String key, String value) throws IOException {
    long writeEpoch = requireActive(key);
    if (value.contains("\n")) {
      throw new IllegalArgumentException("Pointer value must be a single line: " + value);
    }
    if (value.isEmpty()) {
      throw new IllegalArgumentException("Pointer value must not be empty");
    }
    byte[] data = encode(value, writeEpoch);
    Owned owned = ownedVersions.get(key);
    if (owned == null) {
      // first write of a pointer not claimed during promotion, e.g. for an index created by this
      // leader. Claim it now: this fails if a newer leader owns it, and otherwise makes this epoch
      // the owner, so an older leader can not create or move it. A pointer that does not exist yet
      // is created with the value directly, in a single request.
      if (claimFor(key, writeEpoch, value).created()) {
        return;
      }
      owned = ownedVersions.get(key);
      if (owned == null) {
        throw new FencedException(
            String.format("Pointer %s could not be claimed for epoch %d", key, writeEpoch),
            writeEpoch);
      }
    }
    if (owned.epoch() != writeEpoch) {
      // the epoch changed after this write started; the version belongs to a newer claim
      throw fenced(
          String.format(
              "Pointer %s write started in epoch %d, but fence is now at epoch %d",
              key, writeEpoch, owned.epoch()),
          writeEpoch);
    }
    try {
      String version = store.putIfMatch(key, data, epochMetadata(writeEpoch), owned.version());
      recordOwned(key, version, writeEpoch);
    } catch (PreconditionFailedException e) {
      ownedVersions.remove(key, owned);
      throw fenced(
          String.format(
              "Pointer %s was modified by another node, epoch %d is no longer leader",
              key, writeEpoch),
          writeEpoch);
    }
  }

  /** Record an owned version, only if the fence is still at the epoch it was written in. */
  private synchronized void recordOwned(String key, String version, long writtenEpoch) {
    if (epoch == writtenEpoch) {
      ownedVersions.put(key, new Owned(version, writtenEpoch));
    }
  }

  private long requireActive(String key) throws FencedException {
    long current = epoch;
    if (current <= 0) {
      throw new FencedException(
          "Cannot modify " + key + ", node is not the active primary", current);
    }
    return current;
  }

  private FencedException fenced(String message, long fencedEpoch) {
    FencedException e = new FencedException(message, fencedEpoch);
    logger.warn(message);
    Consumer<FencedException> listener = fencedListener.get();
    try {
      if (listener != null) {
        listener.accept(e);
      }
    } catch (Throwable t) {
      logger.warn("Error in fenced callback", t);
    }
    return e;
  }

  private static Map<String, String> epochMetadata(long epoch) {
    return Map.of(EPOCH_METADATA_KEY, Long.toString(epoch));
  }

  /** Value of a claimed pointer that has no resource yet. */
  static final String TOMBSTONE_VALUE = "";

  /**
   * If a pointer body refers to no resource: a tombstone created by a claim of an absent pointer.
   *
   * @param body pointer object content
   */
  public static boolean isAbsent(String body) {
    return decodeValue(body).isEmpty();
  }

  /** Encode a fenced pointer body. A random nonce makes every write produce distinct content. */
  static byte[] encode(String value, long epoch) {
    return (value + FENCE_LINE_PREFIX + "epoch=" + epoch + " nonce=" + UUID.randomUUID())
        .getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Get the pointer value from a pointer body, fenced or not.
   *
   * @param body pointer object content
   * @return pointer value
   */
  public static String decodeValue(String body) {
    int idx = body.indexOf(FENCE_LINE_PREFIX);
    return idx < 0 ? body : body.substring(0, idx);
  }

  /** Get the writer epoch from a pointer body, or 0 if unfenced (written without a fence). */
  public static long decodeEpoch(String body) {
    int idx = body.indexOf(FENCE_LINE_PREFIX);
    if (idx < 0) {
      return 0;
    }
    for (String part : body.substring(idx + FENCE_LINE_PREFIX.length()).trim().split(" ")) {
      if (part.startsWith("epoch=")) {
        try {
          return Long.parseLong(part.substring("epoch=".length()));
        } catch (NumberFormatException e) {
          throw new IllegalStateException("Malformed fence line in pointer: " + body, e);
        }
      }
    }
    throw new IllegalStateException("Malformed fence line in pointer, missing epoch: " + body);
  }
}
