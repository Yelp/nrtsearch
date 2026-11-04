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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.leader.ConditionalObjectStore.PreconditionFailedException;
import com.yelp.nrtsearch.server.leader.ConditionalObjectStore.VersionedObject;
import com.yelp.nrtsearch.server.utils.JsonUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PrimaryElector} using a lease object in a {@link ConditionalObjectStore} (S3 with
 * conditional writes).
 *
 * <p>The lease holds the leader info, epoch, and ttl. The holder renews it with a compare-and-set
 * every poll interval. Other nodes watch the lease and take it over, incrementing the epoch, once
 * they have observed the same lease version unchanged for longer than ttl + safetyMargin.
 *
 * <p>Expiry is measured only on each node's own monotonic clock, never by comparing wall clock
 * times between hosts. The holder considers itself leader until {@code renewSendTime + ttl -
 * safetyMargin}, and a contender only takes over after seeing the lease unchanged for {@code ttl +
 * safetyMargin} from when it first observed that version, which is after the holder sent it. This
 * leaves at least {@code 2 * safetyMargin} between the holder giving up and a takeover, absent
 * extreme clock rate drift. Correctness does not depend on this; remote state writes are fenced
 * separately using the epoch.
 */
public class S3LeaseElector implements PrimaryElector {
  private static final Logger logger = LoggerFactory.getLogger(S3LeaseElector.class);
  public static final String TYPE = "s3_lease";
  public static final long DEFAULT_TTL_MS = 15_000;
  public static final long DEFAULT_SAFETY_MARGIN_MS = 2_000;

  /**
   * Lease object content.
   *
   * @param holder leader holding the lease
   * @param ttlMs lease ttl
   * @param released if the holder released the lease
   * @param renewal renewal counter, increments on every write; S3 derives the version (ETag) from
   *     the content, so the content must change on renewal for contenders to observe it
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Lease(
      @JsonProperty("holder") LeaderInfo holder,
      @JsonProperty("ttlMs") long ttlMs,
      @JsonProperty("released") boolean released,
      @JsonProperty("renewal") long renewal) {}

  private final ConditionalObjectStore store;
  private final String leaseKey;
  private final LeaderInfo localInfo;
  private final long ttlNanos;
  private final long safetyMarginNanos;
  private final long pollIntervalMs;
  private final LongSupplier nanoClock;
  private final LongSupplier wallClockMs;

  private final ExecutorService listenerExecutor =
      Executors.newSingleThreadExecutor(
          r -> {
            Thread t = new Thread(r, "S3LeaseElector-listener");
            t.setDaemon(true);
            return t;
          });
  private ScheduledExecutorService scheduler;
  private LeaderListener listener;

  // state, guarded by this
  /**
   * Held leadership and its validity deadline, published together so lock free readers never see a
   * holder paired with another holder's deadline.
   */
  private record Held(LeaderInfo info, long validUntilNanos) {}

  // non-null while this node holds the lease; written under the lock, read without it
  private volatile Held held;
  private String heldVersion;
  private volatile LeaderInfo lastKnownLeader;
  private String observedVersion;
  private long observedAtNanos;
  private long lastSeenEpoch = 0;
  private long renewalCount = 0;
  private long acquireBlockedUntilNanos = Long.MIN_VALUE;
  private boolean closed = false;

  /**
   * Create elector from params.
   *
   * @param store conditional object store
   * @param context elector context
   * @param params params: ttlMs, safetyMarginMs, pollIntervalMs, leaseKey
   */
  public static S3LeaseElector fromParams(
      ConditionalObjectStore store, PrimaryElectorContext context, Map<String, Object> params) {
    long ttlMs = getLong(params, "ttlMs", DEFAULT_TTL_MS);
    long safetyMarginMs = getLong(params, "safetyMarginMs", DEFAULT_SAFETY_MARGIN_MS);
    long pollIntervalMs = getLong(params, "pollIntervalMs", ttlMs / 3);
    Object keyParam = params.get("leaseKey");
    String leaseKey =
        keyParam != null ? keyParam.toString() : defaultLeaseKey(context.serviceName());
    return new S3LeaseElector(
        store,
        leaseKey,
        context.localInfo(),
        ttlMs,
        safetyMarginMs,
        pollIntervalMs,
        System::nanoTime);
  }

  /** Default object key for the lease of a service. */
  public static String defaultLeaseKey(String serviceName) {
    return serviceName + "/leader/lease";
  }

  @VisibleForTesting
  public S3LeaseElector(
      ConditionalObjectStore store,
      String leaseKey,
      LeaderInfo localInfo,
      long ttlMs,
      long safetyMarginMs,
      long pollIntervalMs,
      LongSupplier nanoClock) {
    if (ttlMs <= 0 || safetyMarginMs < 0 || pollIntervalMs <= 0) {
      throw new IllegalArgumentException(
          "ttlMs and pollIntervalMs must be > 0, safetyMarginMs >= 0");
    }
    if (2 * safetyMarginMs >= ttlMs) {
      throw new IllegalArgumentException("safetyMarginMs must be less than half of ttlMs");
    }
    if (pollIntervalMs >= ttlMs - safetyMarginMs) {
      throw new IllegalArgumentException("pollIntervalMs must be less than ttlMs - safetyMarginMs");
    }
    this.store = Objects.requireNonNull(store);
    this.leaseKey = Objects.requireNonNull(leaseKey);
    this.localInfo = Objects.requireNonNull(localInfo);
    this.ttlNanos = TimeUnit.MILLISECONDS.toNanos(ttlMs);
    this.safetyMarginNanos = TimeUnit.MILLISECONDS.toNanos(safetyMarginMs);
    this.pollIntervalMs = pollIntervalMs;
    this.nanoClock = nanoClock;
    this.wallClockMs = System::currentTimeMillis;
  }

  @Override
  public synchronized void start(LeaderListener listener) {
    startManual(listener);
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "S3LeaseElector");
              t.setDaemon(true);
              return t;
            });
    scheduler.scheduleWithFixedDelay(this::safeTick, 0, pollIntervalMs, TimeUnit.MILLISECONDS);
  }

  /** Register listener without scheduling the background loop; tests call {@link #tick()}. */
  @VisibleForTesting
  synchronized void startManual(LeaderListener listener) {
    if (this.listener != null) {
      throw new IllegalStateException("Elector already started");
    }
    this.listener = Objects.requireNonNull(listener);
  }

  private void safeTick() {
    try {
      tick();
    } catch (Throwable t) {
      logger.warn("Error in lease elector loop", t);
    }
  }

  /** Run one iteration of the lease loop. */
  @VisibleForTesting
  synchronized void tick() throws IOException {
    if (closed) {
      return;
    }
    if (held != null) {
      renew();
    } else {
      maybeAcquire();
    }
  }

  private void renew() throws IOException {
    long sendNanos = nanoClock.getAsLong();
    Held current = held;
    if (sendNanos >= current.validUntilNanos()) {
      // could not renew in time (e.g. long pause), leadership may already have been taken
      lose("lease validity expired before renewal");
      return;
    }
    try {
      heldVersion =
          writeLease(new Lease(current.info(), ttlMillis(), false, ++renewalCount), heldVersion);
      held = new Held(current.info(), sendNanos + ttlNanos - safetyMarginNanos);
    } catch (PreconditionFailedException e) {
      lose("lease was modified by another node");
    } catch (IOException e) {
      logger.warn("Failed to renew lease, will retry", e);
      if (nanoClock.getAsLong() >= current.validUntilNanos()) {
        lose("lease validity expired while renewal failing");
      }
    }
  }

  private void maybeAcquire() throws IOException {
    Optional<VersionedObject> current = store.get(leaseKey);
    long now = nanoClock.getAsLong();
    if (now < acquireBlockedUntilNanos) {
      // recently resigned, only observe the lease
      current.ifPresent(this::observeOnly);
      return;
    }
    if (current.isEmpty()) {
      // the first epoch must be newer than any existing primaryGen, which before leader election
      // was the primary start time in ms
      tryAcquire(null, Math.max(lastSeenEpoch + 1, wallClockMs.getAsLong()), now);
      return;
    }
    VersionedObject obj = current.get();
    Lease lease = parse(obj.data());
    lastSeenEpoch = Math.max(lastSeenEpoch, lease.holder().epoch());
    lastKnownLeader = lease.released() ? null : lease.holder();
    if (!obj.version().equals(observedVersion)) {
      observedVersion = obj.version();
      observedAtNanos = now;
      if (!lease.released()) {
        return;
      }
    }
    long leaseTtlNanos = TimeUnit.MILLISECONDS.toNanos(lease.ttlMs());
    if (lease.released() || now - observedAtNanos > leaseTtlNanos + safetyMarginNanos) {
      tryAcquire(obj.version(), lease.holder().epoch() + 1, now);
    }
  }

  private void tryAcquire(String expectedVersion, long epoch, long sendNanos) throws IOException {
    LeaderInfo info = localInfo.withEpoch(epoch);
    try {
      heldVersion =
          writeLease(new Lease(info, ttlMillis(), false, ++renewalCount), expectedVersion);
    } catch (PreconditionFailedException e) {
      logger.info("Lost race to acquire lease at epoch {}", epoch);
      return;
    }
    held = new Held(info, sendNanos + ttlNanos - safetyMarginNanos);
    lastSeenEpoch = epoch;
    lastKnownLeader = info;
    observedVersion = heldVersion;
    logger.info("Acquired leader lease {} at epoch {}", leaseKey, epoch);
    LeaderListener l = listener;
    listenerExecutor.execute(() -> l.onElected(info));
  }

  private void lose(String reason) {
    LeaderInfo lost = held.info();
    logger.warn("Lost leader lease at epoch {}: {}", lost.epoch(), reason);
    held = null;
    heldVersion = null;
    observedVersion = null;
    LeaderListener l = listener;
    listenerExecutor.execute(() -> l.onLost(lost));
  }

  private String writeLease(Lease lease, String expectedVersion) throws IOException {
    byte[] data = JsonUtils.objectToJsonStr(lease).getBytes(StandardCharsets.UTF_8);
    Map<String, String> meta = Map.of("nrt-epoch", Long.toString(lease.holder().epoch()));
    return expectedVersion == null
        ? store.putIfAbsent(leaseKey, data, meta)
        : store.putIfMatch(leaseKey, data, meta, expectedVersion);
  }

  private static Lease parse(byte[] data) throws IOException {
    return JsonUtils.readValue(new String(data, StandardCharsets.UTF_8), Lease.class);
  }

  private long ttlMillis() {
    return TimeUnit.NANOSECONDS.toMillis(ttlNanos);
  }

  @Override
  public Optional<LeaderInfo> currentLeader() {
    return Optional.ofNullable(lastKnownLeader);
  }

  /** Held leadership if still valid, read lock free so request paths never wait on lease I/O. */
  private Held validHeld() {
    Held current = held;
    return current != null && nanoClock.getAsLong() < current.validUntilNanos() ? current : null;
  }

  @Override
  public boolean isLeader() {
    return validHeld() != null;
  }

  /** Epoch of held leadership, or -1 if not leader. */
  @VisibleForTesting
  long getLeaderEpoch() {
    Held current = validHeld();
    return current != null ? current.info().epoch() : -1;
  }

  private void observeOnly(VersionedObject obj) {
    try {
      Lease lease = parse(obj.data());
      lastSeenEpoch = Math.max(lastSeenEpoch, lease.holder().epoch());
      lastKnownLeader = lease.released() ? null : lease.holder();
    } catch (IOException e) {
      logger.warn("Could not parse lease", e);
    }
  }

  @Override
  public synchronized void resign(long epoch) {
    Held current = held;
    if (current == null || current.info().epoch() != epoch) {
      return;
    }
    releaseHeld(current);
  }

  /** Release held leadership. Must hold the lock. */
  private void releaseHeld(Held current) {
    // give other nodes the first chance to take over; otherwise the next tick would immediately
    // re-acquire the released lease
    acquireBlockedUntilNanos = nanoClock.getAsLong() + ttlNanos;
    LeaderInfo held = current.info();
    try {
      // keep the epoch in the lease so the next leader increments it
      writeLease(new Lease(held, ttlMillis(), true, ++renewalCount), heldVersion);
    } catch (IOException e) {
      logger.warn("Failed to release lease, it will expire after ttl", e);
    }
    lose("resigned");
  }

  @Override
  public void close() {
    synchronized (this) {
      // set before releasing, so a tick can not re-acquire the released lease
      closed = true;
      Held current = held;
      if (current != null) {
        releaseHeld(current);
      }
    }
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
    listenerExecutor.shutdown();
  }

  /** Wait for queued listener callbacks to run, for tests. */
  @VisibleForTesting
  void drainListener() throws Exception {
    listenerExecutor.submit(() -> {}).get(10, TimeUnit.SECONDS);
  }

  private static long getLong(Map<String, Object> params, String key, long def) {
    Object v = params.get(key);
    if (v == null) {
      return def;
    }
    if (v instanceof Number n) {
      return n.longValue();
    }
    return Long.parseLong(v.toString());
  }
}
