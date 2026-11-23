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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.monitoring.WarmingMetrics;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks whether live queries still match the template library, and triggers a relearn when they
 * stop matching, for example after a client changes its requests.
 *
 * <p>One in every {@code checkEvery} collected queries is checked: its template key is computed and
 * looked up in the library. After {@code window} checks, the share that matched is published as
 * {@code nrt_warming_template_match_ratio}. If it is below {@code threshold}, and the last relearn
 * started at least {@code cooldownMs} ago, the relearn task runs on a background thread. Only one
 * relearn runs at a time, and searches never wait for it.
 */
public class TemplateDriftMonitor {
  private static final Logger logger = LoggerFactory.getLogger(TemplateDriftMonitor.class);

  /** Learns templates from recent traffic, merges them into the library and stores it. */
  @FunctionalInterface
  public interface Relearner {
    /**
     * @param matchRatio match ratio that triggered the relearn
     * @return the keys of the new library, or null if nothing was relearned
     */
    Set<String> relearn(double matchRatio) throws Exception;
  }

  private final String index;
  private final int checkEvery;
  private final int window;
  private final double threshold;
  private final long cooldownMs;
  private final Relearner relearner;
  private final LongSupplier clock;
  private final ExecutorService executor;
  private final AtomicLong seen = new AtomicLong();
  private final AtomicBoolean relearning = new AtomicBoolean();
  private volatile Set<String> keys;
  private long checked;
  private long matched;
  private long lastRelearnStartMs = Long.MIN_VALUE / 2;
  private volatile double lastMatchRatio = Double.NaN;

  public TemplateDriftMonitor(
      String index,
      int checkEvery,
      int window,
      double threshold,
      long cooldownMs,
      Relearner relearner) {
    this(
        index,
        checkEvery,
        window,
        threshold,
        cooldownMs,
        relearner,
        System::currentTimeMillis,
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("template-relearn-" + index + "-%d")
                .setDaemon(true)
                .build()));
  }

  TemplateDriftMonitor(
      String index,
      int checkEvery,
      int window,
      double threshold,
      long cooldownMs,
      Relearner relearner,
      LongSupplier clock,
      ExecutorService executor) {
    if (checkEvery <= 0 || window <= 0 || threshold <= 0 || threshold > 1) {
      throw new IllegalArgumentException(
          "Invalid template drift settings: checkEvery and window must be positive, threshold in"
              + " (0, 1]");
    }
    this.index = index;
    this.checkEvery = checkEvery;
    this.window = window;
    this.threshold = threshold;
    this.cooldownMs = cooldownMs;
    this.relearner = relearner;
    this.clock = clock;
    this.executor = executor;
  }

  /** Start checking against the given library keys, or stop checking when null. */
  public void setLibraryKeys(Set<String> keys) {
    synchronized (this) {
      checked = 0;
      matched = 0;
    }
    this.keys = keys;
  }

  public double getLastMatchRatio() {
    return lastMatchRatio;
  }

  public boolean isRelearning() {
    return relearning.get();
  }

  /** Called for each collected live query. Cheap unless the query is one of the checked ones. */
  public void observe(SearchRequest request) {
    Set<String> current = keys;
    if (current == null || seen.incrementAndGet() % checkEvery != 0) {
      return;
    }
    String key = TemplateWarmingQueryGenerator.templateKey(request);
    if (key == null) {
      return;
    }
    boolean hit = current.contains(key);
    double ratio;
    synchronized (this) {
      checked++;
      if (hit) {
        matched++;
      }
      if (checked < window) {
        return;
      }
      ratio = (double) matched / checked;
      checked = 0;
      matched = 0;
    }
    lastMatchRatio = ratio;
    WarmingMetrics.warmingTemplateMatchRatio.labelValues(index).set(ratio);
    if (ratio >= threshold) {
      return;
    }
    long now = clock.getAsLong();
    synchronized (this) {
      if (now - lastRelearnStartMs < cooldownMs) {
        return;
      }
      if (!relearning.compareAndSet(false, true)) {
        return;
      }
      lastRelearnStartMs = now;
    }
    logger.info(
        "Template match ratio for index: {} is {}, below {}, relearning templates",
        index,
        ratio,
        threshold);
    executor.submit(
        () -> {
          try {
            Set<String> newKeys = relearner.relearn(ratio);
            if (newKeys != null) {
              setLibraryKeys(newKeys);
              WarmingMetrics.warmingTemplateRelearnCount.labelValues(index, "success").inc();
            } else {
              WarmingMetrics.warmingTemplateRelearnCount.labelValues(index, "skipped").inc();
            }
          } catch (Exception e) {
            logger.warn("Failed to relearn warming templates for index: {}", index, e);
            WarmingMetrics.warmingTemplateRelearnCount.labelValues(index, "failure").inc();
          } finally {
            relearning.set(false);
          }
        });
  }

  public void close() {
    executor.shutdownNow();
  }
}
