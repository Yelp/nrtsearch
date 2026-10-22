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
package com.yelp.nrtsearch.server.monitoring;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.Unit;

/** Metrics for warming query backup, replay, and guardrails. */
public class WarmingMetrics {

  public static final Counter warmingBackupSuccessCount =
      Counter.builder()
          .name("nrt_warming_backup_success_count")
          .help("Number of warming query backups uploaded to the remote backend.")
          .labelNames("index")
          .build();

  public static final Counter warmingBackupFailureCount =
      Counter.builder()
          .name("nrt_warming_backup_failure_count")
          .help(
              "Number of failed warming query backups, by reason (below_uptime_threshold,"
                  + " below_query_threshold, upload_error, other).")
          .labelNames("index", "reason")
          .build();

  public static final Counter warmingQueryCount =
      Counter.builder()
          .name("nrt_warming_query_count")
          .help(
              "Number of queries replayed during startup warming, by type (full, basic) and outcome"
                  + " (success, error, timeout).")
          .labelNames("index", "type", "outcome")
          .build();

  public static final Histogram warmingQueryLatency =
      Histogram.builder()
          .name("nrt_warming_query_latency_seconds")
          .help("Latency of each query replayed during startup warming, by type (full, basic).")
          .labelNames("index", "type")
          .unit(Unit.SECONDS)
          .classicUpperBounds(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)
          .build();

  public static final Counter warmingQueriesFilteredCount =
      Counter.builder()
          .name("nrt_warming_query_filtered_count")
          .help(
              "Number of queries excluded from warming, by stage (collection, replay) and reason.")
          .labelNames("index", "stage", "reason")
          .build();

  public static final Counter warmingDurationCapReachedCount =
      Counter.builder()
          .name("nrt_warming_duration_cap_reached_count")
          .help("Number of times startup warming was cut short by the total warming duration cap.")
          .labelNames("index")
          .build();

  /**
   * Add all warming metrics to the collector registry.
   *
   * @param registry collector registry
   */
  public static void register(PrometheusRegistry registry) {
    registry.register(warmingBackupSuccessCount);
    registry.register(warmingBackupFailureCount);
    registry.register(warmingQueryCount);
    registry.register(warmingQueryLatency);
    registry.register(warmingQueriesFilteredCount);
    registry.register(warmingDurationCapReachedCount);
  }
}
