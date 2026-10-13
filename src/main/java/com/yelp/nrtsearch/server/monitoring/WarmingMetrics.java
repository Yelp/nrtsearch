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
import io.prometheus.metrics.model.registry.PrometheusRegistry;

/** Metrics for the warming query guardrails. */
public class WarmingMetrics {

  public static final Counter warmingQueryRejectedCount =
      Counter.builder()
          .name("nrt_warming_query_rejected_count")
          .help(
              "Number of queries excluded from warming, by stage (collection, backup, replay) and reason.")
          .labelNames("index", "stage", "reason")
          .build();

  public static final Counter warmingDurationCapExceededCount =
      Counter.builder()
          .name("nrt_warming_duration_cap_exceeded_count")
          .help("Number of times startup warming was cut short by the total warming duration cap.")
          .labelNames("index")
          .build();

  /**
   * Add all warming metrics to the collector registry.
   *
   * @param registry collector registry
   */
  public static void register(PrometheusRegistry registry) {
    registry.register(warmingQueryRejectedCount);
    registry.register(warmingDurationCapExceededCount);
  }
}
