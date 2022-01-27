/*
 * Copyright 2022 Yelp Inc.
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

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;

public class DeadlineMetrics {

  public static final Counter nrtDeadlineCancelCount =
      Counter.build()
          .name("nrt_deadline_cancel_count")
          .help("Number of requests canceled from expired deadlines.")
          .labelNames("operation")
          .create();

  /**
   * Add all deadline metrics to the collector registry.
   *
   * @param registry collector registry
   */
  public static void register(CollectorRegistry registry) {
    registry.register(nrtDeadlineCancelCount);
  }
}
