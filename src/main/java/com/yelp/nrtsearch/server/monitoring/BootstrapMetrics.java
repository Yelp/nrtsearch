/*
 * Copyright 2025 Yelp Inc.
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
import io.prometheus.client.Gauge;

/**
 * Class for collecting the timing of the overall bootstrap time and the major components. The
 * timers shall only be updated during the bootstrap.
 */
public class BootstrapMetrics {

  public static final Gauge nrtsearchBootstrapTimer =
      Gauge.build()
          .name("total_bootstrap_time_seconds")
          .help("timer to record the total bootstrap time.")
          .labelNames("nrtsearch_version")
          .create();

  public static final Gauge pluginInitializationTimer =
      Gauge.build()
          .name("plugin_initialization_time_second")
          .help("timer to record the boostrap time spent on plugin initialization.")
          .labelNames("plugin_name", "plugin_version", "nrtsearch_version")
          .create();

  public static final Gauge dataRestoreTimer =
      Gauge.build()
          .name("data_restore_time_second")
          .help(
              "timer to record the boostrap time spent on restoring the stored data from local or remote source.")
          .labelNames("index", "nrtsearch_version")
          .create();

  public static final Gauge initialNRTTimer =
      Gauge.build()
          .name("initial_nrt_time_second")
          .help("timer to record the boostrap time spent on initial nrt")
          .labelNames("index", "nrtsearch_version")
          .create();

  public static final Gauge warmingQueryTimer =
      Gauge.build()
          .name("warming_time_seconds")
          .help("timer to record the boostrap time spent on plugin initialization.")
          .labelNames("service", "resource", "index", "nrtsearch_version")
          .create();

  /**
   * Add all bootstrap metrics to the collector registry.
   *
   * @param registry collector registry
   */
  public static void register(CollectorRegistry registry) {
    registry.register(nrtsearchBootstrapTimer);
    registry.register(pluginInitializationTimer);
    registry.register(dataRestoreTimer);
    registry.register(initialNRTTimer);
    registry.register(warmingQueryTimer);
  }
}
