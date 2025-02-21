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

import com.yelp.nrtsearch.server.Version;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.Unit;

/**
 * Class for collecting the timing of the overall bootstrap time and the major components. The
 * timers shall only be updated during the bootstrap.
 */
public class BootstrapMetrics {

  public static final Gauge nrtsearchBootstrapTimer =
      Gauge.builder()
          .name("total_bootstrap_time_seconds")
          .help("timer to record the total bootstrap time.")
          .constLabels(Labels.of("nrtsearch_version", Version.CURRENT.toString()))
          .unit(Unit.SECONDS)
          .build();

  public static final Gauge pluginInitializationTimer =
      Gauge.builder()
          .name("plugin_initialization_time_seconds")
          .help("timer to record the boostrap time spent on plugin initialization.")
          .constLabels(Labels.of("nrtsearch_version", Version.CURRENT.toString()))
          .labelNames("plugin_name", "plugin_version")
          .unit(Unit.SECONDS)
          .build();

  public static final Gauge dataRestoreTimer =
      Gauge.builder()
          .name("data_restore_time_seconds")
          .help(
              "timer to record the boostrap time spent on restoring the stored data from local or remote source.")
          .constLabels(Labels.of("nrtsearch_version", Version.CURRENT.toString()))
          .labelNames("index", "unique_index_name")
          .unit(Unit.SECONDS)
          .build();

  public static final Gauge initialNRTTimer =
      Gauge.builder()
          .name("initial_nrt_time_seconds")
          .help("timer to record the boostrap time spent on initial nrt")
          .constLabels(Labels.of("nrtsearch_version", Version.CURRENT.toString()))
          .labelNames("index")
          .unit(Unit.SECONDS)
          .build();

  public static final Gauge warmingQueryTimer =
      Gauge.builder()
          .name("warming_time_seconds")
          .help("timer to record the boostrap time spent on plugin initialization.")
          .constLabels(Labels.of("nrtsearch_version", Version.CURRENT.toString()))
          .labelNames("service", "index")
          .unit(Unit.SECONDS)
          .build();

  /**
   * Add all bootstrap metrics to the collector registry.
   *
   * @param registry collector registry
   */
  public static void register(PrometheusRegistry registry) {
    registry.register(nrtsearchBootstrapTimer);
    registry.register(pluginInitializationTimer);
    registry.register(dataRestoreTimer);
    registry.register(initialNRTTimer);
    registry.register(warmingQueryTimer);
  }
}
