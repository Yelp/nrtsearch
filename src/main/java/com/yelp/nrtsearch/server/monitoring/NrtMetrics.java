/*
 * Copyright 2020 Yelp Inc.
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
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;

/**
 * Class for managing collection of nrt related metrics. Collects metrics on publishing of new nrt
 * points and pre copying merged segments.
 */
public class NrtMetrics {
  public static final Gauge searcherVersion =
      Gauge.build()
          .name("nrt_searcher_version")
          .help("Current searcher version.")
          .labelNames("index")
          .create();

  public static final Counter nrtPrimaryPointCount =
      Counter.build()
          .name("nrt_primary_point_count")
          .help("Number of nrt points created on the primary.")
          .labelNames("index")
          .create();
  public static final Summary nrtPrimaryMergeTime =
      Summary.build()
          .name("nrt_primary_merge_time_ms")
          .help("Time to copy data for merge (ms).")
          .quantile(0.5, 0.05)
          .quantile(0.95, 0.01)
          .quantile(0.99, 0.01)
          .labelNames("index")
          .create();

  public static final Counter nrtPointFailure =
      Counter.build()
          .name("nrt_point_failure_count")
          .help("Number of failed nrt point copies")
          .labelNames("index")
          .create();
  public static final Summary nrtPointSize =
      Summary.build()
          .name("nrt_point_copy_size")
          .help("Data copied for nrt points.")
          .quantile(0.5, 0.05)
          .quantile(0.95, 0.01)
          .quantile(0.99, 0.01)
          .labelNames("index")
          .create();
  public static final Summary nrtPointTime =
      Summary.build()
          .name("nrt_point_copy_time_ms")
          .help("Time to copy data for nrt point (ms).")
          .quantile(0.5, 0.05)
          .quantile(0.95, 0.01)
          .quantile(0.99, 0.01)
          .labelNames("index")
          .create();

  public static final Counter nrtMergeFailure =
      Counter.build()
          .name("nrt_merge_failure_count")
          .help("Number of failed merge copies.")
          .labelNames("index")
          .create();
  public static final Summary nrtMergeSize =
      Summary.build()
          .name("nrt_merge_copy_size")
          .help("Data copied for merges.")
          .quantile(0.5, 0.05)
          .quantile(0.95, 0.01)
          .quantile(0.99, 0.01)
          .labelNames("index")
          .create();
  public static final Summary nrtMergeTime =
      Summary.build()
          .name("nrt_merge_copy_time_ms")
          .help("Time to copy data for merge (ms).")
          .quantile(0.5, 0.05)
          .quantile(0.95, 0.01)
          .quantile(0.99, 0.01)
          .labelNames("index")
          .create();

  public static final Counter nrtMergeCopyStartCount =
      Counter.build()
          .name("nrt_merge_copy_start_count")
          .help("Number of merge copies started")
          .labelNames("index")
          .create();

  public static final Counter nrtMergeCopyEndCount =
      Counter.build()
          .name("nrt_merge_copy_end_count")
          .help("Number of merge copies ended")
          .labelNames("index")
          .create();

  /**
   * Add all nrt metrics to the collector registry.
   *
   * @param registry collector registry
   */
  public static void register(CollectorRegistry registry) {
    registry.register(searcherVersion);
    registry.register(nrtPrimaryPointCount);
    registry.register(nrtPrimaryMergeTime);
    registry.register(nrtPointFailure);
    registry.register(nrtPointSize);
    registry.register(nrtPointTime);
    registry.register(nrtMergeFailure);
    registry.register(nrtMergeSize);
    registry.register(nrtMergeTime);
    registry.register(nrtMergeCopyStartCount);
    registry.register(nrtMergeCopyEndCount);
  }
}
