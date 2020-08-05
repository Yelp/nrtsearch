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

/**
 * Holds information about which metrics should be kept track of during rpc calls. Can be used to
 * turn on more elaborate and expensive metrics, such as latency histograms.
 */
public class Configuration {
  private static double[] DEFAULT_LATENCY_BUCKETS =
      new double[] {.001, .005, .01, .05, 0.075, .1, .25, .5, 1, 2, 5, 10};

  private final boolean isIncludeLatencyHistograms;
  private final CollectorRegistry collectorRegistry;
  private final double[] latencyBuckets;

  /**
   * Returns a {@link com.yelp.nrtsearch.server.monitoring.Configuration} for recording all cheap
   * metrics about the rpcs.
   */
  public static com.yelp.nrtsearch.server.monitoring.Configuration cheapMetricsOnly() {
    return new com.yelp.nrtsearch.server.monitoring.Configuration(
        false /* isIncludeLatencyHistograms */,
        CollectorRegistry.defaultRegistry,
        DEFAULT_LATENCY_BUCKETS);
  }

  /**
   * Returns a {@link com.yelp.nrtsearch.server.monitoring.Configuration} for recording all metrics
   * about the rpcs. This includes metrics which might produce a lot of data, such as latency
   * histograms.
   */
  public static com.yelp.nrtsearch.server.monitoring.Configuration allMetrics() {
    return new com.yelp.nrtsearch.server.monitoring.Configuration(
        true /* isIncludeLatencyHistograms */,
        CollectorRegistry.defaultRegistry,
        DEFAULT_LATENCY_BUCKETS);
  }

  /**
   * Returns a copy {@link com.yelp.nrtsearch.server.monitoring.Configuration} with the difference
   * that Prometheus metrics are recorded using the supplied {@link CollectorRegistry}.
   */
  public com.yelp.nrtsearch.server.monitoring.Configuration withCollectorRegistry(
      CollectorRegistry collectorRegistry) {
    return new com.yelp.nrtsearch.server.monitoring.Configuration(
        isIncludeLatencyHistograms, collectorRegistry, latencyBuckets);
  }

  /**
   * Returns a copy {@link com.yelp.nrtsearch.server.monitoring.Configuration} with the difference
   * that the latency histogram values are recorded with the specified set of buckets.
   */
  public com.yelp.nrtsearch.server.monitoring.Configuration withLatencyBuckets(double[] buckets) {
    return new com.yelp.nrtsearch.server.monitoring.Configuration(
        isIncludeLatencyHistograms, collectorRegistry, buckets);
  }

  /** Returns whether or not latency histograms for calls should be included. */
  public boolean isIncludeLatencyHistograms() {
    return isIncludeLatencyHistograms;
  }

  /** Returns the {@link CollectorRegistry} used to record stats. */
  public CollectorRegistry getCollectorRegistry() {
    return collectorRegistry;
  }

  /** Returns the histogram buckets to use for latency metrics. */
  public double[] getLatencyBuckets() {
    return latencyBuckets;
  }

  private Configuration(
      boolean isIncludeLatencyHistograms,
      CollectorRegistry collectorRegistry,
      double[] latencyBuckets) {
    this.isIncludeLatencyHistograms = isIncludeLatencyHistograms;
    this.collectorRegistry = collectorRegistry;
    this.latencyBuckets = latencyBuckets;
  }
}
