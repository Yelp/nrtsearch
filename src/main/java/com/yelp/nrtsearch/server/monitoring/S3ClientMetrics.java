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

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Summary;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

/**
 * Prometheus {@link MetricPublisher} implementation that translates AWS SDK v2 S3 client metrics
 * into Prometheus metrics. Captures per-operation latency, error rates, retry counts, and HTTP
 * connection pool state.
 *
 * <p>Register with the {@link PrometheusRegistry} via {@link #register(PrometheusRegistry)} and
 * attach to S3 clients via {@link
 * software.amazon.awssdk.core.client.config.ClientOverrideConfiguration#addMetricPublisher}.
 *
 * <p>The singleton instance {@link #getInstance()} is shared across sync and async S3 clients so
 * connection pool gauges reflect a single consistent view.
 */
public class S3ClientMetrics implements MetricPublisher {
  private static final Logger logger = LoggerFactory.getLogger(S3ClientMetrics.class);

  private static final S3ClientMetrics INSTANCE = new S3ClientMetrics();

  // --- Counters ---

  public static final Counter apiCallTotal =
      Counter.builder()
          .name("nrt_s3_api_call_total")
          .help("Total number of S3 API calls.")
          .labelNames("operation")
          .build();

  public static final Counter apiCallErrorTotal =
      Counter.builder()
          .name("nrt_s3_api_call_error_total")
          .help("Total number of failed S3 API calls.")
          .labelNames("operation", "error_type")
          .build();

  public static final Counter retryTotal =
      Counter.builder()
          .name("nrt_s3_retry_total")
          .help("Total number of S3 API call retry attempts.")
          .labelNames("operation")
          .build();

  // --- Summaries (p50/p95/p99) ---

  public static final Summary apiCallDuration =
      Summary.builder()
          .name("nrt_s3_api_call_duration_ms")
          .help("End-to-end duration of S3 API calls in milliseconds.")
          .labelNames("operation")
          .quantile(0.5, 0.05)
          .quantile(0.95, 0.01)
          .quantile(0.99, 0.01)
          .build();

  public static final Summary timeToFirstByte =
      Summary.builder()
          .name("nrt_s3_time_to_first_byte_ms")
          .help("Time to first byte for S3 API call attempts in milliseconds.")
          .labelNames("operation")
          .quantile(0.5, 0.05)
          .quantile(0.95, 0.01)
          .quantile(0.99, 0.01)
          .build();

  public static final Summary concurrencyAcquireDuration =
      Summary.builder()
          .name("nrt_s3_concurrency_acquire_duration_ms")
          .help(
              "Time to acquire a connection from the HTTP connection pool for S3 API calls, in"
                  + " milliseconds.")
          .labelNames("operation")
          .quantile(0.5, 0.05)
          .quantile(0.95, 0.01)
          .quantile(0.99, 0.01)
          .build();

  // --- Gauges (connection pool, last-seen values) ---

  public static final Gauge connectionPoolAvailable =
      Gauge.builder()
          .name("nrt_s3_connection_pool_available")
          .help("Number of available connections in the S3 HTTP connection pool.")
          .build();

  public static final Gauge connectionPoolLeased =
      Gauge.builder()
          .name("nrt_s3_connection_pool_leased")
          .help("Number of leased (in-use) connections in the S3 HTTP connection pool.")
          .build();

  public static final Gauge connectionPoolPending =
      Gauge.builder()
          .name("nrt_s3_connection_pool_pending")
          .help("Number of requests waiting to acquire a connection from the S3 connection pool.")
          .build();

  public static final Gauge connectionPoolMax =
      Gauge.builder()
          .name("nrt_s3_connection_pool_max")
          .help("Maximum number of connections in the S3 HTTP connection pool.")
          .build();

  private S3ClientMetrics() {}

  public static S3ClientMetrics getInstance() {
    return INSTANCE;
  }

  public static void register(PrometheusRegistry registry) {
    registry.register(apiCallTotal);
    registry.register(apiCallErrorTotal);
    registry.register(retryTotal);
    registry.register(apiCallDuration);
    registry.register(timeToFirstByte);
    registry.register(concurrencyAcquireDuration);
    registry.register(connectionPoolAvailable);
    registry.register(connectionPoolLeased);
    registry.register(connectionPoolPending);
    registry.register(connectionPoolMax);
  }

  @Override
  public void publish(MetricCollection metricCollection) {
    try {
      List<String> operationNames = metricCollection.metricValues(CoreMetric.OPERATION_NAME);
      String operation = operationNames.isEmpty() ? "Unknown" : operationNames.get(0);

      apiCallTotal.labelValues(operation).inc();

      List<Boolean> successValues = metricCollection.metricValues(CoreMetric.API_CALL_SUCCESSFUL);
      if (!successValues.isEmpty() && Boolean.FALSE.equals(successValues.get(0))) {
        List<String> errorTypes = metricCollection.metricValues(CoreMetric.ERROR_TYPE);
        String errorType = errorTypes.isEmpty() ? "Unknown" : errorTypes.get(0);
        apiCallErrorTotal.labelValues(operation, errorType).inc();
      }

      List<Integer> retryCounts = metricCollection.metricValues(CoreMetric.RETRY_COUNT);
      if (!retryCounts.isEmpty() && retryCounts.get(0) > 0) {
        retryTotal.labelValues(operation).inc(retryCounts.get(0));
      }

      List<java.time.Duration> callDurations =
          metricCollection.metricValues(CoreMetric.API_CALL_DURATION);
      if (!callDurations.isEmpty()) {
        apiCallDuration
            .labelValues(operation)
            .observe(callDurations.get(0).toNanos() / 1_000_000.0);
      }

      List<MetricCollection> attempts = metricCollection.children();
      for (MetricCollection attempt : attempts) {
        List<java.time.Duration> ttfbValues = attempt.metricValues(CoreMetric.TIME_TO_FIRST_BYTE);
        if (!ttfbValues.isEmpty()) {
          timeToFirstByte.labelValues(operation).observe(ttfbValues.get(0).toNanos() / 1_000_000.0);
        }

        List<java.time.Duration> acquireDurations =
            attempt.metricValues(HttpMetric.CONCURRENCY_ACQUIRE_DURATION);
        if (!acquireDurations.isEmpty()) {
          concurrencyAcquireDuration
              .labelValues(operation)
              .observe(acquireDurations.get(0).toNanos() / 1_000_000.0);
        }
      }

      // Update connection pool gauges from the last attempt (most current snapshot)
      if (!attempts.isEmpty()) {
        MetricCollection lastAttempt = attempts.get(attempts.size() - 1);

        List<Integer> available = lastAttempt.metricValues(HttpMetric.AVAILABLE_CONCURRENCY);
        if (!available.isEmpty()) {
          connectionPoolAvailable.set(available.get(0));
        }

        List<Integer> leased = lastAttempt.metricValues(HttpMetric.LEASED_CONCURRENCY);
        if (!leased.isEmpty()) {
          connectionPoolLeased.set(leased.get(0));
        }

        List<Integer> pending = lastAttempt.metricValues(HttpMetric.PENDING_CONCURRENCY_ACQUIRES);
        if (!pending.isEmpty()) {
          connectionPoolPending.set(pending.get(0));
        }

        List<Integer> max = lastAttempt.metricValues(HttpMetric.MAX_CONCURRENCY);
        if (!max.isEmpty()) {
          connectionPoolMax.set(max.get(0));
        }
      }
    } catch (Exception e) {
      logger.warn("Failed to publish S3 client metrics", e);
    }
  }

  @Override
  public void close() {
    // no-op: metrics lifecycle is managed by the PrometheusRegistry
  }
}
