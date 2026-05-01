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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import io.prometheus.metrics.model.snapshots.SummarySnapshot;
import java.time.Duration;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricCollector;

public class S3ClientMetricsTest {

  @Before
  public void setUp() {
    // Clear all static metric state between tests
    S3ClientMetrics.apiCallTotal.clear();
    S3ClientMetrics.apiCallErrorTotal.clear();
    S3ClientMetrics.retryTotal.clear();
    S3ClientMetrics.apiCallDuration.clear();
    S3ClientMetrics.timeToFirstByte.clear();
    S3ClientMetrics.concurrencyAcquireDuration.clear();
    S3ClientMetrics.connectionPoolAvailable.set(0);
    S3ClientMetrics.connectionPoolLeased.set(0);
    S3ClientMetrics.connectionPoolPending.set(0);
    S3ClientMetrics.connectionPoolMax.set(0);
  }

  /** Returns the first SummaryDataPointSnapshot for the given operation label. */
  private static SummarySnapshot.SummaryDataPointSnapshot getSummaryDataPoint(
      io.prometheus.metrics.core.metrics.Summary summary, String operation) {
    SummarySnapshot snapshot = summary.collect();
    return snapshot.getDataPoints().stream()
        .filter(dp -> operation.equals(dp.getLabels().get("operation")))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No data point for operation: " + operation));
  }

  private static MetricCollection buildApiCallCollection(
      String operation, boolean success, int retryCount, Duration callDuration) {
    MetricCollector collector = MetricCollector.create("ApiCall");
    collector.reportMetric(CoreMetric.OPERATION_NAME, operation);
    collector.reportMetric(CoreMetric.API_CALL_SUCCESSFUL, success);
    collector.reportMetric(CoreMetric.RETRY_COUNT, retryCount);
    if (callDuration != null) {
      collector.reportMetric(CoreMetric.API_CALL_DURATION, callDuration);
    }
    return collector.collect();
  }

  private static MetricCollection buildApiCallCollectionWithError(
      String operation, String errorType) {
    MetricCollector collector = MetricCollector.create("ApiCall");
    collector.reportMetric(CoreMetric.OPERATION_NAME, operation);
    collector.reportMetric(CoreMetric.API_CALL_SUCCESSFUL, false);
    collector.reportMetric(CoreMetric.RETRY_COUNT, 0);
    collector.reportMetric(CoreMetric.ERROR_TYPE, errorType);
    return collector.collect();
  }

  @Test
  public void testSuccessfulApiCallIncrementsCounter() {
    MetricCollection collection = buildApiCallCollection("GetObject", true, 0, null);
    S3ClientMetrics.getInstance().publish(collection);

    assertEquals(1.0, S3ClientMetrics.apiCallTotal.labelValues("GetObject").get(), 0.001);
    assertEquals(0.0, S3ClientMetrics.retryTotal.labelValues("GetObject").get(), 0.001);
  }

  @Test
  public void testApiCallDurationObserved() {
    MetricCollection collection =
        buildApiCallCollection("PutObject", true, 0, Duration.ofMillis(150));
    S3ClientMetrics.getInstance().publish(collection);

    SummarySnapshot.SummaryDataPointSnapshot dp =
        getSummaryDataPoint(S3ClientMetrics.apiCallDuration, "PutObject");
    assertEquals(1, dp.getCount());
    assertEquals(150.0, dp.getSum(), 0.001);
  }

  @Test
  public void testFailedApiCallIncrementsErrorCounter() {
    MetricCollection collection = buildApiCallCollectionWithError("GetObject", "THROTTLING");
    S3ClientMetrics.getInstance().publish(collection);

    assertEquals(1.0, S3ClientMetrics.apiCallTotal.labelValues("GetObject").get(), 0.001);
    assertEquals(
        1.0, S3ClientMetrics.apiCallErrorTotal.labelValues("GetObject", "THROTTLING").get(), 0.001);
  }

  @Test
  public void testFailedApiCallWithServerError() {
    MetricCollection collection = buildApiCallCollectionWithError("HeadObject", "SERVER_ERROR");
    S3ClientMetrics.getInstance().publish(collection);

    assertEquals(
        1.0,
        S3ClientMetrics.apiCallErrorTotal.labelValues("HeadObject", "SERVER_ERROR").get(),
        0.001);
  }

  @Test
  public void testRetryCountAddedToRetryTotal() {
    MetricCollection collection = buildApiCallCollection("GetObject", true, 3, null);
    S3ClientMetrics.getInstance().publish(collection);

    assertEquals(3.0, S3ClientMetrics.retryTotal.labelValues("GetObject").get(), 0.001);
  }

  @Test
  public void testZeroRetriesDoesNotIncrementRetryCounter() {
    MetricCollection collection = buildApiCallCollection("GetObject", true, 0, null);
    S3ClientMetrics.getInstance().publish(collection);

    assertEquals(0.0, S3ClientMetrics.retryTotal.labelValues("GetObject").get(), 0.001);
  }

  @Test
  public void testAttemptChildMetrics() {
    MetricCollector collector = MetricCollector.create("ApiCall");
    collector.reportMetric(CoreMetric.OPERATION_NAME, "GetObject");
    collector.reportMetric(CoreMetric.API_CALL_SUCCESSFUL, true);
    collector.reportMetric(CoreMetric.RETRY_COUNT, 0);

    MetricCollector attempt = collector.createChild("ApiCallAttempt");
    attempt.reportMetric(CoreMetric.TIME_TO_FIRST_BYTE, Duration.ofMillis(50));
    attempt.reportMetric(HttpMetric.CONCURRENCY_ACQUIRE_DURATION, Duration.ofMillis(5));
    attempt.reportMetric(HttpMetric.AVAILABLE_CONCURRENCY, 45);
    attempt.reportMetric(HttpMetric.LEASED_CONCURRENCY, 5);
    attempt.reportMetric(HttpMetric.PENDING_CONCURRENCY_ACQUIRES, 2);
    attempt.reportMetric(HttpMetric.MAX_CONCURRENCY, 50);
    attempt.collect();

    MetricCollection collection = collector.collect();
    S3ClientMetrics.getInstance().publish(collection);

    SummarySnapshot.SummaryDataPointSnapshot ttfbDp =
        getSummaryDataPoint(S3ClientMetrics.timeToFirstByte, "GetObject");
    assertEquals(1, ttfbDp.getCount());
    assertEquals(50.0, ttfbDp.getSum(), 0.001);

    SummarySnapshot.SummaryDataPointSnapshot acquireDp =
        getSummaryDataPoint(S3ClientMetrics.concurrencyAcquireDuration, "GetObject");
    assertEquals(1, acquireDp.getCount());
    assertEquals(5.0, acquireDp.getSum(), 0.001);

    assertEquals(45.0, S3ClientMetrics.connectionPoolAvailable.get(), 0.001);
    assertEquals(5.0, S3ClientMetrics.connectionPoolLeased.get(), 0.001);
    assertEquals(2.0, S3ClientMetrics.connectionPoolPending.get(), 0.001);
    assertEquals(50.0, S3ClientMetrics.connectionPoolMax.get(), 0.001);
  }

  @Test
  public void testConnectionPoolGaugesUpdatedFromLastAttempt() {
    MetricCollector collector = MetricCollector.create("ApiCall");
    collector.reportMetric(CoreMetric.OPERATION_NAME, "GetObject");
    collector.reportMetric(CoreMetric.API_CALL_SUCCESSFUL, true);
    collector.reportMetric(CoreMetric.RETRY_COUNT, 1);

    // First attempt
    MetricCollector attempt1 = collector.createChild("ApiCallAttempt");
    attempt1.reportMetric(HttpMetric.AVAILABLE_CONCURRENCY, 30);
    attempt1.reportMetric(HttpMetric.LEASED_CONCURRENCY, 20);
    attempt1.collect();

    // Second (last) attempt — should win for pool gauges
    MetricCollector attempt2 = collector.createChild("ApiCallAttempt");
    attempt2.reportMetric(HttpMetric.AVAILABLE_CONCURRENCY, 10);
    attempt2.reportMetric(HttpMetric.LEASED_CONCURRENCY, 40);
    attempt2.collect();

    MetricCollection collection = collector.collect();
    S3ClientMetrics.getInstance().publish(collection);

    assertEquals(10.0, S3ClientMetrics.connectionPoolAvailable.get(), 0.001);
    assertEquals(40.0, S3ClientMetrics.connectionPoolLeased.get(), 0.001);
  }

  @Test
  public void testMultipleOperationsTrackedSeparately() {
    S3ClientMetrics.getInstance()
        .publish(buildApiCallCollection("GetObject", true, 0, Duration.ofMillis(100)));
    S3ClientMetrics.getInstance()
        .publish(buildApiCallCollection("GetObject", true, 0, Duration.ofMillis(200)));
    S3ClientMetrics.getInstance()
        .publish(buildApiCallCollection("PutObject", true, 0, Duration.ofMillis(300)));

    assertEquals(2.0, S3ClientMetrics.apiCallTotal.labelValues("GetObject").get(), 0.001);
    assertEquals(1.0, S3ClientMetrics.apiCallTotal.labelValues("PutObject").get(), 0.001);
    assertEquals(
        300.0, getSummaryDataPoint(S3ClientMetrics.apiCallDuration, "GetObject").getSum(), 0.001);
    assertEquals(
        300.0, getSummaryDataPoint(S3ClientMetrics.apiCallDuration, "PutObject").getSum(), 0.001);
  }

  @Test
  public void testMissingOperationNameDefaultsToUnknown() {
    MetricCollector collector = MetricCollector.create("ApiCall");
    collector.reportMetric(CoreMetric.API_CALL_SUCCESSFUL, true);
    collector.reportMetric(CoreMetric.RETRY_COUNT, 0);
    MetricCollection collection = collector.collect();

    S3ClientMetrics.getInstance().publish(collection);

    assertEquals(1.0, S3ClientMetrics.apiCallTotal.labelValues("Unknown").get(), 0.001);
  }

  @Test
  public void testMissingErrorTypeDefaultsToUnknown() {
    MetricCollector collector = MetricCollector.create("ApiCall");
    collector.reportMetric(CoreMetric.OPERATION_NAME, "GetObject");
    collector.reportMetric(CoreMetric.API_CALL_SUCCESSFUL, false);
    collector.reportMetric(CoreMetric.RETRY_COUNT, 0);
    MetricCollection collection = collector.collect();

    S3ClientMetrics.getInstance().publish(collection);

    assertEquals(
        1.0, S3ClientMetrics.apiCallErrorTotal.labelValues("GetObject", "Unknown").get(), 0.001);
  }

  @Test
  public void testNoAttemptsDoesNotUpdatePoolGauges() {
    MetricCollection collection = buildApiCallCollection("HeadObject", true, 0, null);
    S3ClientMetrics.getInstance().publish(collection);

    // Gauges should remain at 0.0 (initial value) since no attempt children
    assertEquals(0.0, S3ClientMetrics.connectionPoolAvailable.get(), 0.001);
    assertEquals(0.0, S3ClientMetrics.connectionPoolLeased.get(), 0.001);
  }

  @Test
  public void testRegisterAddsAllMetricsToRegistry() {
    PrometheusRegistry registry = new PrometheusRegistry();
    S3ClientMetrics.register(registry);

    MetricSnapshots snapshots = registry.scrape();
    long s3MetricCount =
        snapshots.stream().filter(s -> s.getMetadata().getName().startsWith("nrt_s3_")).count();
    // 3 counters + 3 summaries (each produces _count, _sum, and quantile series) + 4 gauges = 10
    assertEquals(10, s3MetricCount);
  }

  @Test
  public void testGetInstanceReturnsSingleton() {
    assertNotNull(S3ClientMetrics.getInstance());
    assertEquals(S3ClientMetrics.getInstance(), S3ClientMetrics.getInstance());
  }

  @Test
  public void testSubMillisecondDuration() {
    MetricCollector collector = MetricCollector.create("ApiCall");
    collector.reportMetric(CoreMetric.OPERATION_NAME, "HeadObject");
    collector.reportMetric(CoreMetric.API_CALL_SUCCESSFUL, true);
    collector.reportMetric(CoreMetric.RETRY_COUNT, 0);
    collector.reportMetric(CoreMetric.API_CALL_DURATION, Duration.ofNanos(500_000)); // 0.5ms
    MetricCollection collection = collector.collect();

    S3ClientMetrics.getInstance().publish(collection);

    assertEquals(
        0.5, getSummaryDataPoint(S3ClientMetrics.apiCallDuration, "HeadObject").getSum(), 0.001);
  }
}
