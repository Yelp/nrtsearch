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
package com.yelp.nrtsearch.otelexporter.simpleclient;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetricAdapterTest {

  private InMemoryMetricReader reader;
  private SdkMeterProvider meterProvider;
  private Meter meter;

  @Before
  public void setUp() {
    reader = InMemoryMetricReader.create();
    meterProvider = SdkMeterProvider.builder().registerMetricReader(reader).build();
    meter = meterProvider.get("test");
  }

  @After
  public void tearDown() {
    meterProvider.close();
  }

  // --- Counter (monotonic LongSum) ---

  @Test
  public void testMonotonicLongSumBecomesCounter() {
    meter.counterBuilder("requests_total").build().add(5);

    MetricData metricData = getSingleMetric();
    Collector.MetricFamilySamples mfs = MetricAdapter.toMetricFamilySamples(metricData);

    assertThat(mfs).isNotNull();
    assertThat(mfs.type).isEqualTo(Collector.Type.COUNTER);
    assertThat(mfs.samples).hasSize(1);
    assertThat(mfs.samples.get(0).name).endsWith("_total");
    assertThat(mfs.samples.get(0).value).isEqualTo(5.0);
  }

  @Test
  public void testMonotonicDoubleSumBecomesCounter() {
    meter.counterBuilder("bytes_total").ofDoubles().build().add(3.14);

    MetricData metricData = getSingleMetric();
    Collector.MetricFamilySamples mfs = MetricAdapter.toMetricFamilySamples(metricData);

    assertThat(mfs).isNotNull();
    assertThat(mfs.type).isEqualTo(Collector.Type.COUNTER);
    assertThat(mfs.samples).hasSize(1);
    assertThat(mfs.samples.get(0).name).endsWith("_total");
    assertThat(mfs.samples.get(0).value).isEqualTo(3.14);
  }

  // --- Gauge ---

  @Test
  public void testLongGaugeBecomesGauge() {
    meter.gaugeBuilder("queue_size").ofLongs().build().set(42);

    MetricData metricData = getSingleMetric();
    Collector.MetricFamilySamples mfs = MetricAdapter.toMetricFamilySamples(metricData);

    assertThat(mfs).isNotNull();
    assertThat(mfs.type).isEqualTo(Collector.Type.GAUGE);
    assertThat(mfs.samples).hasSize(1);
    assertThat(mfs.samples.get(0).value).isEqualTo(42.0);
  }

  @Test
  public void testDoubleGaugeBecomesGauge() {
    meter.gaugeBuilder("temperature").build().set(98.6);

    MetricData metricData = getSingleMetric();
    Collector.MetricFamilySamples mfs = MetricAdapter.toMetricFamilySamples(metricData);

    assertThat(mfs).isNotNull();
    assertThat(mfs.type).isEqualTo(Collector.Type.GAUGE);
    assertThat(mfs.samples).hasSize(1);
    assertThat(mfs.samples.get(0).value).isEqualTo(98.6);
  }

  @Test
  public void testNonMonotonicLongSumBecomesGauge() {
    // UpDownCounter produces a non-monotonic LONG_SUM
    meter.upDownCounterBuilder("active_requests").build().add(3);

    MetricData metricData = getSingleMetric();
    Collector.MetricFamilySamples mfs = MetricAdapter.toMetricFamilySamples(metricData);

    assertThat(mfs).isNotNull();
    assertThat(mfs.type).isEqualTo(Collector.Type.GAUGE);
    assertThat(mfs.samples.get(0).name).doesNotEndWith("_total");
  }

  @Test
  public void testNonMonotonicDoubleSumBecomesGauge() {
    meter.upDownCounterBuilder("active_bytes").ofDoubles().build().add(1.5);

    MetricData metricData = getSingleMetric();
    Collector.MetricFamilySamples mfs = MetricAdapter.toMetricFamilySamples(metricData);

    assertThat(mfs).isNotNull();
    assertThat(mfs.type).isEqualTo(Collector.Type.GAUGE);
  }

  // --- Histogram ---

  @Test
  public void testHistogramConversion() {
    meter.histogramBuilder("request_duration_seconds").build().record(0.5);
    meter.histogramBuilder("request_duration_seconds").build().record(2.0);
    meter.histogramBuilder("request_duration_seconds").build().record(7.0);

    MetricData metricData = getSingleMetric();
    Collector.MetricFamilySamples mfs = MetricAdapter.toMetricFamilySamples(metricData);

    assertThat(mfs).isNotNull();
    assertThat(mfs.type).isEqualTo(Collector.Type.HISTOGRAM);

    Map<String, Sample> samplesByName =
        mfs.samples.stream().collect(Collectors.toMap(s -> s.name + labelKey(s), s -> s));

    // _count and _sum must be present
    assertThat(mfs.samples.stream().filter(s -> s.name.endsWith("_count")).findFirst())
        .isPresent()
        .hasValueSatisfying(s -> assertThat(s.value).isEqualTo(3.0));
    assertThat(mfs.samples.stream().filter(s -> s.name.endsWith("_sum")).findFirst())
        .isPresent()
        .hasValueSatisfying(s -> assertThat(s.value).isEqualTo(9.5));

    // _bucket samples must be present and include +Inf
    List<Sample> buckets =
        mfs.samples.stream().filter(s -> s.name.endsWith("_bucket")).collect(Collectors.toList());
    assertThat(buckets).isNotEmpty();

    Sample infBucket =
        buckets.stream()
            .filter(s -> s.labelValues.contains("+Inf"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("+Inf bucket missing"));
    assertThat(infBucket.value).isEqualTo(3.0);

    // Buckets must be cumulative (each >= previous)
    for (int i = 1; i < buckets.size(); i++) {
      assertThat(buckets.get(i).value).isGreaterThanOrEqualTo(buckets.get(i - 1).value);
    }
  }

  @Test
  public void testHistogramHasLeLabel() {
    meter.histogramBuilder("latency").build().record(1.0);

    MetricData metricData = getSingleMetric();
    Collector.MetricFamilySamples mfs = MetricAdapter.toMetricFamilySamples(metricData);

    assertThat(mfs).isNotNull();
    List<Sample> buckets =
        mfs.samples.stream().filter(s -> s.name.endsWith("_bucket")).collect(Collectors.toList());
    assertThat(buckets).allSatisfy(s -> assertThat(s.labelNames).contains("le"));
  }

  // --- Attributes → labels ---

  @Test
  public void testAttributesBecomeLabelNamesAndValues() {
    meter
        .counterBuilder("http_requests")
        .build()
        .add(1, Attributes.builder().put("service.name", "foo").put("status_code", "200").build());

    MetricData metricData = getSingleMetric();
    Collector.MetricFamilySamples mfs = MetricAdapter.toMetricFamilySamples(metricData);

    assertThat(mfs).isNotNull();
    assertThat(mfs.samples).hasSize(1);
    Sample sample = mfs.samples.get(0);

    // service.name → service_name (dot replaced with underscore)
    int serviceIdx = sample.labelNames.indexOf("service_name");
    assertThat(serviceIdx).isGreaterThanOrEqualTo(0);
    assertThat(sample.labelValues.get(serviceIdx)).isEqualTo("foo");

    int statusIdx = sample.labelNames.indexOf("status_code");
    assertThat(statusIdx).isGreaterThanOrEqualTo(0);
    assertThat(sample.labelValues.get(statusIdx)).isEqualTo("200");
  }

  @Test
  public void testMultiplePointsProduceMultipleSamples() {
    Attributes attrs1 = Attributes.builder().put("region", "us-east").build();
    Attributes attrs2 = Attributes.builder().put("region", "us-west").build();
    meter.counterBuilder("requests").build().add(10, attrs1);
    meter.counterBuilder("requests").build().add(20, attrs2);

    MetricData metricData = getSingleMetric();
    Collector.MetricFamilySamples mfs = MetricAdapter.toMetricFamilySamples(metricData);

    assertThat(mfs).isNotNull();
    assertThat(mfs.samples).hasSize(2);
  }

  // --- Metric name sanitization ---

  @Test
  public void testMetricNameDotsBecomesUnderscores() {
    meter.counterBuilder("http.server.requests").build().add(1);

    MetricData metricData = getSingleMetric();
    Collector.MetricFamilySamples mfs = MetricAdapter.toMetricFamilySamples(metricData);

    assertThat(mfs).isNotNull();
    assertThat(mfs.name).doesNotContain(".");
    assertThat(mfs.name).contains("http_server_requests");
  }

  // --- Label name sanitization ---

  @Test
  public void testLabelNameSanitization() {
    assertThat(MetricAdapter.sanitizeLabelName("service.name")).isEqualTo("service_name");
    assertThat(MetricAdapter.sanitizeLabelName("my-label")).isEqualTo("my_label");
    assertThat(MetricAdapter.sanitizeLabelName("123abc")).isEqualTo("_123abc");
    assertThat(MetricAdapter.sanitizeLabelName("valid_name")).isEqualTo("valid_name");
  }

  // --- Helpers ---

  private MetricData getSingleMetric() {
    Collection<MetricData> metrics = reader.collectAllMetrics();
    assertThat(metrics).hasSize(1);
    return metrics.iterator().next();
  }

  /** Returns a unique key combining name + label values so we can dedup samples in a map. */
  private static String labelKey(Sample s) {
    if (s.labelValues.isEmpty()) return "";
    return ":" + String.join(",", s.labelValues);
  }

  private static AttributesBuilder attrs() {
    return Attributes.builder();
  }
}
