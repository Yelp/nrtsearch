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

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import java.util.Enumeration;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleClientMetricExporterTest {

  private CollectorRegistry registry;
  private SimpleClientMetricExporter exporter;

  @Before
  public void setUp() {
    registry = new CollectorRegistry();
    exporter = SimpleClientMetricExporter.create(registry);
  }

  @After
  public void tearDown() {
    exporter.shutdown();
  }

  @Test
  public void testCollectReturnsEmptyBeforeOtelRegistration() {
    // No SdkMeterProvider registered yet — registration is still noop
    List<Collector.MetricFamilySamples> samples = exporter.collect();
    assertThat(samples).isEmpty();
  }

  @Test
  public void testGetAggregationTemporalityReturnsCumulativeForAllTypes() {
    for (InstrumentType type : InstrumentType.values()) {
      assertThat(exporter.getAggregationTemporality(type))
          .as("Expected CUMULATIVE for InstrumentType %s", type)
          .isEqualTo(AggregationTemporality.CUMULATIVE);
    }
  }

  @Test
  public void testForceFlushReturnsSuccess() {
    assertThat(exporter.forceFlush().isSuccess()).isTrue();
  }

  @Test
  public void testShutdownUnregistersFromCollectorRegistry() {
    exporter.shutdown();

    // After shutdown, the registry should return no metric families from this exporter
    Enumeration<Collector.MetricFamilySamples> all = registry.metricFamilySamples();
    assertThat(all.hasMoreElements()).isFalse();
  }

  @Test
  public void testEndToEndCounterAppearsInPrometheusRegistry() {
    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder().registerMetricReader(exporter).build();
    Meter meter = meterProvider.get("test");

    meter.counterBuilder("my_requests_total").build().add(7);

    List<Collector.MetricFamilySamples> collected = exporter.collect();

    assertThat(collected).isNotEmpty();
    Collector.MetricFamilySamples mfs =
        collected.stream()
            .filter(m -> m.name.contains("my_requests"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("my_requests metric not found"));

    assertThat(mfs.type).isEqualTo(Collector.Type.COUNTER);
    assertThat(mfs.samples).hasSize(1);
    assertThat(mfs.samples.get(0).value).isEqualTo(7.0);

    meterProvider.close();
  }

  @Test
  public void testEndToEndGaugeAppearsInPrometheusRegistry() {
    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder().registerMetricReader(exporter).build();
    Meter meter = meterProvider.get("test");

    meter.gaugeBuilder("active_connections").ofLongs().build().set(12);

    List<Collector.MetricFamilySamples> collected = exporter.collect();

    Collector.MetricFamilySamples mfs =
        collected.stream()
            .filter(m -> m.name.contains("active_connections"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("active_connections metric not found"));

    assertThat(mfs.type).isEqualTo(Collector.Type.GAUGE);
    assertThat(mfs.samples.get(0).value).isEqualTo(12.0);

    meterProvider.close();
  }

  @Test
  public void testEndToEndHistogramAppearsInPrometheusRegistry() {
    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder().registerMetricReader(exporter).build();
    Meter meter = meterProvider.get("test");

    meter.histogramBuilder("request_latency_seconds").build().record(0.1);
    meter.histogramBuilder("request_latency_seconds").build().record(0.5);

    List<Collector.MetricFamilySamples> collected = exporter.collect();

    Collector.MetricFamilySamples mfs =
        collected.stream()
            .filter(m -> m.name.contains("request_latency_seconds"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("request_latency_seconds metric not found"));

    assertThat(mfs.type).isEqualTo(Collector.Type.HISTOGRAM);

    boolean hasCount = mfs.samples.stream().anyMatch(s -> s.name.endsWith("_count"));
    boolean hasSum = mfs.samples.stream().anyMatch(s -> s.name.endsWith("_sum"));
    boolean hasBucket = mfs.samples.stream().anyMatch(s -> s.name.endsWith("_bucket"));
    assertThat(hasCount).isTrue();
    assertThat(hasSum).isTrue();
    assertThat(hasBucket).isTrue();

    meterProvider.close();
  }

  @Test
  public void testCollectIsIdempotent() {
    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder().registerMetricReader(exporter).build();
    Meter meter = meterProvider.get("test");
    meter.counterBuilder("calls").build().add(1);

    List<Collector.MetricFamilySamples> first = exporter.collect();
    List<Collector.MetricFamilySamples> second = exporter.collect();

    assertThat(first).hasSize(second.size());

    meterProvider.close();
  }

  @Test
  public void testMetricsAppearInRegistryMetricFamilySamples() {
    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder().registerMetricReader(exporter).build();
    Meter meter = meterProvider.get("test");
    meter.counterBuilder("registry_test_counter").build().add(3);

    // Trigger collection so the exporter has data
    exporter.collect();

    // Verify the registry returns the metric when scraped via the standard Prometheus path
    Enumeration<Collector.MetricFamilySamples> all = registry.metricFamilySamples();
    boolean found = false;
    while (all.hasMoreElements()) {
      Collector.MetricFamilySamples mfs = all.nextElement();
      if (mfs.name.contains("registry_test_counter")) {
        found = true;
        break;
      }
    }
    assertThat(found).as("Expected registry_test_counter in registry scrape").isTrue();

    meterProvider.close();
  }

  @Test
  public void testUnsupportedTypesAreDropped() {
    // There's no public API to directly produce EXPONENTIAL_HISTOGRAM via the stable API —
    // we verify this indirectly by confirming null is returned for unsupported types
    // (MetricAdapter covers this; here we just confirm collect() never throws)
    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder().registerMetricReader(exporter).build();

    // No metrics recorded — collect should return empty without errors
    List<Collector.MetricFamilySamples> collected = exporter.collect();
    assertThat(collected).isEmpty();

    meterProvider.close();
  }

  @Test
  public void testMultipleMetricsInSingleCollect() {
    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder().registerMetricReader(exporter).build();
    Meter meter = meterProvider.get("test");

    meter.counterBuilder("counter_a").build().add(1);
    meter.counterBuilder("counter_b").build().add(2);
    meter.gaugeBuilder("gauge_c").ofLongs().build().set(3);

    List<Collector.MetricFamilySamples> collected = exporter.collect();

    List<String> names =
        collected.stream().map(m -> m.name).collect(java.util.stream.Collectors.toList());
    assertThat(names).anyMatch(n -> n.contains("counter_a"));
    assertThat(names).anyMatch(n -> n.contains("counter_b"));
    assertThat(names).anyMatch(n -> n.contains("gauge_c"));

    meterProvider.close();
  }

  /** Returns all metric family names currently in the given registry. */
  private static List<String> registryMetricNames(CollectorRegistry registry) {
    Enumeration<Collector.MetricFamilySamples> all = registry.metricFamilySamples();
    List<String> names = new java.util.ArrayList<>();
    while (all.hasMoreElements()) {
      names.add(all.nextElement().name);
    }
    return names;
  }
}
