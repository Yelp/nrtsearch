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

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.CollectionRegistration;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bridges OpenTelemetry SDK metrics to a Prometheus simpleclient v0.x {@link CollectorRegistry}.
 *
 * <p>This class implements both {@link MetricReader} (OTel side) and extends {@link Collector}
 * (Prometheus v0.x side). On each Prometheus scrape, it pulls a fresh snapshot of all OTel metrics
 * and converts them to {@link Collector.MetricFamilySamples}.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * SimpleClientMetricExporter bridge = SimpleClientMetricExporter.create();
 *
 * SdkMeterProvider meterProvider = SdkMeterProvider.builder()
 *     .registerMetricReader(bridge)
 *     .build();
 * OpenTelemetrySdk otelSdk = OpenTelemetrySdk.builder()
 *     .setMeterProvider(meterProvider)
 *     .build();
 *
 * // bridge is already registered with CollectorRegistry.defaultRegistry
 * }</pre>
 */
public class SimpleClientMetricExporter extends Collector implements MetricReader {

  private volatile CollectionRegistration registration = CollectionRegistration.noop();
  private final CollectorRegistry collectorRegistry;
  private final AtomicBoolean shutdown = new AtomicBoolean(false);

  private SimpleClientMetricExporter(CollectorRegistry collectorRegistry) {
    this.collectorRegistry = collectorRegistry;
  }

  /**
   * Creates a new {@link SimpleClientMetricExporter} registered with {@link
   * CollectorRegistry#defaultRegistry}.
   */
  public static SimpleClientMetricExporter create() {
    return create(CollectorRegistry.defaultRegistry);
  }

  /**
   * Creates a new {@link SimpleClientMetricExporter} registered with the given {@link
   * CollectorRegistry}.
   */
  public static SimpleClientMetricExporter create(CollectorRegistry registry) {
    SimpleClientMetricExporter exporter = new SimpleClientMetricExporter(registry);
    exporter.register(registry);
    return exporter;
  }

  // --- MetricReader ---

  /**
   * Called by the OTel {@code SdkMeterProvider} during setup. Stores the registration handle used
   * to pull metric snapshots on demand.
   */
  @Override
  public void register(CollectionRegistration registration) {
    this.registration = registration;
  }

  /**
   * Prometheus requires cumulative temporality so counters increase monotonically between scrapes.
   */
  @Override
  public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
    return AggregationTemporality.CUMULATIVE;
  }

  /** No-op for a pull-based reader — there is nothing to flush. */
  @Override
  public CompletableResultCode forceFlush() {
    return CompletableResultCode.ofSuccess();
  }

  /** Unregisters this collector from the Prometheus registry on shutdown. Idempotent. */
  @Override
  public CompletableResultCode shutdown() {
    if (shutdown.compareAndSet(false, true)) {
      collectorRegistry.unregister(this);
    }
    return CompletableResultCode.ofSuccess();
  }

  // --- Collector ---

  /**
   * Called by Prometheus on each scrape. Pulls a fresh metric snapshot from the OTel SDK and
   * converts it to Prometheus {@link Collector.MetricFamilySamples}.
   */
  @Override
  public List<MetricFamilySamples> collect() {
    Collection<MetricData> otelMetrics = registration.collectAllMetrics();
    if (otelMetrics.isEmpty()) {
      return Collections.emptyList();
    }
    List<MetricFamilySamples> result = new ArrayList<>(otelMetrics.size());
    for (MetricData metricData : otelMetrics) {
      MetricFamilySamples mfs = MetricAdapter.toMetricFamilySamples(metricData);
      if (mfs != null) {
        result.add(mfs);
      }
    }
    return result;
  }
}
