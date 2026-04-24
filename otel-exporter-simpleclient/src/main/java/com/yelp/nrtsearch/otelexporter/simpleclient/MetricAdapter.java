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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.data.SummaryPointData;
import io.opentelemetry.sdk.metrics.data.ValueAtQuantile;
import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Stateless utility for converting OpenTelemetry {@link MetricData} to Prometheus simpleclient v0.x
 * {@link Collector.MetricFamilySamples}.
 */
final class MetricAdapter {

  private MetricAdapter() {}

  /**
   * Converts a single {@link MetricData} to a {@link Collector.MetricFamilySamples}, or {@code
   * null} if the metric type is not supported (e.g. EXPONENTIAL_HISTOGRAM).
   */
  static Collector.MetricFamilySamples toMetricFamilySamples(MetricData metricData) {
    String name = Collector.sanitizeMetricName(metricData.getName());
    Collector.Type type = mapType(metricData);
    if (type == null) {
      return null;
    }
    List<Sample> samples = toSamples(name, metricData, type);
    return new Collector.MetricFamilySamples(name, type, metricData.getDescription(), samples);
  }

  private static Collector.Type mapType(MetricData metricData) {
    switch (metricData.getType()) {
      case LONG_GAUGE:
      case DOUBLE_GAUGE:
        return Collector.Type.GAUGE;
      case LONG_SUM:
        return metricData.getLongSumData().isMonotonic()
            ? Collector.Type.COUNTER
            : Collector.Type.GAUGE;
      case DOUBLE_SUM:
        return metricData.getDoubleSumData().isMonotonic()
            ? Collector.Type.COUNTER
            : Collector.Type.GAUGE;
      case HISTOGRAM:
        return Collector.Type.HISTOGRAM;
      case SUMMARY:
        return Collector.Type.SUMMARY;
      default:
        // EXPONENTIAL_HISTOGRAM and any future types are dropped
        return null;
    }
  }

  private static List<Sample> toSamples(String name, MetricData metricData, Collector.Type type) {
    switch (metricData.getType()) {
      case LONG_GAUGE:
        return convertLongPoints(name, metricData.getLongGaugeData().getPoints(), type);
      case DOUBLE_GAUGE:
        return convertDoublePoints(name, metricData.getDoubleGaugeData().getPoints(), type);
      case LONG_SUM:
        return convertLongPoints(name, metricData.getLongSumData().getPoints(), type);
      case DOUBLE_SUM:
        return convertDoublePoints(name, metricData.getDoubleSumData().getPoints(), type);
      case HISTOGRAM:
        return convertHistogramPoints(name, metricData.getHistogramData().getPoints());
      case SUMMARY:
        return convertSummaryPoints(name, metricData.getSummaryData().getPoints());
      default:
        return List.of();
    }
  }

  private static List<Sample> convertLongPoints(
      String name, Collection<LongPointData> points, Collector.Type type) {
    String sampleName = type == Collector.Type.COUNTER ? name + "_total" : name;
    List<Sample> samples = new ArrayList<>(points.size());
    for (LongPointData point : points) {
      List<String> labelNames = new ArrayList<>();
      List<String> labelValues = new ArrayList<>();
      attributesToLabels(point.getAttributes(), labelNames, labelValues);
      samples.add(new Sample(sampleName, labelNames, labelValues, (double) point.getValue()));
    }
    return samples;
  }

  private static List<Sample> convertDoublePoints(
      String name, Collection<DoublePointData> points, Collector.Type type) {
    String sampleName = type == Collector.Type.COUNTER ? name + "_total" : name;
    List<Sample> samples = new ArrayList<>(points.size());
    for (DoublePointData point : points) {
      List<String> labelNames = new ArrayList<>();
      List<String> labelValues = new ArrayList<>();
      attributesToLabels(point.getAttributes(), labelNames, labelValues);
      samples.add(new Sample(sampleName, labelNames, labelValues, point.getValue()));
    }
    return samples;
  }

  private static List<Sample> convertHistogramPoints(
      String name, Collection<HistogramPointData> points) {
    List<Sample> samples = new ArrayList<>();
    for (HistogramPointData point : points) {
      List<String> baseLabelNames = new ArrayList<>();
      List<String> baseLabelValues = new ArrayList<>();
      attributesToLabels(point.getAttributes(), baseLabelNames, baseLabelValues);

      // Bucket samples (cumulative)
      List<Double> boundaries = point.getBoundaries();
      List<Long> counts = point.getCounts();
      long cumulative = 0;
      for (int i = 0; i < boundaries.size(); i++) {
        cumulative += counts.get(i);
        List<String> bucketLabelNames = new ArrayList<>(baseLabelNames);
        List<String> bucketLabelValues = new ArrayList<>(baseLabelValues);
        bucketLabelNames.add("le");
        bucketLabelValues.add(Collector.doubleToGoString(boundaries.get(i)));
        samples.add(new Sample(name + "_bucket", bucketLabelNames, bucketLabelValues, cumulative));
      }
      // +Inf bucket
      List<String> infLabelNames = new ArrayList<>(baseLabelNames);
      List<String> infLabelValues = new ArrayList<>(baseLabelValues);
      infLabelNames.add("le");
      infLabelValues.add("+Inf");
      samples.add(new Sample(name + "_bucket", infLabelNames, infLabelValues, point.getCount()));

      // _count and _sum
      samples.add(new Sample(name + "_count", baseLabelNames, baseLabelValues, point.getCount()));
      samples.add(new Sample(name + "_sum", baseLabelNames, baseLabelValues, point.getSum()));
    }
    return samples;
  }

  private static List<Sample> convertSummaryPoints(
      String name, Collection<SummaryPointData> points) {
    List<Sample> samples = new ArrayList<>();
    for (SummaryPointData point : points) {
      List<String> baseLabelNames = new ArrayList<>();
      List<String> baseLabelValues = new ArrayList<>();
      attributesToLabels(point.getAttributes(), baseLabelNames, baseLabelValues);

      // Quantile samples
      for (ValueAtQuantile vaq : point.getValues()) {
        List<String> quantileLabelNames = new ArrayList<>(baseLabelNames);
        List<String> quantileLabelValues = new ArrayList<>(baseLabelValues);
        quantileLabelNames.add("quantile");
        quantileLabelValues.add(Collector.doubleToGoString(vaq.getQuantile()));
        samples.add(new Sample(name, quantileLabelNames, quantileLabelValues, vaq.getValue()));
      }

      // _count and _sum
      samples.add(new Sample(name + "_count", baseLabelNames, baseLabelValues, point.getCount()));
      samples.add(new Sample(name + "_sum", baseLabelNames, baseLabelValues, point.getSum()));
    }
    return samples;
  }

  private static void attributesToLabels(
      Attributes attributes, List<String> labelNames, List<String> labelValues) {
    attributes.forEach(
        (key, value) -> {
          labelNames.add(sanitizeLabelName(key.getKey()));
          labelValues.add(value == null ? "" : value.toString());
        });
  }

  /**
   * Sanitizes an OTel attribute key to a valid Prometheus label name. Replaces characters outside
   * {@code [a-zA-Z0-9_]} with {@code _}, and prepends {@code _} if the name starts with a digit.
   */
  static String sanitizeLabelName(String name) {
    String sanitized = name.replaceAll("[^a-zA-Z0-9_]", "_");
    if (!sanitized.isEmpty() && Character.isDigit(sanitized.charAt(0))) {
      sanitized = "_" + sanitized;
    }
    return sanitized;
  }

  /** Returns all points from a collection of {@link PointData}. Visible for testing. */
  static <T extends PointData> List<T> toList(Collection<T> points) {
    return new ArrayList<>(points);
  }
}
