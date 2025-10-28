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
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

/**
 * Metrics for S3Directory operations. Tracks reads, bytes transferred, latency, and errors when
 * using remote-only index mode.
 */
public class S3DirectoryMetrics {

  /** Total number of S3 range read requests made by S3IndexInput. */
  public static final Counter s3ReadRequests =
      Counter.builder()
          .name("nrt_s3_directory_read_requests_total")
          .help("Total number of S3 range read requests for index files.")
          .labelNames("index")
          .build();

  /** Total number of bytes read from S3 by S3Directory. */
  public static final Counter s3BytesRead =
      Counter.builder()
          .name("nrt_s3_directory_bytes_read_total")
          .help("Total bytes read from S3 for index files.")
          .labelNames("index")
          .build();

  /** Total number of S3 read errors. */
  public static final Counter s3ReadErrors =
      Counter.builder()
          .name("nrt_s3_directory_read_errors_total")
          .help("Total number of S3 read errors.")
          .labelNames("index", "error_type")
          .build();

  /** Histogram of S3 read request latencies in seconds. */
  public static final Histogram s3ReadLatency =
      Histogram.builder()
          .name("nrt_s3_directory_read_latency_seconds")
          .help("Latency of S3 range read requests in seconds.")
          .labelNames("index")
          .build();

  /** Number of currently open S3IndexInput instances. */
  public static final Gauge s3OpenInputs =
      Gauge.builder()
          .name("nrt_s3_directory_open_inputs")
          .help("Number of currently open S3IndexInput instances.")
          .labelNames("index")
          .build();

  /** Number of files in the S3Directory. */
  public static final Gauge s3DirectoryFiles =
      Gauge.builder()
          .name("nrt_s3_directory_files")
          .help("Number of files in S3Directory metadata.")
          .labelNames("index")
          .build();

  /**
   * Add all S3Directory metrics to the collector registry.
   *
   * @param registry collector registry
   */
  public static void register(PrometheusRegistry registry) {
    registry.register(s3ReadRequests);
    registry.register(s3BytesRead);
    registry.register(s3ReadErrors);
    registry.register(s3ReadLatency);
    registry.register(s3OpenInputs);
    registry.register(s3DirectoryFiles);
  }

  /**
   * Record a successful S3 read operation.
   *
   * @param index index name
   * @param bytesRead number of bytes read
   * @param latencySeconds read latency in seconds
   */
  public static void recordRead(String index, long bytesRead, double latencySeconds) {
    s3ReadRequests.labelValues(index).inc();
    s3BytesRead.labelValues(index).inc(bytesRead);
    s3ReadLatency.labelValues(index).observe(latencySeconds);
  }

  /**
   * Record an S3 read error.
   *
   * @param index index name
   * @param errorType type of error (e.g., "IOException", "AmazonS3Exception")
   */
  public static void recordError(String index, String errorType) {
    s3ReadErrors.labelValues(index, errorType).inc();
  }

  /**
   * Update the number of open inputs for an index.
   *
   * @param index index name
   * @param count number of open inputs
   */
  public static void updateOpenInputs(String index, int count) {
    s3OpenInputs.labelValues(index).set(count);
  }

  /**
   * Update the number of files in S3Directory.
   *
   * @param index index name
   * @param fileCount number of files
   */
  public static void updateFileCount(String index, int fileCount) {
    s3DirectoryFiles.labelValues(index).set(fileCount);
  }
}
