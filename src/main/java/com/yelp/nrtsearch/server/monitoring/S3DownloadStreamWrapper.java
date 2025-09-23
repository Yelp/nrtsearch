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

import io.prometheus.metrics.core.datapoints.CounterDataPoint;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.InputStream;
import org.apache.commons.io.input.ProxyInputStream;

/**
 * InputStream wrapper that counts the number of bytes read from an S3 download stream
 * and updates a Prometheus counter with the total bytes downloaded per index.
 */
public class S3DownloadStreamWrapper extends ProxyInputStream {
  public static final Counter nrtS3DownloadBytes =
      Counter.builder()
          .name("nrt_s3_download_bytes_total")
          .help("Total number of bytes downloaded from S3.")
          .labelNames("index")
          .build();

  public static void register(PrometheusRegistry registry) {
    registry.register(nrtS3DownloadBytes);
  }

  private final CounterDataPoint counterDataPoint;

  public S3DownloadStreamWrapper(InputStream proxy, String indexName) {
    super(proxy);
    counterDataPoint = nrtS3DownloadBytes.labelValues(indexName);
  }

  @Override
  protected void afterRead(int n) {
    if (n != -1) {
      counterDataPoint.inc(n);
    }
  }
}
