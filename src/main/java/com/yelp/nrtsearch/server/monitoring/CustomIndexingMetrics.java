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

public class CustomIndexingMetrics {

  public static final Counter updateDocValuesRequestsReceived =
      Counter.builder()
          .name("update_doc_values_requests_received")
          .help("Number of requests received for the update doc values API ")
          .labelNames("index")
          .build();

  // counter for addDocument requests received for the index with the index name as the label value
  public static final Counter addDocumentRequestsReceived =
      Counter.builder()
          .name("add_document_requests_received")
          .help("Number of requests received for the add document API ")
          .labelNames("index")
          .build();

  public static final Gauge updateDocValuesLatency =
      Gauge.builder()
          .name("update_doc_values_latency")
          .help("Latency of the update doc values API")
          .labelNames("index")
          .build();

  // gauge for the latency of the addDocument API with the index name as the label value
  public static final Gauge addDocumentLatency =
      Gauge.builder()
          .name("add_document_latency")
          .help("Latency of the add document API")
          .labelNames("index")
          .build();
}
