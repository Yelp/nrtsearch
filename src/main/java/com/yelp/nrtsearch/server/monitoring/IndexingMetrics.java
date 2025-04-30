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
import io.prometheus.metrics.core.metrics.Summary;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

public class IndexingMetrics {

  public static final String UPDATE_DOC_VALUES_REQUESTS_RECEIVED =
      "nrt_update_doc_values_requests_received";
  public static final String ADD_DOCUMENT_REQUESTS_RECEIVED = "nrt_add_document_requests_received";
  public static final String UPDATE_DOC_VALUES_LATENCY = "nrt_update_doc_values_latency_ms";
  public static final String ADD_DOCUMENT_LATENCY = "nrt_add_document_latency_ms";

  public static final Counter updateDocValuesRequestsReceived =
      Counter.builder()
          .name(UPDATE_DOC_VALUES_REQUESTS_RECEIVED)
          .help("Number of requests received for the update doc values API ")
          .labelNames("index")
          .build();

  // counter for addDocument requests received for the index with the index name as the label value
  public static final Counter addDocumentRequestsReceived =
      Counter.builder()
          .name(ADD_DOCUMENT_REQUESTS_RECEIVED)
          .help("Number of requests received for the add document API ")
          .labelNames("index")
          .build();

  public static final Summary updateDocValuesLatency =
      Summary.builder()
          .name(UPDATE_DOC_VALUES_LATENCY)
          .help("Latency of the update doc values API")
          .labelNames("index")
          .quantile(0.25, 0.01)
          .quantile(0.5, 0.01)
          .quantile(0.95, 0.01)
          .quantile(0.99, 0.01)
          .build();

  // gauge for the latency of the addDocument API with the index name as the label value
  public static final Summary addDocumentLatency =
      Summary.builder()
          .name(ADD_DOCUMENT_LATENCY)
          .help("Latency of the add document API")
          .labelNames("index")
          .quantile(0.25, 0.01)
          .quantile(0.5, 0.01)
          .quantile(0.95, 0.01)
          .quantile(0.99, 0.01)
          .build();

  public static void register(PrometheusRegistry registry) {
    registry.register(updateDocValuesRequestsReceived);
    registry.register(addDocumentRequestsReceived);
    registry.register(updateDocValuesLatency);
    registry.register(addDocumentLatency);
  }
}
