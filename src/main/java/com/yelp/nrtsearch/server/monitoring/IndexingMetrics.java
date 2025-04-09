package com.yelp.nrtsearch.server.monitoring;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

public class IndexingMetrics {

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

  public static void register(PrometheusRegistry registry) {
    registry.register(updateDocValuesRequestsReceived);
    registry.register(addDocumentRequestsReceived);
    registry.register(updateDocValuesLatency);
    registry.register(addDocumentLatency);
  }
}
