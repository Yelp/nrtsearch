/*
 * Copyright 2020 Yelp Inc.
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

import io.grpc.MethodDescriptor;
import io.grpc.Status.Code;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.SimpleCollector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Prometheus metric definitions used for server-side monitoring of grpc services.
 *
 * <p>Instances of this class hold the counters we increment for a specific pair of grpc service
 * definition and collection registry.
 */
class ServerMetrics {
  private static final Counter.Builder serverStartedBuilder =
      Counter.build()
          .namespace("grpc")
          .subsystem("server")
          .name("started_total")
          .labelNames("grpc_type", "grpc_service", "grpc_method", "serviceName", "nodeName")
          .help("Total number of RPCs started on the server.");

  private static final Counter.Builder serverHandledBuilder =
      Counter.build()
          .namespace("grpc")
          .subsystem("server")
          .name("handled_total")
          .labelNames("grpc_type", "grpc_service", "grpc_method", "serviceName", "nodeName", "code")
          .help("Total number of RPCs completed on the server, regardless of success or failure.");

  private static final Histogram.Builder serverHandledLatencySecondsBuilder =
      Histogram.build()
          .namespace("grpc")
          .subsystem("server")
          .name("handled_latency_seconds")
          .labelNames("grpc_type", "grpc_service", "grpc_method", "serviceName", "nodeName")
          .help(
              "Histogram of response latency (seconds) of gRPC that had been application-level "
                  + "handled by the server.");

  private static final Counter.Builder serverStreamMessagesReceivedBuilder =
      Counter.build()
          .namespace("grpc")
          .subsystem("server")
          .name("msg_received_total")
          .labelNames("grpc_type", "grpc_service", "grpc_method", "serviceName", "nodeName")
          .help("Total number of stream messages received from the client.");

  private static final Counter.Builder serverStreamMessagesSentBuilder =
      Counter.build()
          .namespace("grpc")
          .subsystem("server")
          .name("msg_sent_total")
          .labelNames("grpc_type", "grpc_service", "grpc_method", "serviceName", "nodeName")
          .help("Total number of stream messages sent by the server.");

  private final Counter serverStarted;
  private final Counter serverHandled;
  private final Counter serverStreamMessagesReceived;
  private final Counter serverStreamMessagesSent;
  private final Optional<Histogram> serverHandledLatencySeconds;

  private final String serviceName;
  private final String nodeName;

  private final com.yelp.nrtsearch.server.monitoring.GrpcMethod method;

  private ServerMetrics(
      com.yelp.nrtsearch.server.monitoring.GrpcMethod method,
      Counter serverStarted,
      Counter serverHandled,
      Counter serverStreamMessagesReceived,
      Counter serverStreamMessagesSent,
      Optional<Histogram> serverHandledLatencySeconds,
      String serviceName,
      String nodeName) {
    this.method = method;
    this.serverStarted = serverStarted;
    this.serverHandled = serverHandled;
    this.serverStreamMessagesReceived = serverStreamMessagesReceived;
    this.serverStreamMessagesSent = serverStreamMessagesSent;
    this.serverHandledLatencySeconds = serverHandledLatencySeconds;
    this.serviceName = serviceName;
    this.nodeName = nodeName;
  }

  public void recordCallStarted() {
    addLabels(serverStarted).inc();
  }

  public void recordServerHandled(Code code) {
    addLabels(serverHandled, code.toString()).inc();
  }

  public void recordStreamMessageSent() {
    addLabels(serverStreamMessagesSent).inc();
  }

  public void recordStreamMessageReceived() {
    addLabels(serverStreamMessagesReceived).inc();
  }

  /**
   * Only has any effect if monitoring is configured to include latency histograms. Otherwise, this
   * does nothing.
   */
  public void recordLatency(double latencySec) {
    if (!this.serverHandledLatencySeconds.isPresent()) {
      return;
    }
    addLabels(this.serverHandledLatencySeconds.get()).observe(latencySec);
  }

  /**
   * Knows how to produce {@link com.yelp.nrtsearch.server.monitoring.ServerMetrics} instances for
   * individual methods.
   */
  static class Factory {
    private final Counter serverStarted;
    private final Counter serverHandled;
    private final Counter serverStreamMessagesReceived;
    private final Counter serverStreamMessagesSent;
    private final Optional<Histogram> serverHandledLatencySeconds;

    private final String serviceName;
    private final String nodeName;

    Factory(Configuration configuration, String serviceName, String nodeName) {
      CollectorRegistry registry = configuration.getCollectorRegistry();
      this.serverStarted = serverStartedBuilder.register(registry);
      this.serverHandled = serverHandledBuilder.register(registry);
      this.serverStreamMessagesReceived = serverStreamMessagesReceivedBuilder.register(registry);
      this.serverStreamMessagesSent = serverStreamMessagesSentBuilder.register(registry);

      this.serviceName = serviceName;
      this.nodeName = nodeName;

      if (configuration.isIncludeLatencyHistograms()) {
        this.serverHandledLatencySeconds =
            Optional.of(
                serverHandledLatencySecondsBuilder
                    .buckets(configuration.getLatencyBuckets())
                    .register(registry));
      } else {
        this.serverHandledLatencySeconds = Optional.empty();
      }
    }

    /**
     * Creates a {@link com.yelp.nrtsearch.server.monitoring.ServerMetrics} for the supplied method.
     */
    <R, S> com.yelp.nrtsearch.server.monitoring.ServerMetrics createMetricsForMethod(
        MethodDescriptor<R, S> methodDescriptor) {
      return new com.yelp.nrtsearch.server.monitoring.ServerMetrics(
          com.yelp.nrtsearch.server.monitoring.GrpcMethod.of(methodDescriptor),
          serverStarted,
          serverHandled,
          serverStreamMessagesReceived,
          serverStreamMessagesSent,
          serverHandledLatencySeconds,
          serviceName,
          nodeName);
    }
  }

  private <T> T addLabels(SimpleCollector<T> collector, String... labels) {
    List<String> allLabels = new ArrayList<>();
    Collections.addAll(
        allLabels, method.type(), method.serviceName(), method.methodName(), serviceName, nodeName);
    Collections.addAll(allLabels, labels);
    return collector.labels(allLabels.toArray(new String[0]));
  }
}
