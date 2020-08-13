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

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.Status;
import java.time.Clock;
import java.time.Instant;

/**
 * A {@link ForwardingServerCall} which updates Prometheus metrics based on the server-side actions
 * taken for a single rpc, e.g., messages sent, latency, etc.
 */
class MonitoringServerCall<R, S> extends ForwardingServerCall.SimpleForwardingServerCall<R, S> {
  private static final long MILLIS_PER_SECOND = 1000L;

  private final Clock clock;
  private final com.yelp.nrtsearch.server.monitoring.GrpcMethod grpcMethod;
  private final com.yelp.nrtsearch.server.monitoring.ServerMetrics serverMetrics;
  private final com.yelp.nrtsearch.server.monitoring.Configuration configuration;
  private final Instant startInstant;

  MonitoringServerCall(
      ServerCall<R, S> delegate,
      Clock clock,
      com.yelp.nrtsearch.server.monitoring.GrpcMethod grpcMethod,
      com.yelp.nrtsearch.server.monitoring.ServerMetrics serverMetrics,
      Configuration configuration) {
    super(delegate);
    this.clock = clock;
    this.grpcMethod = grpcMethod;
    this.serverMetrics = serverMetrics;
    this.configuration = configuration;
    this.startInstant = clock.instant();

    // TODO(dino): Consider doing this in the onReady() method of the listener instead.
    reportStartMetrics();
  }

  @Override
  public void close(Status status, Metadata responseHeaders) {
    reportEndMetrics(status);
    super.close(status, responseHeaders);
  }

  @Override
  public void sendMessage(S message) {
    if (grpcMethod.streamsResponses()) {
      serverMetrics.recordStreamMessageSent();
    }
    super.sendMessage(message);
  }

  private void reportStartMetrics() {
    serverMetrics.recordCallStarted();
  }

  private void reportEndMetrics(Status status) {
    serverMetrics.recordServerHandled(status.getCode());
    if (configuration.isIncludeLatencyHistograms()) {
      double latencySec =
          (clock.millis() - startInstant.toEpochMilli()) / (double) MILLIS_PER_SECOND;
      serverMetrics.recordLatency(latencySec);
    }
  }
}
