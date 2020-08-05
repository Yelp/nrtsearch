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

import io.grpc.ForwardingServerCallListener;
import io.grpc.ServerCall;

/**
 * A {@link ForwardingServerCallListener} which updates Prometheus metrics for a single rpc based on
 * updates received from grpc.
 */
class MonitoringServerCallListener<R> extends ForwardingServerCallListener<R> {
  private final ServerCall.Listener<R> delegate;
  private final com.yelp.nrtsearch.server.monitoring.GrpcMethod grpcMethod;
  private final com.yelp.nrtsearch.server.monitoring.ServerMetrics serverMetrics;

  MonitoringServerCallListener(
      ServerCall.Listener<R> delegate,
      com.yelp.nrtsearch.server.monitoring.ServerMetrics serverMetrics,
      com.yelp.nrtsearch.server.monitoring.GrpcMethod grpcMethod) {
    this.delegate = delegate;
    this.serverMetrics = serverMetrics;
    this.grpcMethod = grpcMethod;
  }

  @Override
  protected ServerCall.Listener<R> delegate() {
    return delegate;
  }

  @Override
  public void onMessage(R request) {
    if (grpcMethod.streamsRequests()) {
      serverMetrics.recordStreamMessageReceived();
    }
    super.onMessage(request);
  }
}
