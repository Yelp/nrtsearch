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

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.time.Clock;

/** A {@link ServerInterceptor} which sends stats about incoming grpc calls to Prometheus. */
public class LuceneServerMonitoringServerInterceptor implements ServerInterceptor {
  private final Clock clock;
  private final Configuration configuration;
  private final ServerMetrics.Factory serverMetricsFactory;

  public static LuceneServerMonitoringServerInterceptor create(
      Configuration configuration, String serviceName, String nodeName) {
    return new LuceneServerMonitoringServerInterceptor(
        Clock.systemDefaultZone(),
        configuration,
        new ServerMetrics.Factory(configuration, serviceName, nodeName));
  }

  private LuceneServerMonitoringServerInterceptor(
      Clock clock, Configuration configuration, ServerMetrics.Factory serverMetricsFactory) {
    this.clock = clock;
    this.configuration = configuration;
    this.serverMetricsFactory = serverMetricsFactory;
  }

  @Override
  public <R, S> ServerCall.Listener<R> interceptCall(
      ServerCall<R, S> call, Metadata requestHeaders, ServerCallHandler<R, S> next) {
    MethodDescriptor<R, S> method = call.getMethodDescriptor();
    com.yelp.nrtsearch.server.monitoring.ServerMetrics metrics =
        serverMetricsFactory.createMetricsForMethod(method);
    com.yelp.nrtsearch.server.monitoring.GrpcMethod grpcMethod =
        com.yelp.nrtsearch.server.monitoring.GrpcMethod.of(method);
    ServerCall<R, S> monitoringCall =
        new MonitoringServerCall(call, clock, grpcMethod, metrics, configuration);
    return new MonitoringServerCallListener<>(
        next.startCall(monitoringCall, requestHeaders),
        metrics,
        com.yelp.nrtsearch.server.monitoring.GrpcMethod.of(method));
  }
}
