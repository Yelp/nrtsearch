package com.yelp.nrtsearch.server.monitoring;

import io.grpc.ForwardingServerCallListener;
import io.grpc.ServerCall;

/**
 * A {@link ForwardingServerCallListener} which updates Prometheus metrics for a single rpc based
 * on updates received from grpc.
 */
class MonitoringServerCallListener<R> extends ForwardingServerCallListener<R> {
  private final ServerCall.Listener<R> delegate;
  private final com.yelp.nrtsearch.server.monitoring.GrpcMethod grpcMethod;
  private final com.yelp.nrtsearch.server.monitoring.ServerMetrics serverMetrics;

  MonitoringServerCallListener(
      ServerCall.Listener<R> delegate, com.yelp.nrtsearch.server.monitoring.ServerMetrics serverMetrics, com.yelp.nrtsearch.server.monitoring.GrpcMethod grpcMethod) {
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
