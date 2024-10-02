/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.handler;

import com.google.api.HttpBody;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsHandler extends Handler<Empty, HttpBody> {
  private static final Logger logger = LoggerFactory.getLogger(MetricsHandler.class);
  private static MetricsHandler instance;
  private static PrometheusRegistry prometheusRegistry;

  public MetricsHandler(GlobalState globalState) {
    super(globalState);
  }

  public static void initialize(GlobalState globalState, PrometheusRegistry prometheusRegistry) {
    instance = new MetricsHandler(globalState);
    MetricsHandler.prometheusRegistry = prometheusRegistry;
  }

  public static MetricsHandler getInstance() {
    return instance;
  }

  @Override
  public void handle(Empty request, StreamObserver<HttpBody> responseObserver) {
    try {
      HttpBody reply = process();
      logger.debug("MetricsRequestHandler returned " + reply.toString());
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.warn("error while trying to get metrics", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription("error while trying to get metrics")
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }

  private HttpBody process() throws IOException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
      PrometheusTextFormatWriter prometheusTextFormat = new PrometheusTextFormatWriter(false);
      prometheusTextFormat.write(byteArrayOutputStream, prometheusRegistry.scrape());
      return HttpBody.newBuilder()
          .setContentType("text/plain")
          .setData(ByteString.copyFrom(byteArrayOutputStream.toByteArray()))
          .build();
    }
  }
}
