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
import io.grpc.MethodDescriptor.MethodType;

/** Knows how to extract information about a single grpc method. */
class GrpcMethod {
  private final String serviceName;
  private final String methodName;
  private final MethodType type;

  static com.yelp.nrtsearch.server.monitoring.GrpcMethod of(MethodDescriptor<?, ?> method) {
    String serviceName = MethodDescriptor.extractFullServiceName(method.getFullMethodName());

    // Full method names are of the form: "full.serviceName/MethodName". We extract the last part.
    String methodName = method.getFullMethodName().substring(serviceName.length() + 1);
    return new com.yelp.nrtsearch.server.monitoring.GrpcMethod(
        serviceName, methodName, method.getType());
  }

  private GrpcMethod(String serviceName, String methodName, MethodType type) {
    this.serviceName = serviceName;
    this.methodName = methodName;
    this.type = type;
  }

  String serviceName() {
    return serviceName;
  }

  String methodName() {
    return methodName;
  }

  String type() {
    return type.toString();
  }

  boolean streamsRequests() {
    return type == MethodType.CLIENT_STREAMING || type == MethodType.BIDI_STREAMING;
  }

  boolean streamsResponses() {
    return type == MethodType.SERVER_STREAMING || type == MethodType.BIDI_STREAMING;
  }
}
