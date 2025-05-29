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
package com.yelp.nrtsearch.server.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.HashMap;
import java.util.Map;

/**
 * Interceptor to extract headers starting with "nrtsearch-" from gRPC calls and store them in the
 * gRPC context.
 *
 * <p>This allows for passing custom metadata through the gRPC call, which can be accessed later in
 * the request processing pipeline.
 */
public class NrtsearchHeaderInterceptor implements ServerInterceptor {
  private static final String HEADER_PREFIX = "nrtsearch-";

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    // Extract headers that start with "nrtsearch-" and do not end with
    // Metadata.BINARY_HEADER_SUFFIX
    Map<String, String> matchedHeaders = new HashMap<>();
    for (String keyName : headers.keys()) {
      if (keyName.startsWith(HEADER_PREFIX) && !keyName.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
        Metadata.Key<String> stringKey = Metadata.Key.of(keyName, Metadata.ASCII_STRING_MARSHALLER);
        String value = headers.get(stringKey);
        if (value != null) {
          matchedHeaders.put(keyName, value);
        }
      }
    }
    // Attach the map to the gRPC context
    Context ctx = Context.current().withValue(ContextKeys.NRTSEARCH_HEADER_MAP, matchedHeaders);
    return Contexts.interceptCall(ctx, call, headers, next);
  }
}
