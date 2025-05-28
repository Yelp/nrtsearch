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
import java.util.Map;

/**
 * Context keys used in NRTSearch gRPC server.
 *
 * <p>These keys are used to store and retrieve metadata related to the request, such as headers.
 */
public class ContextKeys {
  public static final Context.Key<Map<String, String>> NRTSEARCH_HEADER_MAP =
      Context.key("nrtsearch-header-map");
}
