/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.plugins;

import java.util.Map;

/**
 * Plugin interface that allows adding one or more routes and their corresponding processors which
 * can be called via the `custom` RPC. This can be implemented by plugins to modify behavior without
 * requiring updating config files and restarting nrtsearch.
 */
public interface CustomRequestPlugin {

  @FunctionalInterface
  interface RequestProcessor {
    /**
     * Defines how a custom request must be processed.
     *
     * @param path Path from {@link com.yelp.nrtsearch.server.grpc.CustomRequest}
     * @param request Parameters sent in the request
     * @return response as a {@code Map<String,String>}
     */
    Map<String, String> process(String path, Map<String, String> request);
  }

  String id();

  Map<String, RequestProcessor> getRoutes();
}
