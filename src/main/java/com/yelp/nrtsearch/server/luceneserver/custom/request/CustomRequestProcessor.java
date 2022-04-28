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
package com.yelp.nrtsearch.server.luceneserver.custom.request;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.CustomRequest;
import com.yelp.nrtsearch.server.grpc.CustomResponse;
import com.yelp.nrtsearch.server.plugins.CustomRequestPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.util.HashMap;
import java.util.Map;

public class CustomRequestProcessor {

  private static CustomRequestProcessor instance;
  private final Map<String, Map<String, CustomRequestPlugin.RequestProcessor>> routeMapping =
      new HashMap<>();

  public CustomRequestProcessor(LuceneServerConfiguration configuration) {}

  public static void initialize(LuceneServerConfiguration configuration, Iterable<Plugin> plugins) {
    instance = new CustomRequestProcessor(configuration);
    for (Plugin plugin : plugins) {
      if (plugin instanceof CustomRequestPlugin) {
        instance.registerRoutes((CustomRequestPlugin) plugin);
      }
    }
  }

  public static CustomResponse processCustomRequest(CustomRequest request) {
    if (!instance.routeMapping.containsKey(request.getId())) {
      throw new RouteNotFoundException(request.getId());
    }
    Map<String, CustomRequestPlugin.RequestProcessor> routesForId =
        instance.routeMapping.get(request.getId());
    if (!routesForId.containsKey(request.getPath())) {
      throw new RouteNotFoundException(request.getId(), request.getPath());
    }
    Map<String, String> response =
        routesForId.get(request.getPath()).process(request.getPath(), request.getParamsMap());
    return CustomResponse.newBuilder().putAllResponse(response).build();
  }

  private void registerRoutes(CustomRequestPlugin plugin) {
    String id = plugin.id();
    if (routeMapping.containsKey(id)) {
      throw new DuplicateRouteException(id);
    }
    Map<String, CustomRequestPlugin.RequestProcessor> routesForId = new HashMap<>();
    for (Map.Entry<String, CustomRequestPlugin.RequestProcessor> route :
        plugin.getRoutes().entrySet()) {
      String path = route.getKey();
      routesForId.put(path, route.getValue());
    }
    routeMapping.put(id, routesForId);
  }
}
