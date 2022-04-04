package com.yelp.nrtsearch.server.luceneserver.custom.request;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.CustomRequest;
import com.yelp.nrtsearch.server.grpc.CustomResponse;
import com.yelp.nrtsearch.server.plugins.CustomRequestPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomRequestProcessor {

    private static CustomRequestProcessor instance;
    private Map<String, Map<String, CustomRequestPlugin.Route>> routeMapping = new HashMap<>();

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
        Map<String, CustomRequestPlugin.Route> routesForId = instance.routeMapping.get(request.getId());
        if (!routesForId.containsKey(request.getPath())) {
            throw new RouteNotFoundException(request.getId(), request.getPath());
        }
        Map<String, String> response = routesForId.get(request.getPath()).process(request.getParamsMap());
        return CustomResponse.newBuilder()
                .putAllResponse(response)
                .build();
    }

    private void registerRoutes(CustomRequestPlugin plugin) {
        String id = plugin.id();
        List<CustomRequestPlugin.Route> routes = plugin.getRoutes();
        if (routeMapping.containsKey(id)) {
            throw new DuplicateRouteException(id);
        }
        Map<String, CustomRequestPlugin.Route> routesForId = new HashMap<>();
        for (CustomRequestPlugin.Route route : routes) {
            if (routesForId.containsKey(route.path())) {
                throw new DuplicateRouteException(id, route.path());
            }
            routesForId.put(route.path(), route);
        }
        routeMapping.put(id, routesForId);
    }
}
