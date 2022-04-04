package com.yelp.nrtsearch.server.plugins;

import java.util.List;
import java.util.Map;

/**
 * Plugin interface that allows adding one or more routes and their corresponding processors
 * which can be called via the `custom` RPC. This can be implemented by plugins to modify behavior
 * without requiring updating config files and restarting nrtsearch.
 */
public interface CustomRequestPlugin {

    interface Route {

        String path();

        Map<String, String> process(Map<String, String> request);
    }

    String id();

    List<Route> getRoutes();
}
