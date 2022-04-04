package com.yelp.nrtsearch.server.luceneserver.custom.request;

import java.text.MessageFormat;

public class RouteNotFoundException extends RuntimeException {

    public RouteNotFoundException(String id) {
        super(MessageFormat.format("No routes found for id {0}", id));
    }

    public RouteNotFoundException(String id, String path) {
        super(MessageFormat.format("Path {0} found for id {}, please have unique paths for routes in a CustomRequestPlugin ", path));
    }
}
