package com.yelp.nrtsearch.server.luceneserver.custom.request;

import java.text.MessageFormat;

public class DuplicateRouteException extends RuntimeException {

    public DuplicateRouteException(String id) {
        super(MessageFormat.format("Multiple custom request plugins with id {0} found, please have unique ids in plugins ", id));
    }

    public DuplicateRouteException(String id, String path) {
        super(MessageFormat.format("Duplicate path {0} found for id {1}, please have unique paths for routes in a CustomRequestPlugin ", path));
    }
}
