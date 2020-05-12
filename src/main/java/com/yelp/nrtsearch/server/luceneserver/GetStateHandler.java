package com.yelp.nrtsearch.server.luceneserver;

import com.google.gson.JsonObject;
import com.yelp.nrtsearch.server.grpc.StateRequest;
import com.yelp.nrtsearch.server.grpc.StateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GetStateHandler implements Handler<StateRequest, StateResponse> {
    Logger logger = LoggerFactory.getLogger(GetStateHandler.class);

    @Override
    public StateResponse handle(IndexState indexState, StateRequest stateRequest) throws HandlerException {
        StateResponse.Builder builder = StateResponse.newBuilder();
        JsonObject savedState = new JsonObject();
        try {
            savedState.add("state", indexState.getSaveState());
        } catch (IOException e) {
            logger.error("Could not load state for index " + indexState.name, e);
            throw new GetStateHandlerException(e);
        }
        builder.setResponse(savedState.toString());
        return builder.build();
    }

    public static class GetStateHandlerException extends Handler.HandlerException {

        public GetStateHandlerException(Throwable err) {
            super(err);
        }

    }

}
