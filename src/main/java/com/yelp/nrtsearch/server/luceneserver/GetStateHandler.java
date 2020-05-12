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
        if (!indexState.isStarted()) {
            throw new IllegalStateException("Index not started yet. Index needs to be started to get its state");
        }
        JsonObject savedState;
        try {
            savedState = indexState.getSaveState();
        } catch (IOException e) {
            logger.error("Could not load state for index " + indexState.name, e);
            throw new GetStateHandlerException(e);
        }
        builder.setResponse(savedState.getAsString());
        return builder.build();
    }

    public static class GetStateHandlerException extends Handler.HandlerException {

        public GetStateHandlerException(Throwable err) {
            super(err);
        }

    }

}
