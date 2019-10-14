package org.apache.platypus.server;

import org.apache.platypus.server.grpc.DeleteIndexRequest;
import org.apache.platypus.server.grpc.DeleteIndexResponse;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeleteIndexHandler implements  Handler<DeleteIndexRequest, DeleteIndexResponse> {
    private static final Logger logger = Logger.getLogger(DeleteIndexHandler.class.getName());
    @Override
    public DeleteIndexResponse handle(IndexState indexState, DeleteIndexRequest protoRequest) throws DeleteIndexHandlerException {
        try {
            indexState.close();
            indexState.deleteIndex();
        } catch (IOException e) {
            logger.log(Level.WARNING, String.format("ThreadId: %s, deleteIndex failed", Thread.currentThread().getName() + Thread.currentThread().getId()));
            throw new DeleteIndexHandlerException(e);
        }
        return DeleteIndexResponse.newBuilder().setOk("ok").build();
    }

    public static class DeleteIndexHandlerException extends Handler.HandlerException {

        public DeleteIndexHandlerException(Throwable err) {
            super(err);
        }

    }


}
