package org.apache.platypus.server;

import org.apache.platypus.server.grpc.*;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeleteAllDocumentsHandler implements Handler<DeleteAllDocumentsRequest, DeleteAllDocumentsResponse> {
    private static final Logger logger = Logger.getLogger(DeleteAllDocumentsHandler.class.getName());

    @Override
    public DeleteAllDocumentsResponse handle(IndexState indexState, DeleteAllDocumentsRequest deleteAllDocumentsRequest) throws DeleteAllDocumentsHandlerException {
        final ShardState shardState = indexState.getShard(0);
        indexState.verifyStarted();
        long gen;
        try {
            gen = shardState.writer.deleteAll();
        } catch (IOException e) {
            logger.log(Level.WARNING, String.format("ThreadId: %s, writer.deleteAll failed", Thread.currentThread().getName() + Thread.currentThread().getId()));
            throw new DeleteAllDocumentsHandlerException(e);
        }
        return DeleteAllDocumentsResponse.newBuilder().setGenId(String.valueOf(gen)).build();
    }

    public static class DeleteAllDocumentsHandlerException extends Handler.HandlerException {

        public DeleteAllDocumentsHandlerException(Throwable err) {
            super(err);
        }

    }

}

