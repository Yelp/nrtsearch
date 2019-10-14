package org.apache.platypus.server;

import com.google.protobuf.ProtocolStringList;
import org.apache.lucene.index.Term;
import org.apache.platypus.server.grpc.AddDocumentRequest;
import org.apache.platypus.server.grpc.AddDocumentResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeleteDocumentsHandler implements Handler<AddDocumentRequest, AddDocumentResponse> {
    private static final Logger logger = Logger.getLogger(DeleteDocumentsHandler.class.getName());
    @Override
    public AddDocumentResponse handle(IndexState indexState, AddDocumentRequest addDocumentRequest) throws DeleteDocumentsHandlerException {
        final ShardState shardState = indexState.getShard(0);
        indexState.verifyStarted();

        Map<String, AddDocumentRequest.MultiValuedField> fields = addDocumentRequest.getFieldsMap();
        List<Term> terms = new ArrayList<>();
        for (Map.Entry<String, AddDocumentRequest.MultiValuedField> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            AddDocumentRequest.MultiValuedField multiValuedField = entry.getValue();
            ProtocolStringList fieldValues = multiValuedField.getValueList();
            for (String fieldValue : fieldValues) {
                // TODO: how to allow arbitrary binary keys?  how to
                // pass binary data via json...?  byte array?
                terms.add(new Term(fieldName, fieldValue));
            }
        }
        try {
            shardState.writer.deleteDocuments(terms.stream().toArray(Term[]::new));
        } catch (IOException e) {
            logger.log(Level.WARNING, String.format("ThreadId: %s, writer.deleteDocuments failed", Thread.currentThread().getName() + Thread.currentThread().getId()));
            throw new DeleteDocumentsHandlerException(e);
        }
        long genId = shardState.writer.getMaxCompletedSequenceNumber();
        return AddDocumentResponse.newBuilder().setGenId(String.valueOf(genId)).build();
    }

    public static class DeleteDocumentsHandlerException extends Handler.HandlerException {

        public DeleteDocumentsHandlerException(Throwable err) {
            super(err);
        }

    }

}
