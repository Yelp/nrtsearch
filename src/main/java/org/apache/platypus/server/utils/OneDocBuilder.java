package org.apache.platypus.server.utils;

import com.google.gson.Gson;
import org.apache.platypus.server.grpc.AddDocumentRequest;

import java.util.List;

public interface OneDocBuilder {

    default void addField(String fieldName, String value, AddDocumentRequest.Builder addDocumentRequestBuilder) {
        AddDocumentRequest.MultiValuedField.Builder multiValuedFieldsBuilder = AddDocumentRequest.MultiValuedField.newBuilder();
        addDocumentRequestBuilder.putFields(fieldName, multiValuedFieldsBuilder.addValue(value).build());
    }

    default void addField(String fieldName, List<String> value, AddDocumentRequest.Builder addDocumentRequestBuilder) {
        AddDocumentRequest.MultiValuedField.Builder multiValuedFieldsBuilder = AddDocumentRequest.MultiValuedField.newBuilder();
        addDocumentRequestBuilder.putFields(fieldName, multiValuedFieldsBuilder.addAllValue(value).build());
    }


    AddDocumentRequest buildOneDoc(String line, Gson gson);
}
