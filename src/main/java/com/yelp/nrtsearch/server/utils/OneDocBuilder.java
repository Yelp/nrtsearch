package com.yelp.nrtsearch.server.utils;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;

import com.google.gson.Gson;

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
