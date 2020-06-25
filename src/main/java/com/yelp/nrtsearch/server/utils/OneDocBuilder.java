/*
 * Copyright 2020 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yelp.nrtsearch.server.utils;

import com.google.gson.Gson;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import java.util.List;

public interface OneDocBuilder {

  default void addField(
      String fieldName, String value, AddDocumentRequest.Builder addDocumentRequestBuilder) {
    AddDocumentRequest.MultiValuedField.Builder multiValuedFieldsBuilder =
        AddDocumentRequest.MultiValuedField.newBuilder();
    addDocumentRequestBuilder.putFields(
        fieldName, multiValuedFieldsBuilder.addValue(value).build());
  }

  default void addField(
      String fieldName, List<String> value, AddDocumentRequest.Builder addDocumentRequestBuilder) {
    AddDocumentRequest.MultiValuedField.Builder multiValuedFieldsBuilder =
        AddDocumentRequest.MultiValuedField.newBuilder();
    addDocumentRequestBuilder.putFields(
        fieldName, multiValuedFieldsBuilder.addAllValue(value).build());
  }

  AddDocumentRequest buildOneDoc(String line, Gson gson);
}
