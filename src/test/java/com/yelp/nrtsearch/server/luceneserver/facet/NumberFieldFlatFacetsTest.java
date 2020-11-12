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
package com.yelp.nrtsearch.server.luceneserver.facet;

import static org.junit.Assert.assertNotNull;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Test;

public class NumberFieldFlatFacetsTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final List<String> fields =
      Arrays.asList(
          new String[] {
            "int_number_facet_field",
            "float_number_facet_field",
            "long_number_facet_field",
            "double_number_facet_field"
          });
  private static final List<String> numericValues =
      Arrays.asList(new String[] {"1", "10", "20", "30"});

  private Map<String, AddDocumentRequest.MultiValuedField> getFieldsMapForOneDocument(
      String value) {
    Map<String, AddDocumentRequest.MultiValuedField> fieldsMap = new HashMap<>();
    for (String field : fields) {
      fieldsMap.put(
          field, AddDocumentRequest.MultiValuedField.newBuilder().addValue(value).build());
    }
    return fieldsMap;
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/facet/number_field_flat_facets.json");
  }

  @Override
  protected boolean shouldInitializeIndicesAsPrimary() {
    return true;
  }

  @Test
  public void addDocuments() throws InterruptedException {
    List<AddDocumentRequest> documentRequests = new ArrayList<>();
    for (String value : numericValues) {
      documentRequests.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(DEFAULT_TEST_INDEX)
              .putAllFields(getFieldsMapForOneDocument(value))
              .build());
    }
    assertNotNull(addDocuments(documentRequests.stream()));
  }
}
