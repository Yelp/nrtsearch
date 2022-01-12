/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class VectorFieldDefTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final String FIELD_NAME = "vector_field";
  private static final String FIELD_TYPE = "VECTOR";
  private static final List<String> VECTOR_FIELD_VALUES =
      Arrays.asList("[1.0, 2.5, 1000.1000]", "[0.1, -2.0, 5.6]");

  private Map<String, MultiValuedField> getFieldsMapForOneDocument(String value) {
    Map<String, AddDocumentRequest.MultiValuedField> fieldsMap = new HashMap<>();
    fieldsMap.put(
        FIELD_NAME, AddDocumentRequest.MultiValuedField.newBuilder().addValue(value).build());
    return fieldsMap;
  }

  private List<AddDocumentRequest> buildDocuments(
      String indexName, List<String> vectorFieldValues) {
    List<AddDocumentRequest> documentRequests = new ArrayList<>();
    for (String value : vectorFieldValues) {
      documentRequests.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(indexName)
              .putAllFields(getFieldsMapForOneDocument(value))
              .build());
    }
    return documentRequests;
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsVector.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    List<AddDocumentRequest> documents = buildDocuments(name, VECTOR_FIELD_VALUES);
    addDocuments(documents.stream());
  }

  public FieldDef getFieldDef(String testIndex, String fieldName) throws IOException {
    FieldDef fieldDef = getGrpcServer().getGlobalState().getIndex(testIndex).getField(fieldName);
    return fieldDef;
  }

  @Test
  public void vectorFieldDefTest() throws IOException {
    FieldDef vectorFieldDef = getFieldDef(DEFAULT_TEST_INDEX, FIELD_NAME);
    assertEquals(FIELD_TYPE, vectorFieldDef.getType());
    assertEquals(FIELD_NAME, vectorFieldDef.getName());
  }

  @Test
  public void vectorFieldDefDimensionMismatchTest() {
    List<String> vectorFields = Arrays.asList("[1.0, 2.5]");
    List<AddDocumentRequest> documents = buildDocuments(DEFAULT_TEST_INDEX, vectorFields);
    Exception exception =
        Assert.assertThrows(RuntimeException.class, () -> addDocuments(documents.stream()));
    assertTrue(
        exception
            .getMessage()
            .contains("The size of the vector data should match vectorDimensions field property"));
  }

  @Test
  public void parseVectorFieldToFloatArrTest() {
    float[] expected = {1.0f, 2.5f, 1000.1000f};
    String testJson = "[1.0, 2.5, 1000.1000]";

    float[] resultVector = VectorFieldDef.parseVectorFieldToFloatArr(testJson);
    assertTrue(Arrays.equals(expected, resultVector));
  }
}
