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

import com.google.common.primitives.Floats;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue.Vector;
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
    return getGrpcServer().getGlobalState().getIndex(testIndex).getField(fieldName);
  }

  @Test
  public void validVectorFieldDefTest() throws IOException {
    FieldDef vectorFieldDef = getFieldDef(DEFAULT_TEST_INDEX, FIELD_NAME);
    assertEquals(FIELD_TYPE, vectorFieldDef.getType());
    assertEquals(FIELD_NAME, vectorFieldDef.getName());

    float[] expectedVector1 = {1.0f, 2.5f, 1000.1000f};
    float[] expectedVector2 = {0.1f, -2.0f, 5.6f};

    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .addRetrieveFields(FIELD_NAME)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(Query.newBuilder().build())
                    .build());
    Vector fieldValue1 =
        searchResponse.getHits(0).getFieldsOrThrow(FIELD_NAME).getFieldValue(0).getVectorValue();
    Vector fieldValue2 =
        searchResponse.getHits(1).getFieldsOrThrow(FIELD_NAME).getFieldValue(0).getVectorValue();
    assertEquals(Floats.asList(expectedVector1), fieldValue1.getValueList());
    assertEquals(Floats.asList(expectedVector2), fieldValue2.getValueList());
  }

  @Test
  public void vectorDimensionMismatchTest() {
    List<String> vectorFields = Arrays.asList("[1.0, 2.5]");
    List<AddDocumentRequest> documents = buildDocuments(DEFAULT_TEST_INDEX, vectorFields);
    Exception exception =
        Assert.assertThrows(RuntimeException.class, () -> addDocuments(documents.stream()));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "The size of the vector data: 2 should match vectorDimensions field property: 3"));
  }

  @Test
  public void vectorFieldMultiValueExceptionTest() {
    List<AddDocumentRequest> documentRequests = new ArrayList<>();
    documentRequests.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .putFields(
                FIELD_NAME,
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addAllValue(VECTOR_FIELD_VALUES)
                    .build())
            .build());
    Exception exception =
        Assert.assertThrows(RuntimeException.class, () -> addDocuments(documentRequests.stream()));
    assertTrue(
        exception
            .getMessage()
            .contains("Cannot index multiple values into single value field: vector_field"));
  }

  @Test
  public void parseVectorFieldToFloatArrTest() throws Exception {
    float[] expected = {1.0f, 2.5f, 1000.1000f};

    List<String> vectorFields = Arrays.asList("[1.0, 2.5, 1000.1000]");
    List<AddDocumentRequest> documents = buildDocuments(DEFAULT_TEST_INDEX, vectorFields);
    addDocuments(documents.stream());

    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .addRetrieveFields(FIELD_NAME)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(Query.newBuilder().build())
                    .build());
    Vector fieldValue =
        searchResponse.getHits(0).getFieldsOrThrow(FIELD_NAME).getFieldValue(0).getVectorValue();
    assertEquals(Floats.asList(expected), fieldValue.getValueList());
  }

  @Test
  public void parseVectorFieldToFloatArrFailTest() {
    List<String> invalidJsonList = Arrays.asList("[a, b, c]");
    List<AddDocumentRequest> documents = buildDocuments(DEFAULT_TEST_INDEX, invalidJsonList);
    Exception exception =
        Assert.assertThrows(RuntimeException.class, () -> addDocuments(documents.stream()));
    assertTrue(exception.getMessage().contains("For input string: \"a\""));
  }

  @Test
  public void vectorStoreRequestFailTest() {
    Exception exception =
        Assert.assertThrows(
            RuntimeException.class,
            () ->
                getGrpcServer()
                    .getBlockingStub()
                    .updateFields(
                        FieldDefRequest.newBuilder()
                            .setIndexName(DEFAULT_TEST_INDEX)
                            .addField(
                                Field.newBuilder()
                                    .setName("vector_field_store")
                                    .setType(FieldType.VECTOR)
                                    .setStoreDocValues(true)
                                    .setStore(true)
                                    .build())
                            .build()));
    assertTrue(exception.getMessage().contains("Vector fields cannot be stored"));
  }

  @Test
  public void vectorSearchRequestFailTest() {
    Exception exception =
        Assert.assertThrows(
            RuntimeException.class,
            () ->
                getGrpcServer()
                    .getBlockingStub()
                    .updateFields(
                        FieldDefRequest.newBuilder()
                            .setIndexName(DEFAULT_TEST_INDEX)
                            .addField(
                                Field.newBuilder()
                                    .setName("vector_field_search")
                                    .setType(FieldType.VECTOR)
                                    .setStoreDocValues(true)
                                    .setSearch(true)
                                    .build())
                            .build()));
    assertTrue(exception.getMessage().contains("Vector fields cannot be searched"));
  }

  @Test
  public void vectorMultiValueRequestFailTest() {
    Exception exception =
        Assert.assertThrows(
            RuntimeException.class,
            () ->
                getGrpcServer()
                    .getBlockingStub()
                    .updateFields(
                        FieldDefRequest.newBuilder()
                            .setIndexName(DEFAULT_TEST_INDEX)
                            .addField(
                                Field.newBuilder()
                                    .setName("vector_field_multi_value")
                                    .setType(FieldType.VECTOR)
                                    .setStoreDocValues(true)
                                    .setMultiValued(true)
                                    .build())
                            .build()));
    assertTrue(exception.getMessage().contains("Vector fields cannot be multivalued"));
  }

  @Test
  public void vectorInvalidDimensionRequestFailTest() {
    Exception exception =
        Assert.assertThrows(
            RuntimeException.class,
            () ->
                getGrpcServer()
                    .getBlockingStub()
                    .updateFields(
                        FieldDefRequest.newBuilder()
                            .setIndexName(DEFAULT_TEST_INDEX)
                            .addField(
                                Field.newBuilder()
                                    .setName("vector_field_missing_dimensions")
                                    .setType(FieldType.VECTOR)
                                    .setStoreDocValues(true)
                                    .build())
                            .build()));
    assertTrue(exception.getMessage().contains("Vector dimension should be > 0"));
  }
}
