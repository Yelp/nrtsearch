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
package com.yelp.nrtsearch.server.luceneserver.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.BooleanClause;
import com.yelp.nrtsearch.server.grpc.BooleanQuery;
import com.yelp.nrtsearch.server.grpc.DeleteByQueryRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RefreshRequest;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.utils.StructValueTransformer;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Tests for multivalued objects. These tests verify that: 1. Multivalued objects can be indexed,
 * updated, deleted, retrieved and queried 2. The objects order is not defined in the response 3.
 * Objects that don't have fields defined cannot be queried 4. Multivalued objects need to have all
 * child fields multivalued as well
 *
 * <p>The test documents have all integer type fields defined as doubles to make comparison with the
 * response easier since the response sets integer values in the double field.
 */
public class MultivaluedObjectTest extends ServerTestCase {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Map<Integer, String> MULTIVALUED_OBJECTS_JSON;

  private static final Map<Integer, Map<String, Object>> MULTIVALUED_OBJECTS;

  static {
    MULTIVALUED_OBJECTS_JSON = new HashMap<>();
    MULTIVALUED_OBJECTS = new HashMap<>();

    for (int i = 1; i <= 13; i++) {
      String fileName = String.format("object%d.json", i);
      MULTIVALUED_OBJECTS_JSON.put(i, readObjectFromResource(fileName));
      MULTIVALUED_OBJECTS.put(i, convertJsonToMap(getMultivaluedObject(i)));
    }
  }

  private static final Map<Integer, List<Map<String, Object>>> DOC_ID_TO_MULTIVALUED_OBJECTS =
      Map.of(
          1,
          List.of(MULTIVALUED_OBJECTS.get(1)),
          2,
          List.of(
              MULTIVALUED_OBJECTS.get(2), MULTIVALUED_OBJECTS.get(3), MULTIVALUED_OBJECTS.get(4)),
          3,
          List.of(
              MULTIVALUED_OBJECTS.get(5), MULTIVALUED_OBJECTS.get(6), MULTIVALUED_OBJECTS.get(7)),
          4,
          List.of(MULTIVALUED_OBJECTS.get(8), MULTIVALUED_OBJECTS.get(9)),
          5,
          List.of(MULTIVALUED_OBJECTS.get(12), MULTIVALUED_OBJECTS.get(13)));

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsMultivaluedObject.json");
  }

  private static String readObjectFromResource(String fileName) {
    String resourceFileName = "/field/multivalued_object/" + fileName;
    InputStream fileStream = ServerTestCase.class.getResourceAsStream(resourceFileName);
    return new BufferedReader(new InputStreamReader(fileStream, StandardCharsets.UTF_8))
        .lines()
        .collect(Collectors.joining(System.lineSeparator()));
  }

  private static Map<String, Object> convertJsonToMap(String json) {
    TypeReference<HashMap<String, Object>> typeRef = new TypeReference<>() {};
    try {
      return OBJECT_MAPPER.readValue(json, typeRef);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getMultivaluedObject(int id) {
    return MULTIVALUED_OBJECTS_JSON.get(id);
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs =
        List.of(
            buildAddDocumentRequest(name, "1", 1),
            buildAddDocumentRequest(name, "2", 2, 3, 4),
            buildAddDocumentRequest(name, "3", 5, 6, 7),
            buildAddDocumentRequest(name, "4", 8, 9),
            buildAddDocumentRequest(name, "5", 12, 13));
    addDocuments(docs.stream());
  }

  private AddDocumentRequest buildAddDocumentRequest(
      String indexName, String docId, int... objectIds) {
    AddDocumentRequest.MultiValuedField.Builder builder =
        AddDocumentRequest.MultiValuedField.newBuilder();
    for (int objectId : objectIds) {
      builder.addValue(getMultivaluedObject(objectId));
    }
    return AddDocumentRequest.newBuilder()
        .setIndexName(indexName)
        .putFields(
            "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue(docId).build())
        .putFields("multivalued_object", builder.build())
        .build();
  }

  @Test
  public void testQueryObjectFieldsAndRetrieval() {
    List<List> inputsAndExpectedOutput =
        List.of(
            List.of("doc_id", "1", 1),
            List.of("multivalued_object.field1", "11", 1),
            List.of("multivalued_object.field1", "22", 2),
            List.of("multivalued_object.field2", "object3_field2", 2),
            List.of("multivalued_object.field2", "object4_field2_c", 2),
            List.of(
                "multivalued_object.inner_object1.inner_field1",
                "object5_inner_object1_a_inner_field1",
                3),
            List.of(
                "multivalued_object.inner_object1.inner_field2",
                "object6_inner_object1_b_inner_field2",
                3),
            List.of(
                "multivalued_object.inner_object1.inner_field1",
                "object7_inner_object1_c_inner_field1_b",
                3),
            List.of(
                "multivalued_object.inner_object1.inner_field2",
                "object7_inner_object1_b_inner_field2_c",
                3));

    for (List inputAndExpectedOutput : inputsAndExpectedOutput) {
      String field = (String) inputAndExpectedOutput.get(0);
      String value = (String) inputAndExpectedOutput.get(1);
      int docId = (int) inputAndExpectedOutput.get(2);

      SearchResponse response = doQuery(buildTermQuery(field, value));

      assertEquals(1, response.getHitsCount());
      SearchResponse.Hit hit = response.getHits(0);
      assertEquals(
          String.valueOf(docId), hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue());

      assertEquals(
          new HashSet<>(DOC_ID_TO_MULTIVALUED_OBJECTS.get(docId)),
          new HashSet<>(getMultivaluedObjectsFromHit(hit)));
    }
  }

  @Test
  public void testQueryMultipleObjectFields() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setBooleanQuery(
                    BooleanQuery.newBuilder()
                        .addClauses(
                            BooleanClause.newBuilder()
                                .setOccur(BooleanClause.Occur.SHOULD)
                                .setQuery(
                                    buildTermQuery("multivalued_object.field1", "22")
                                        .toBuilder()
                                        .setBoost(2))
                                .build())
                        .addClauses(
                            BooleanClause.newBuilder()
                                .setOccur(BooleanClause.Occur.SHOULD)
                                .setQuery(
                                    buildTermQuery(
                                        "multivalued_object.inner_object1.inner_field2",
                                        "object6_inner_object1_b_inner_field2"))
                                .build())
                        .build())
                .build());

    assertEquals(2, response.getHitsCount());

    SearchResponse.Hit hit = response.getHits(0);
    assertEquals("2", hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue());
    assertEquals(
        new HashSet<>(DOC_ID_TO_MULTIVALUED_OBJECTS.get(2)),
        new HashSet<>(getMultivaluedObjectsFromHit(hit)));

    hit = response.getHits(1);
    assertEquals("3", hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue());
    assertEquals(
        new HashSet<>(DOC_ID_TO_MULTIVALUED_OBJECTS.get(3)),
        new HashSet<>(getMultivaluedObjectsFromHit(hit)));
  }

  @Test
  public void testMultivaluedObjectUpdate() throws Exception {
    // Test uses document with doc_id 4
    AddDocumentRequest addDocumentRequest =
        buildAddDocumentRequest(DEFAULT_TEST_INDEX, "4", 10, 11);
    addDocuments(Stream.of(addDocumentRequest));
    getGrpcServer()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(DEFAULT_TEST_INDEX).build());

    SearchResponse response =
        doQuery(
            buildTermQuery(
                "multivalued_object.inner_object1.inner_field2", "object10_inner_field2"));

    assertEquals(1, response.getHitsCount());
    SearchResponse.Hit hit = response.getHits(0);
    assertEquals("4", hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue());
    HashSet<Map<String, Object>> expectedObjects =
        new HashSet<>(List.of(MULTIVALUED_OBJECTS.get(10), MULTIVALUED_OBJECTS.get(11)));
    assertEquals(expectedObjects, new HashSet<>(getMultivaluedObjectsFromHit(hit)));
  }

  @Test
  public void testMultivaluedObjectDelete() {
    // Test uses document with doc_id 5
    SearchResponse response = doQuery(buildTermQuery("doc_id", "5"));

    assertEquals(1, response.getHitsCount());
    SearchResponse.Hit hit = response.getHits(0);
    assertEquals("5", hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue());
    assertEquals(
        new HashSet<>(DOC_ID_TO_MULTIVALUED_OBJECTS.get(5)),
        new HashSet<>(getMultivaluedObjectsFromHit(hit)));

    getGrpcServer()
        .getBlockingStub()
        .deleteByQuery(
            DeleteByQueryRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .addQuery(
                    buildTermQuery(
                        "multivalued_object.inner_object1.inner_field1",
                        "object13_inner_object1_b_inner_field1_b"))
                .build());

    getGrpcServer()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(DEFAULT_TEST_INDEX).build());

    response = doQuery(buildTermQuery("doc_id", "5"));

    assertEquals(0, response.getHitsCount());
  }

  @Test
  public void testSearchObjectWithoutDefinedFields() {
    StatusRuntimeException exception = null;
    try {
      doQuery(buildTermQuery("multivalued_object.inner_object1.inner_object2.object", "anything"));
    } catch (StatusRuntimeException e) {
      exception = e;
    }
    assertTrue(
        exception
            .getMessage()
            .contains(
                "field \"multivalued_object.inner_object1.inner_object2.object\" is unknown: it was not registered with registerField"));

    exception = null;
    try {
      doQuery(buildTermQuery("multivalued_object.inner_object1.inner_object2", "object"));
    } catch (StatusRuntimeException e) {
      exception = e;
    }
    assertTrue(
        exception.getMessage().contains("field type: OBJECT is not supported for TermQuery"));
  }

  private Query buildTermQuery(String doc_id, String s) {
    return Query.newBuilder()
        .setTermQuery(TermQuery.newBuilder().setField(doc_id).setTextValue(s).build())
        .build();
  }

  private SearchResponse doQuery(Query query) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addAllRetrieveFields(List.of("doc_id", "multivalued_object"))
                .setQuery(query)
                .build());
  }

  private List<Map<String, Object>> getMultivaluedObjectsFromHit(SearchResponse.Hit hit) {
    return convertMultivaluedObjectsFromResponseToMap(hit.getFieldsMap().get("multivalued_object"));
  }

  private List<Map<String, Object>> convertMultivaluedObjectsFromResponseToMap(
      SearchResponse.Hit.CompositeFieldValue multivaluedObject) {
    List<Map<String, Object>> multivaluedObjects = new ArrayList<>();
    for (SearchResponse.Hit.FieldValue object : multivaluedObject.getFieldValueList()) {
      multivaluedObjects.add(StructValueTransformer.transformStruct(object.getStructValue()));
    }
    return multivaluedObjects;
  }
}
