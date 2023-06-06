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

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

public class ObjectFieldDefTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected Gson gson = new GsonBuilder().serializeNulls().create();

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsObject.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put("hours", List.of(1));
    map.put("zipcode", List.of("94105"));
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put("partner_id", "abcd");
    innerMap.put("partner_name", "efg");
    map.put("partner", innerMap);

    Map<String, Object> pickup1 = new HashMap<>();
    pickup1.put("name", "AAA");
    pickup1.put("hours", List.of(2));

    Map<String, Object> pickup2 = new HashMap<>();
    pickup2.put("name", "BBB");
    pickup2.put("hours", List.of(3));

    Map<String, Object> dummy1 = new HashMap<>(pickup1);
    dummy1.put("test_null", null);

    Map<String, Object> dummy2 = new HashMap<>(pickup2);

    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "real_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("1").build())
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("1").build())
            .putFields(
                "delivery_areas",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue(gson.toJson(map)).build())
            .putFields(
                "pickup_partners",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(gson.toJson(pickup1))
                    .addValue(gson.toJson(pickup2))
                    .build())
            .putFields(
                "dummy_object",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(gson.toJson(dummy1))
                    .addValue(gson.toJson(dummy2))
                    .build())
            .build();
    docs.add(request);

    Map<String, Object> map2 = new HashMap<>();
    map2.put("hours", List.of(2));
    map2.put("zipcode", List.of("ec2y8ne"));
    Map<String, Object> innerMap2 = new HashMap<>();
    innerMap2.put("partner_id", "1234");
    innerMap2.put("partner_name", "567");
    map2.put("partner", innerMap2);
    Map<String, Object> pickup3 = new HashMap<>();
    pickup3.put("name", "AAA");
    pickup3.put("hours", List.of(2));

    Map<String, Object> pickup4 = new HashMap<>();
    pickup4.put("name", "CCC");
    pickup4.put("hours", List.of(3));

    AddDocumentRequest request2 =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "real_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("2").build())
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("2").build())
            .putFields(
                "delivery_areas",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(gson.toJson(map2))
                    .build())
            .putFields(
                "pickup_partners",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(gson.toJson(pickup3))
                    .addValue(gson.toJson(pickup4))
                    .build())
            .build();
    docs.add(request2);

    // add this document to test update
    Map<String, Object> pickup5 = new HashMap<>();
    pickup5.put("name", "EEE");
    pickup5.put("hours", List.of(2));
    AddDocumentRequest request3 =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "real_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("3").build())
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("3").build())
            .putFields(
                "pickup_partners",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(gson.toJson(pickup5))
                    .build())
            .build();

    docs.add(request3);
    addDocuments(docs.stream());
    docs.clear();

    Map<String, Object> pickup7 = new HashMap<>();
    pickup7.put("name", "GGG");
    pickup7.put("hours", List.of(2));

    // add this doc to test delete
    AddDocumentRequest request5 =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "real_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("4").build())
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("4").build())
            .putFields(
                "pickup_partners",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(gson.toJson(pickup7))
                    .build())
            .build();
    docs.add(request5);
    addDocuments(docs.stream());
  }

  @Test
  public void testObjectChildFields() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder()
                        .setField("delivery_areas.zipcode")
                        .setTextValue("94105")
                        .build())
                .build(),
            List.of("doc_id"));
    assertFields(response, "1");
  }

  @Test
  public void testFlattenedNestedObjectChildFields() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder()
                        .setField("delivery_areas.partner.partner_name")
                        .setTextValue("efg")
                        .build())
                .build(),
            List.of("doc_id"));
    assertFields(response, "1");
  }

  @Test
  public void testSimpleNestedDocs() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setNestedQuery(
                    NestedQuery.newBuilder()
                        .setPath("pickup_partners")
                        .setQuery(
                            Query.newBuilder()
                                .setTermQuery(
                                    TermQuery.newBuilder()
                                        .setField("pickup_partners.name")
                                        .setTextValue("BBB")
                                        .build()))
                        .build())
                .build(),
            List.of("doc_id"));
    assertFields(response, "1");
  }

  @Test
  public void testCombinedNestedQuery() {
    Query childQuery =
        Query.newBuilder()
            .setNestedQuery(
                NestedQuery.newBuilder()
                    .setPath("pickup_partners")
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("pickup_partners.name")
                                    .setTextValue("AAA")
                                    .build()))
                    .build())
            .build();

    SearchResponse response1 =
        doQuery(
            Query.newBuilder()
                .setBooleanQuery(
                    BooleanQuery.newBuilder()
                        .addClauses(
                            BooleanClause.newBuilder()
                                .setQuery(childQuery)
                                .setOccur(BooleanClause.Occur.MUST)
                                .build())
                        .addClauses(
                            BooleanClause.newBuilder()
                                .setQuery(
                                    Query.newBuilder()
                                        .setTermQuery(
                                            TermQuery.newBuilder()
                                                .setField("delivery_areas.partner.partner_name")
                                                .setTextValue("567")
                                                .build())
                                        .build())
                                .setOccur(BooleanClause.Occur.MUST)
                                .build())
                        .build())
                .build(),
            List.of("doc_id"));
    assertFields(response1, "2");
  }

  @Test
  public void testUpdateNestedObject() throws Exception {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setNestedQuery(
                    NestedQuery.newBuilder()
                        .setPath("pickup_partners")
                        .setQuery(
                            Query.newBuilder()
                                .setTermQuery(
                                    TermQuery.newBuilder()
                                        .setField("pickup_partners.name")
                                        .setTextValue("EEE")
                                        .build()))
                        .build())
                .build(),
            List.of("doc_id"));
    assertFields(response, "3");

    SearchResponse childResponse =
        doQueryWithNestedPath(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder()
                        .setField("pickup_partners.name")
                        .setTextValue("EEE")
                        .build())
                .build(),
            List.of("pickup_partners.name"),
            "pickup_partners");
    assertDataFields(childResponse, "pickup_partners.name", "EEE");

    // add this document with same id to test update
    Map<String, Object> pickup6 = new HashMap<>();
    pickup6.put("name", "FFF");
    pickup6.put("hours", List.of(2));
    AddDocumentRequest request4 =
        AddDocumentRequest.newBuilder()
            .setIndexName(getIndices().get(0))
            .putFields(
                "real_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("3").build())
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("5").build())
            .putFields(
                "pickup_partners",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(gson.toJson(pickup6))
                    .build())
            .build();
    addDocuments(List.of(request4).stream());
    // refresh the index
    getGrpcServer()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(DEFAULT_TEST_INDEX).build());

    // query parent doc with previous child doc value
    response =
        doQuery(
            Query.newBuilder()
                .setNestedQuery(
                    NestedQuery.newBuilder()
                        .setPath("pickup_partners")
                        .setQuery(
                            Query.newBuilder()
                                .setTermQuery(
                                    TermQuery.newBuilder()
                                        .setField("pickup_partners.name")
                                        .setTextValue("EEE")
                                        .build()))
                        .build())
                .build(),
            List.of("doc_id"));
    // no result
    assertFields(response);

    // query child doc with previous child doc value
    childResponse =
        doQueryWithNestedPath(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder()
                        .setField("pickup_partners.name")
                        .setTextValue("EEE")
                        .build())
                .build(),
            List.of("pickup_partners.name"),
            "pickup_partners");
    // no result
    assertDataFields(childResponse, "pickup_partners.name");

    // query parent doc with new child doc value
    response =
        doQuery(
            Query.newBuilder()
                .setNestedQuery(
                    NestedQuery.newBuilder()
                        .setPath("pickup_partners")
                        .setQuery(
                            Query.newBuilder()
                                .setTermQuery(
                                    TermQuery.newBuilder()
                                        .setField("pickup_partners.name")
                                        .setTextValue("FFF")
                                        .build()))
                        .build())
                .build(),
            List.of("doc_id"));
    // get the new parent doc
    assertFields(response, "5");
    // query parent doc with new child doc value
    childResponse =
        doQueryWithNestedPath(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder()
                        .setField("pickup_partners.name")
                        .setTextValue("FFF")
                        .build())
                .build(),
            List.of("pickup_partners.name"),
            "pickup_partners");
    // get new child doc
    assertDataFields(childResponse, "pickup_partners.name", "FFF");
  }

  @Test
  public void testDeleteNestedObject() {
    // query parent doc with child doc value
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setNestedQuery(
                    NestedQuery.newBuilder()
                        .setPath("pickup_partners")
                        .setQuery(
                            Query.newBuilder()
                                .setTermQuery(
                                    TermQuery.newBuilder()
                                        .setField("pickup_partners.name")
                                        .setTextValue("GGG")
                                        .build()))
                        .build())
                .build(),
            List.of("doc_id"));
    // get the parent doc id
    assertFields(response, "4");

    // query child doc by child doc value
    SearchResponse childResponse =
        doQueryWithNestedPath(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder()
                        .setField("pickup_partners.name")
                        .setTextValue("GGG")
                        .build())
                .build(),
            List.of("pickup_partners.name"),
            "pickup_partners");
    // get matching child doc
    assertDataFields(childResponse, "pickup_partners.name", "GGG");

    // delete the last document
    AddDocumentRequest deleteRequest =
        AddDocumentRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .putFields(
                "real_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("4").build())
            .build();
    AddDocumentResponse addDocumentResponse =
        getGrpcServer().getBlockingStub().delete(deleteRequest);
    // refresh the index
    getGrpcServer()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(DEFAULT_TEST_INDEX).build());

    // query the parent doc by the child doc value
    SearchResponse response2 =
        doQuery(
            Query.newBuilder()
                .setNestedQuery(
                    NestedQuery.newBuilder()
                        .setPath("pickup_partners")
                        .setQuery(
                            Query.newBuilder()
                                .setTermQuery(
                                    TermQuery.newBuilder()
                                        .setField("pickup_partners.name")
                                        .setTextValue("GGG")
                                        .build()))
                        .build())
                .build(),
            List.of("doc_id"));
    // no result
    assertFields(response2);
    // query child doc by child doc value
    SearchResponse childResponse2 =
        doQueryWithNestedPath(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder()
                        .setField("pickup_partners.name")
                        .setTextValue("GGG")
                        .build())
                .build(),
            List.of("pickup_partners.name"),
            "pickup_partners");
    // no result
    assertDataFields(childResponse2, "pickup_partners.name");
  }

  @Test
  public void testQueryNestedPath() {
    SearchResponse response =
        doQueryWithNestedPath(
            Query.newBuilder()
                .setTermQuery(TermQuery.newBuilder().setField("real_id").setTextValue("1").build())
                .build(),
            List.of("pickup_partners.name"),
            "pickup_partners");
    assertDataFields(response, "pickup_partners.name", "AAA", "BBB");
  }

  @Test
  public void testQueryNestedPath_notNested() {
    assertThatThrownBy(
            () ->
                doQueryWithNestedPath(
                    Query.newBuilder()
                        .setTermQuery(
                            TermQuery.newBuilder().setField("real_id").setTextValue("1").build())
                        .build(),
                    List.of("pickup_partners.name"),
                    "doc_id"))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("Nested path is not a nested object field: doc_id");
  }

  @Test
  public void testRetrieveObjectField() throws IOException {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setTermQuery(TermQuery.newBuilder().setField("doc_id").setTextValue("1").build())
                .build(),
            List.of("dummy_object", "delivery_areas"));
    assertEquals(1, response.getHitsCount());
    SearchResponse.Hit hit = response.getHits(0);
    assertEquals(2, hit.getFieldsOrThrow("dummy_object").getFieldValueCount());

    assertEquals(
        Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build(),
        hit.getFieldsOrThrow("dummy_object")
            .getFieldValue(0)
            .getStructValue()
            .getFieldsOrThrow("test_null"));

    Struct delivery_areas =
        hit.getFieldsOrThrow("delivery_areas").getFieldValue(0).getStructValue();
    assertEquals(
        "94105",
        delivery_areas.getFieldsOrThrow("zipcode").getListValue().getValues(0).getStringValue());
    delivery_areas.getFieldsOrThrow("hours").getListValue().getValues(0).getNumberValue();
    assertEquals(
        "abcd",
        delivery_areas
            .getFieldsOrThrow("partner")
            .getStructValue()
            .getFieldsOrThrow("partner_id")
            .getStringValue());
  }

  private SearchResponse doQuery(Query query, List<String> fields) {
    return doQueryWithNestedPath(query, fields, "");
  }

  private SearchResponse doQueryWithNestedPath(
      Query query, List<String> fields, String queryNestedPath) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addAllRetrieveFields(fields)
                .setQuery(query)
                .setQueryNestedPath(queryNestedPath)
                .build());
  }

  private void assertFields(SearchResponse response, String... expectedIds) {
    assertDataFields(response, "doc_id", expectedIds);
  }

  private void assertDataFields(
      SearchResponse response, String fieldName, String... expectedValues) {
    Set<String> seenSet = new HashSet<>();
    for (SearchResponse.Hit hit : response.getHitsList()) {
      String id = hit.getFieldsOrThrow(fieldName).getFieldValue(0).getTextValue();
      seenSet.add(id);
    }
    Set<String> expectedSet = new HashSet<>(Arrays.asList(expectedValues));
    assertEquals(seenSet, expectedSet);
  }
}
