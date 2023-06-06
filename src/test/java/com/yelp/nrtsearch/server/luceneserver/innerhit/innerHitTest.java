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
package com.yelp.nrtsearch.server.luceneserver.innerhit;

import static org.assertj.core.api.AssertionsForClassTypes.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.*;
import org.junit.ClassRule;
import org.junit.Test;

public class innerHitTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected Gson gson = new GsonBuilder().serializeNulls().create();

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/registerFieldsInnerHit.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();

    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "real_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("1").build())
            .putFields(
                "branch_id",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("101").build())
            .putFields(
                "location",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addAllValue(Arrays.asList("1.234", "1.567"))
                    .build())
            .putFields(
                "street_number",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("6").build())
            .putFields(
                "street_name",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("nice street").build())
            .putFields(
                "employees",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(
                        gson.toJson(
                            Map.of(
                                "employee_id",
                                "12",
                                "name",
                                "Tom",
                                "age",
                                "26",
                                "motto",
                                "I love my work place and my work culture.")))
                    .addValue(
                        gson.toJson(
                            Map.of(
                                "employee_id",
                                "13",
                                "name",
                                "Lily",
                                "age",
                                "45",
                                "motto",
                                "work hard play hard")))
                    .build())
            .putFields(
                "food",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(
                        gson.toJson(
                            Map.of(
                                "price",
                                "1.2",
                                "name",
                                "ice cone",
                                "description",
                                "Best dessert that everyone loves so much.")))
                    .addValue(
                        gson.toJson(
                            Map.of(
                                "price",
                                "1.3",
                                "name",
                                "cheeseburger",
                                "description",
                                "Who doesn't love such a juicy burger. Just love it.")))
                    .build())
            .build();
    docs.add(request);

    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "real_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("2").build())
            .putFields(
                "branch_id",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("102").build())
            .putFields(
                "location",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addAllValue(Arrays.asList("-1.234", "-1.567"))
                    .build())
            .putFields(
                "street_number",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("86").build())
            .putFields(
                "street_name",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("good avenue").build())
            .putFields(
                "employees",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(
                        gson.toJson(
                            Map.of(
                                "employee_id",
                                "48",
                                "name",
                                "Jack",
                                "age",
                                "26",
                                "motto",
                                "I am into the work vibe here.")))
                    .addValue(
                        gson.toJson(
                            Map.of(
                                "employee_id",
                                "19",
                                "name",
                                "Rose",
                                "age",
                                "45",
                                "motto",
                                "No pain no gain.")))
                    .build())
            .putFields(
                "food",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(
                        gson.toJson(
                            Map.of(
                                "price",
                                "1.4",
                                "name",
                                "deluxe ice cone",
                                "description",
                                "Best edition of the most loved ice cone.")))
                    .addValue(
                        gson.toJson(
                            Map.of(
                                "price",
                                "1.1",
                                "name",
                                "hamburger",
                                "description",
                                "No cheese, but in great value. Love its yummy taste.")))
                    .build())
            .build();
    docs.add(request);

    addDocuments(docs.stream());
  }

  @Test
  public void testEmptyParentQuery() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(List.of("branch_id"))
                    .putInnerHits(
                        "menu",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("food")
                            .setStartHit(0)
                            .setTopHits(10)
                            .setInnerQuery(
                                Query.newBuilder()
                                    .setRangeQuery(
                                        RangeQuery.newBuilder()
                                            .setField("food.price")
                                            .setUpper("1.2")))
                            .addAllRetrieveFields(List.of("food.name"))
                            .build())
                    .build());

    assertThat(response.getHitsCount()).isEqualTo(2);

    assertThat(response.getHits(0).getFieldsOrThrow("branch_id").getFieldValue(0).getTextValue())
        .isEqualTo("101");
    assertThat(response.getHits(0).getInnerHitsMap().get("menu").getHitsCount()).isEqualTo(1);
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("ice cone");

    assertThat(response.getHits(1).getFieldsOrThrow("branch_id").getFieldValue(0).getTextValue())
        .isEqualTo("102");
    assertThat(response.getHits(1).getInnerHitsMap().get("menu").getHitsCount()).isEqualTo(1);
    assertThat(
            response
                .getHits(1)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("hamburger");
  }

  @Test
  public void testEmptyChildQuery() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("branch_id").setTextValue("102")))
                    .addAllRetrieveFields(List.of("branch_id"))
                    .putInnerHits(
                        "menu",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("food")
                            .setStartHit(0)
                            .setTopHits(10)
                            .addAllRetrieveFields(List.of("food.name"))
                            .build())
                    .build());

    assertThat(response.getHitsCount()).isEqualTo(1);

    assertThat(response.getHits(0).getFieldsOrThrow("branch_id").getFieldValue(0).getTextValue())
        .isEqualTo("102");
    assertThat(response.getHits(0).getInnerHitsMap().get("menu").getHitsCount()).isEqualTo(2);
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("deluxe ice cone");
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(1)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("hamburger");
  }

  @Test
  public void testEmptyParentAndChildQuery() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(List.of("branch_id"))
                    .putInnerHits(
                        "menu",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("food")
                            .setStartHit(0)
                            .setTopHits(10)
                            .addAllRetrieveFields(List.of("food.name"))
                            .build())
                    .build());

    assertThat(response.getHitsCount()).isEqualTo(2);

    assertThat(response.getHits(0).getFieldsOrThrow("branch_id").getFieldValue(0).getTextValue())
        .isEqualTo("101");
    assertThat(response.getHits(0).getInnerHitsMap().get("menu").getHitsCount()).isEqualTo(2);
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("ice cone");
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(1)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("cheeseburger");

    assertThat(response.getHits(1).getFieldsOrThrow("branch_id").getFieldValue(0).getTextValue())
        .isEqualTo("102");
    assertThat(response.getHits(1).getInnerHitsMap().get("menu").getHitsCount()).isEqualTo(2);
    assertThat(
            response
                .getHits(1)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("deluxe ice cone");
    assertThat(
            response
                .getHits(1)
                .getInnerHitsMap()
                .get("menu")
                .getHits(1)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("hamburger");
  }

  @Test
  public void testBasic() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("branch_id").setTextValue("102")))
                    .addAllRetrieveFields(List.of("branch_id"))
                    .putInnerHits(
                        "menu",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("food")
                            .setStartHit(0)
                            .setTopHits(10)
                            .setInnerQuery(
                                Query.newBuilder()
                                    .setRangeQuery(
                                        RangeQuery.newBuilder()
                                            .setField("food.price")
                                            .setUpper("1.2")))
                            .addAllRetrieveFields(List.of("food.name"))
                            .build())
                    .build());

    assertThat(response.getHitsCount()).isEqualTo(1);

    assertThat(response.getHits(0).getFieldsOrThrow("branch_id").getFieldValue(0).getTextValue())
        .isEqualTo("102");
    assertThat(response.getHits(0).getInnerHitsMap().get("menu").getHitsCount()).isEqualTo(1);
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("hamburger");
  }

  @Test
  public void testTwoInnerHits() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("branch_id").setTextValue("102")))
                    .addAllRetrieveFields(List.of("branch_id"))
                    .putInnerHits(
                        "menu",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("food")
                            .setStartHit(0)
                            .setTopHits(10)
                            .setInnerQuery(
                                Query.newBuilder()
                                    .setRangeQuery(
                                        RangeQuery.newBuilder()
                                            .setField("food.price")
                                            .setUpper("1.2")))
                            .addAllRetrieveFields(List.of("food.name"))
                            .build())
                    .putInnerHits(
                        "staff",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("employees")
                            .setStartHit(0)
                            .setTopHits(10)
                            .setInnerQuery(
                                Query.newBuilder()
                                    .setRangeQuery(
                                        RangeQuery.newBuilder()
                                            .setField("employees.age")
                                            .setLower("30")))
                            .addAllRetrieveFields(List.of("employees.name"))
                            .build())
                    .build());

    assertThat(response.getHitsCount()).isEqualTo(1);

    assertThat(response.getHits(0).getFieldsOrThrow("branch_id").getFieldValue(0).getTextValue())
        .isEqualTo("102");
    assertThat(response.getHits(0).getInnerHitsMap().get("menu").getHitsCount()).isEqualTo(1);
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("hamburger");
    assertThat(response.getHits(0).getInnerHitsMap().get("staff").getHitsCount()).isEqualTo(1);
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("staff")
                .getHits(0)
                .getFieldsOrThrow("employees.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("Rose");
  }

  @Test
  public void testNoNestedQueryPath() {
    assertThatThrownBy(
            () ->
                getGrpcServer()
                    .getBlockingStub()
                    .search(
                        SearchRequest.newBuilder()
                            .setIndexName(DEFAULT_TEST_INDEX)
                            .setStartHit(0)
                            .setTopHits(10)
                            .setQuery(
                                Query.newBuilder()
                                    .setTermQuery(
                                        TermQuery.newBuilder()
                                            .setField("branch_id")
                                            .setTextValue("102")))
                            .addAllRetrieveFields(List.of("branch_id"))
                            .putInnerHits(
                                "menu",
                                InnerHit.newBuilder()
                                    .setStartHit(0)
                                    .setTopHits(10)
                                    .setInnerQuery(
                                        Query.newBuilder()
                                            .setRangeQuery(
                                                RangeQuery.newBuilder()
                                                    .setField("food.price")
                                                    .setUpper("1.2")))
                                    .addAllRetrieveFields(List.of("food.name"))
                                    .build())
                            .build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("queryNestedPath in InnerHit [menu] cannot be empty");
  }

  @Test
  public void test_nestedQueryPath_notRegistered() {
    assertThatThrownBy(
            () ->
                getGrpcServer()
                    .getBlockingStub()
                    .search(
                        SearchRequest.newBuilder()
                            .setIndexName(DEFAULT_TEST_INDEX)
                            .setStartHit(0)
                            .setTopHits(10)
                            .setQuery(
                                Query.newBuilder()
                                    .setTermQuery(
                                        TermQuery.newBuilder()
                                            .setField("branch_id")
                                            .setTextValue("102")))
                            .addAllRetrieveFields(List.of("branch_id"))
                            .putInnerHits(
                                "menu",
                                InnerHit.newBuilder()
                                    .setStartHit(0)
                                    .setTopHits(10)
                                    .setQueryNestedPath("abcdefg")
                                    .setInnerQuery(
                                        Query.newBuilder()
                                            .setRangeQuery(
                                                RangeQuery.newBuilder()
                                                    .setField("food.price")
                                                    .setUpper("1.2")))
                                    .addAllRetrieveFields(List.of("food.name"))
                                    .build())
                            .build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining(
            "field \"abcdefg\" is unknown: it was not registered with registerField");
  }

  @Test
  public void test_nestedQueryPath_notNested() {
    assertThatThrownBy(
            () ->
                getGrpcServer()
                    .getBlockingStub()
                    .search(
                        SearchRequest.newBuilder()
                            .setIndexName(DEFAULT_TEST_INDEX)
                            .setStartHit(0)
                            .setTopHits(10)
                            .setQuery(
                                Query.newBuilder()
                                    .setTermQuery(
                                        TermQuery.newBuilder()
                                            .setField("branch_id")
                                            .setTextValue("102")))
                            .addAllRetrieveFields(List.of("branch_id"))
                            .putInnerHits(
                                "menu",
                                InnerHit.newBuilder()
                                    .setStartHit(0)
                                    .setTopHits(10)
                                    .setQueryNestedPath("food_not_nested")
                                    .setInnerQuery(
                                        Query.newBuilder()
                                            .setRangeQuery(
                                                RangeQuery.newBuilder()
                                                    .setField("food.price")
                                                    .setUpper("1.2")))
                                    .addAllRetrieveFields(List.of("food_not_nested.name"))
                                    .build())
                            .build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("Nested path is not a nested object field: food_not_nested");
  }

  @Test
  public void testStartHit() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("branch_id").setTextValue("102")))
                    .addAllRetrieveFields(List.of("branch_id"))
                    .putInnerHits(
                        "menu",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("food")
                            .setStartHit(1)
                            .setTopHits(10)
                            .addAllRetrieveFields(List.of("food.name"))
                            .build())
                    .build());

    assertThat(response.getHitsCount()).isEqualTo(1);

    assertThat(response.getHits(0).getFieldsOrThrow("branch_id").getFieldValue(0).getTextValue())
        .isEqualTo("102");
    assertThat(response.getHits(0).getInnerHitsMap().get("menu").getHitsCount()).isEqualTo(1);
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("hamburger");
  }

  @Test
  public void testLargeStartHit() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("branch_id").setTextValue("102")))
                    .addAllRetrieveFields(List.of("branch_id"))
                    .putInnerHits(
                        "menu",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("food")
                            .setStartHit(8)
                            .setTopHits(10)
                            .addAllRetrieveFields(List.of("food.name"))
                            .build())
                    .build());

    assertThat(response.getHitsCount()).isEqualTo(1);

    assertThat(response.getHits(0).getFieldsOrThrow("branch_id").getFieldValue(0).getTextValue())
        .isEqualTo("102");
    assertThat(response.getHits(0).getInnerHitsMap().get("menu").getHitsCount()).isEqualTo(0);
  }

  @Test
  public void testTopHits() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("branch_id").setTextValue("102")))
                    .addAllRetrieveFields(List.of("branch_id"))
                    .putInnerHits(
                        "menu",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("food")
                            .setStartHit(0)
                            .setTopHits(1)
                            .addAllRetrieveFields(List.of("food.name"))
                            .build())
                    .build());

    assertThat(response.getHitsCount()).isEqualTo(1);

    assertThat(response.getHits(0).getFieldsOrThrow("branch_id").getFieldValue(0).getTextValue())
        .isEqualTo("102");
    assertThat(response.getHits(0).getInnerHitsMap().get("menu").getHitsCount()).isEqualTo(1);
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("deluxe ice cone");
  }

  @Test
  public void testTopHitsSmallerThanStartHit() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("branch_id").setTextValue("102")))
                    .addAllRetrieveFields(List.of("branch_id"))
                    .putInnerHits(
                        "menu",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("food")
                            .setStartHit(2)
                            .setTopHits(1)
                            .addAllRetrieveFields(List.of("food.name"))
                            .build())
                    .build());

    assertThat(response.getHitsCount()).isEqualTo(1);

    assertThat(response.getHits(0).getFieldsOrThrow("branch_id").getFieldValue(0).getTextValue())
        .isEqualTo("102");
    assertThat(response.getHits(0).getInnerHitsMap().get("menu").getHitsCount()).isEqualTo(0);
  }

  @Test
  public void testRetrieveFields() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("branch_id").setTextValue("102")))
                    .addAllRetrieveFields(List.of("branch_id"))
                    .putInnerHits(
                        "menu",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("food")
                            .setStartHit(0)
                            .setTopHits(1)
                            .addAllRetrieveFields(
                                List.of(
                                    "food.name",
                                    "food.price",
                                    "food.description",
                                    "real_id",
                                    "branch_id"))
                            .build())
                    .build());

    assertThat(response.getHitsCount()).isEqualTo(1);

    assertThat(response.getHits(0).getFieldsOrThrow("branch_id").getFieldValue(0).getTextValue())
        .isEqualTo("102");
    assertThat(response.getHits(0).getInnerHitsMap().get("menu").getHitsCount()).isEqualTo(1);
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("deluxe ice cone");
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("food.price")
                .getFieldValue(0)
                .getDoubleValue())
        .isEqualTo(1.4);
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("food.description")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("Best edition of the most loved ice cone.");

    // parent's field: only _ID field can be retrieved from the innerHit
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("real_id")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("2");

    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("branch_id")
                .getFieldValueCount())
        .isEqualTo(0);
  }

  @Test
  public void testBadRetrieveFields() {
    assertThatThrownBy(
            () ->
                getGrpcServer()
                    .getBlockingStub()
                    .search(
                        SearchRequest.newBuilder()
                            .setIndexName(DEFAULT_TEST_INDEX)
                            .setStartHit(0)
                            .setTopHits(10)
                            .setQuery(
                                Query.newBuilder()
                                    .setTermQuery(
                                        TermQuery.newBuilder()
                                            .setField("branch_id")
                                            .setTextValue("102")))
                            .addAllRetrieveFields(List.of("branch_id"))
                            .putInnerHits(
                                "menu",
                                InnerHit.newBuilder()
                                    .setQueryNestedPath("food")
                                    .setStartHit(0)
                                    .setTopHits(1)
                                    .addAllRetrieveFields(List.of("food.abcdefg"))
                                    .build())
                            .build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("RetrieveFields: food.abcdefg does not exist");
  }

  @Test
  public void testSortedFields() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("branch_id").setTextValue("102")))
                    .addAllRetrieveFields(List.of("branch_id"))
                    .putInnerHits(
                        "menu",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("food")
                            .setStartHit(0)
                            .setTopHits(10)
                            .addAllRetrieveFields(List.of("food.name", "food.price"))
                            .setQuerySort(
                                QuerySortField.newBuilder()
                                    .setFields(
                                        SortFields.newBuilder()
                                            .addSortedFields(
                                                SortType.newBuilder().setFieldName("food.price"))))
                            .build())
                    .build());

    assertThat(response.getHitsCount()).isEqualTo(1);

    assertThat(response.getHits(0).getFieldsOrThrow("branch_id").getFieldValue(0).getTextValue())
        .isEqualTo("102");
    assertThat(response.getHits(0).getInnerHitsMap().get("menu").getHitsCount()).isEqualTo(2);
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("hamburger");
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(0)
                .getFieldsOrThrow("food.price")
                .getFieldValue(0)
                .getDoubleValue())
        .isEqualTo(1.1);
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(1)
                .getFieldsOrThrow("food.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("deluxe ice cone");
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("menu")
                .getHits(1)
                .getFieldsOrThrow("food.price")
                .getFieldValue(0)
                .getDoubleValue())
        .isEqualTo(1.4);
  }

  @Test
  public void testHighlightFields() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("branch_id").setTextValue("102")))
                    .addAllRetrieveFields(List.of("branch_id"))
                    .putInnerHits(
                        "staff",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("employees")
                            .setStartHit(0)
                            .setTopHits(10)
                            .addAllRetrieveFields(List.of("employees.name"))
                            .setHighlight(
                                Highlight.newBuilder()
                                    .setSettings(
                                        Highlight.Settings.newBuilder()
                                            .setHighlighterType(Highlight.Type.FAST_VECTOR)
                                            .setHighlightQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("employees.motto")
                                                            .setTextValue("work"))))
                                    .addFields("employees.motto"))
                            .build())
                    .build());

    assertThat(response.getHitsCount()).isEqualTo(1);

    assertThat(response.getHits(0).getFieldsOrThrow("branch_id").getFieldValue(0).getTextValue())
        .isEqualTo("102");
    assertThat(response.getHits(0).getInnerHitsMap().get("staff").getHitsCount()).isEqualTo(2);
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("staff")
                .getHits(0)
                .getFieldsOrThrow("employees.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("Jack");
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("staff")
                .getHits(0)
                .getHighlightsOrThrow("employees.motto")
                .getFragmentsList())
        .isEqualTo(List.of("I am into the <em>work</em> vibe here."));
    assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("staff")
                .getHits(1)
                .getFieldsOrThrow("employees.name")
                .getFieldValue(0)
                .getTextValue())
        .isEqualTo("Rose");
    assertThat(response.getHits(0).getInnerHitsMap().get("staff").getHits(1).getHighlightsCount())
        .isEqualTo(0);
  }
}
