/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver;

import com.google.gson.Gson;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.CommitRequest;
import com.yelp.nrtsearch.server.grpc.DeleteIndexRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.InnerHit;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class RegisterNewChildFieldsTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private Gson gson = new Gson();

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/update_fields/baseFields.json");
  }

  @Before
  public void recreateIndex() throws Exception {
    getGrpcServer()
        .getBlockingStub()
        .deleteIndex(DeleteIndexRequest.newBuilder().setIndexName(DEFAULT_TEST_INDEX).build());
    initIndices();
  }

  @Test
  public void testRegisterNewChildFieldSuccessful() throws Exception {
    updateFields("/field/update_fields/updateFields.json");
  }

  @Test
  public void testRegisterNewChildFieldNoRootParent() {
    Assertions.assertThatThrownBy(
            () -> updateFields("/field/update_fields/updateFieldsNoRootParent.json"))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining(
            "error while trying to RegisterFields for index: test_index\n"
                + "Root field doesnt_exist doesn't exist for field doesnt_exist.child");
  }

  @Test
  public void testRegisterNewChildFieldNoParent() {
    Assertions.assertThatThrownBy(
            () -> updateFields("/field/update_fields/updateFieldsNoParent.json"))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining(
            "error while trying to RegisterFields for index: test_index\n"
                + "Parent field doesnt_exist doesn't exist for field locations.doesnt_exist.B");
  }

  @Test
  public void testRegisterNewChildFieldDuplicateChild() {
    Assertions.assertThatThrownBy(
            () -> updateFields("/field/update_fields/updateFieldsDuplicateChild.json"))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining(
            "error while trying to RegisterFields for index: test_index\n"
                + "Duplicate field registration: locations.store under its parent field: locations");
  }

  @Test
  public void testRegisterNewChildFieldGetOldDoc() throws Exception {
    addDocumentWithOriginalFields();
    getGrpcServer()
        .getBlockingStub()
        .commit(CommitRequest.newBuilder().setIndexName(DEFAULT_TEST_INDEX).build());
    updateFields("/field/update_fields/updateFields.json");

    // new docs need some time before being searchable
    Thread.sleep(1000);
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(100)
                    .addAllRetrieveFields(
                        List.of("locations.store", "locations.warehouse", "comment.searchable"))
                    .putInnerHits(
                        "employees_info",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("employees")
                            .addAllRetrieveFields(List.of("employees.id", "employees.salary"))
                            .build())
                    .build());

    Assertions.assertThat(response.getHitsCount()).isEqualTo(1);
    Assertions.assertThat(
            response.getHits(0).getFieldsMap().get("locations.store").getFieldValueList().stream()
                .map(FieldValue::getTextValue))
        .containsExactly("store A 1", "store A 2");
    Assertions.assertThat(
            response.getHits(0).getFieldsMap().get("locations.warehouse").getFieldValueCount())
        .isEqualTo(0);
    Assertions.assertThat(
            response.getHits(0).getFieldsMap().get("comment.searchable").getFieldValueCount())
        .isEqualTo(0);

    Assertions.assertThat(
            response.getHits(0).getInnerHitsMap().get("employees_info").getHitsCount())
        .isEqualTo(2);
    Assertions.assertThat(
            response.getHits(0).getInnerHitsMap().get("employees_info").getHits(0).getFieldsMap()
                .get("employees.id").getFieldValueList().stream()
                .map(FieldValue::getIntValue))
        .containsExactly(101);
    Assertions.assertThat(
            response.getHits(0).getInnerHitsMap().get("employees_info").getHits(1).getFieldsMap()
                .get("employees.id").getFieldValueList().stream()
                .map(FieldValue::getIntValue))
        .containsExactly(102);
    Assertions.assertThat(
            response
                .getHits(0)
                .getInnerHitsMap()
                .get("employees_info")
                .getHits(0)
                .getFieldsMap()
                .get("employees.salary")
                .getFieldValueCount())
        .isEqualTo(0);
  }

  @Test
  public void testRegisterNewChildFieldGetNewDoc() throws Exception {
    getGrpcServer()
        .getBlockingStub()
        .commit(CommitRequest.newBuilder().setIndexName(DEFAULT_TEST_INDEX).build());
    updateFields("/field/update_fields/updateFields.json");
    addDocumentWithUpdatedFields();

    // new docs need some time before being searchable
    Thread.sleep(1000);
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(100)
                    .addAllRetrieveFields(
                        List.of("locations.store", "locations.warehouse", "comment.searchable"))
                    .putInnerHits(
                        "employees_info",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("employees")
                            .addAllRetrieveFields(List.of("employees.id", "employees.salary"))
                            .build())
                    .build());

    Assertions.assertThat(response.getHitsCount()).isEqualTo(1);
    Assertions.assertThat(
            response.getHits(0).getFieldsMap().get("locations.store").getFieldValueList().stream()
                .map(FieldValue::getTextValue))
        .containsExactly("store B 1", "store B 2");
    Assertions.assertThat(
            response.getHits(0).getFieldsMap().get("locations.warehouse").getFieldValueList()
                .stream()
                .map(FieldValue::getTextValue))
        .containsExactly("warehouse B 1", "warehouse B 2");
    Assertions.assertThat(
            response.getHits(0).getFieldsMap().get("comment.searchable").getFieldValueList()
                .stream()
                .map(FieldValue::getTextValue))
        .containsExactly("good chain store");

    Assertions.assertThat(
            response.getHits(0).getInnerHitsMap().get("employees_info").getHitsCount())
        .isEqualTo(2);
    Assertions.assertThat(
            response.getHits(0).getInnerHitsMap().get("employees_info").getHits(0).getFieldsMap()
                .get("employees.id").getFieldValueList().stream()
                .map(FieldValue::getIntValue))
        .containsExactly(201);
    Assertions.assertThat(
            response.getHits(0).getInnerHitsMap().get("employees_info").getHits(1).getFieldsMap()
                .get("employees.id").getFieldValueList().stream()
                .map(FieldValue::getIntValue))
        .containsExactly(202);
    Assertions.assertThat(
            response.getHits(0).getInnerHitsMap().get("employees_info").getHits(0).getFieldsMap()
                .get("employees.salary").getFieldValueList().stream()
                .map(FieldValue::getDoubleValue))
        .containsExactly(999.9);
    Assertions.assertThat(
            response.getHits(0).getInnerHitsMap().get("employees_info").getHits(1).getFieldsMap()
                .get("employees.salary").getFieldValueList().stream()
                .map(FieldValue::getDoubleValue))
        .containsExactly(888.8);
  }

  public void addDocumentWithOriginalFields() throws Exception {
    addDocuments(
        Stream.<AddDocumentRequest>builder()
            .add(
                AddDocumentRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .putAllFields(
                        Map.of(
                            "locations",
                            MultiValuedField.newBuilder()
                                .addValue(
                                    gson.toJson(Map.of("store", List.of("store A 1", "store A 2"))))
                                .build(),
                            "comment",
                            MultiValuedField.newBuilder().addValue("good brand").build(),
                            "employees",
                            MultiValuedField.newBuilder()
                                .addValue(gson.toJson(Map.of("id", 101)))
                                .addValue(gson.toJson(Map.of("id", 102)))
                                .build()))
                    .build())
            .build());
  }

  public void addDocumentWithUpdatedFields() throws Exception {
    addDocuments(
        Stream.<AddDocumentRequest>builder()
            .add(
                AddDocumentRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .putAllFields(
                        Map.of(
                            "locations",
                                MultiValuedField.newBuilder()
                                    .addValue(
                                        gson.toJson(
                                            Map.of(
                                                "store",
                                                List.of("store B 1", "store B 2"),
                                                "warehouse",
                                                List.of("warehouse B 1", "warehouse B 2"))))
                                    .build(),
                            "comment",
                                MultiValuedField.newBuilder().addValue("good chain store").build(),
                            "employees",
                                MultiValuedField.newBuilder()
                                    .addValue(gson.toJson(Map.of("id", 201, "salary", 999.9)))
                                    .addValue(gson.toJson(Map.of("id", 202, "salary", 888.8)))
                                    .build()))
                    .build())
            .build());
  }

  private void updateFields(String schemaPath) throws IOException {
    getGrpcServer().getBlockingStub().registerFields(getFieldsFromResourceFile(schemaPath));
  }
}
