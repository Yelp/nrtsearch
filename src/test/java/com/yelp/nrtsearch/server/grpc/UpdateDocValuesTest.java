/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class UpdateDocValuesTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  TestServer primaryServer;

  String testPartialUpdateIndex = "test_partial_update_index";

  private final List<Field> fields =
      List.of(
          Field.newBuilder()
              .setName("primary_key")
              .setType(FieldType._ID)
              .setStoreDocValues(true)
              .setSearch(true)
              .build(),
          Field.newBuilder()
              .setName("update_field_one")
              .setStoreDocValues(true)
              .setMultiValued(false)
              .setType(FieldType.INT)
              .build(),
          Field.newBuilder()
              .setName("update_field_two")
              .setStoreDocValues(true)
              .setMultiValued(false)
              .setType(FieldType.INT)
              .build(),
          Field.newBuilder()
              .setName("non_updatable_field")
              .setStoreDocValues(true)
              .setMultiValued(false)
              .setType(FieldType.INT)
              .setSearch(true)
              .build());

  @Test
  public void testupdateDocValuesOnly() throws Exception {

    primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();

    primaryServer.createIndex(testPartialUpdateIndex);
    primaryServer.registerFields(testPartialUpdateIndex, fields);
    primaryServer.startPrimaryIndex(testPartialUpdateIndex, -1, null);

    primaryServer.addDocs(buildAddDocRequest(3).stream());
    primaryServer.commit(testPartialUpdateIndex);
    primaryServer.refresh(testPartialUpdateIndex);
    verifyAddDocsPresent(3, false, -1);

    primaryServer.addDocs(buildUpdateRequest(2).stream());
    primaryServer.commit(testPartialUpdateIndex);
    primaryServer.refresh(testPartialUpdateIndex);

    verifyAddDocsPresent(3, true, 2);
    // send more updates
    sendMoreUpdatesAndVerify();
    // send full doc insert for same docs and verify
    primaryServer.addDocs(buildAddDocRequest(3).stream());
    primaryServer.commit(testPartialUpdateIndex);
    primaryServer.refresh(testPartialUpdateIndex);
    verifyAddDocsPresent(3, false, -1);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testUpdateFailsWhenUpdateFieldIsSearchable() throws Throwable {
    primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();

    primaryServer.createIndex(testPartialUpdateIndex);
    primaryServer.registerFields(testPartialUpdateIndex, fields);
    primaryServer.startPrimaryIndex(testPartialUpdateIndex, -1, null);

    primaryServer.addDocs(buildAddDocRequest(3).stream());
    primaryServer.commit(testPartialUpdateIndex);

    try {
      primaryServer.addDocs(buildInvalidUpdateRequest(2).stream());
    } catch (Throwable t) {
      throw t.getCause().getCause();
    }
  }

  private List<AddDocumentRequest> buildInvalidUpdateRequest(int i) {
    List<AddDocumentRequest> requests = new ArrayList<>();
    AddDocumentRequest.Builder request =
        AddDocumentRequest.newBuilder().setIndexName(testPartialUpdateIndex);
    request.setRequestType(IndexingRequestType.UPDATE_DOC_VALUES).build();
    request.putFields(
        "primary_key", MultiValuedField.newBuilder().addValue(String.valueOf(i)).build());
    request.putFields(
        "non_updatable_field", MultiValuedField.newBuilder().addValue(String.valueOf(100)).build());
    requests.add(request.build());
    return requests;
  }

  private void sendMoreUpdatesAndVerify() throws InterruptedException {
    AddDocumentRequest.Builder request =
        AddDocumentRequest.newBuilder().setIndexName(testPartialUpdateIndex);

    request.putFields(
        "primary_key", MultiValuedField.newBuilder().addValue(String.valueOf(1)).build());
    request.putFields(
        "update_field_two", MultiValuedField.newBuilder().addValue(String.valueOf(101)).build());
    request.setRequestType(IndexingRequestType.UPDATE_DOC_VALUES).build();

    AddDocumentRequest.Builder request2 =
        AddDocumentRequest.newBuilder().setIndexName(testPartialUpdateIndex);

    request2.putFields(
        "primary_key", MultiValuedField.newBuilder().addValue(String.valueOf(3)).build());
    request2.putFields(
        "update_field_one", MultiValuedField.newBuilder().addValue(String.valueOf(1001)).build());
    request2.setRequestType(IndexingRequestType.UPDATE_DOC_VALUES).build();

    primaryServer.addDocs(List.of(request.build(), request2.build()).stream());
    primaryServer.commit(testPartialUpdateIndex);
    primaryServer.refresh(testPartialUpdateIndex);

    SearchResponse response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(testPartialUpdateIndex)
                    .addAllRetrieveFields(fields.stream().map(Field::getName).toList())
                    .setTopHits(4)
                    .setStartHit(0)
                    .build());

    for (Hit hit : response.getHitsList()) {
      int id =
          Integer.parseInt(hit.getFieldsOrThrow("primary_key").getFieldValue(0).getTextValue());
      int f1 = hit.getFieldsOrThrow("update_field_one").getFieldValue(0).getIntValue();
      int f2 = hit.getFieldsOrThrow("update_field_two").getFieldValue(0).getIntValue();
      int f4 = hit.getFieldsOrThrow("non_updatable_field").getFieldValue(0).getIntValue();

      if (id == 1) {
        assertEquals(101, f2);
        assertEquals(f1, 1);
        assertEquals(f4, 1);
      }
      if (id == 2) {
        assertEquals(100, f1);
        assertEquals(f2, 2);
        assertEquals(f4, 2);
      }
      if (id == 3) {
        assertEquals(f1, 1001);
        assertEquals(f2, 3);
        assertEquals(f4, 3);
      }
    }
  }

  private List<AddDocumentRequest> buildAddDocRequest(int numberOfDocs) {
    List<AddDocumentRequest> requests = new ArrayList<>();
    for (int i = 0; i < numberOfDocs; i++) {
      AddDocumentRequest.Builder request =
          AddDocumentRequest.newBuilder().setIndexName(testPartialUpdateIndex);
      for (Field field : fields) {
        request.putFields(
            field.getName(), MultiValuedField.newBuilder().addValue(String.valueOf(i)).build());
      }
      requests.add(request.build());
    }
    return requests;
  }

  private List<AddDocumentRequest> buildUpdateRequest(int updatedDoc) {
    List<AddDocumentRequest> requests = new ArrayList<>();
    AddDocumentRequest.Builder request =
        AddDocumentRequest.newBuilder().setIndexName(testPartialUpdateIndex);
    request.setRequestType(IndexingRequestType.UPDATE_DOC_VALUES).build();
    request.putFields(
        "primary_key", MultiValuedField.newBuilder().addValue(String.valueOf(updatedDoc)).build());
    request.putFields(
        "update_field_one", MultiValuedField.newBuilder().addValue(String.valueOf(100)).build());
    requests.add(request.build());
    return requests;
  }

  private void verifyAddDocsPresent(int numberOfDocs, boolean isUpdate, int updateId) {
    SearchResponse response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(testPartialUpdateIndex)
                    .addAllRetrieveFields(fields.stream().map(Field::getName).toList())
                    .setTopHits(numberOfDocs + 1)
                    .setStartHit(0)
                    .build());

    assertEquals(numberOfDocs, response.getHitsCount());
    for (Hit hit : response.getHitsList()) {
      int id =
          Integer.parseInt(hit.getFieldsOrThrow("primary_key").getFieldValue(0).getTextValue());
      int f1 = hit.getFieldsOrThrow("update_field_one").getFieldValue(0).getIntValue();
      int f2 = hit.getFieldsOrThrow("update_field_two").getFieldValue(0).getIntValue();
      int f3 = hit.getFieldsOrThrow("non_updatable_field").getFieldValue(0).getIntValue();

      if (!isUpdate || id != updateId) {
        assertEquals(id, f1);
        assertEquals(id, f2);
        assertEquals(id, f3);
      } else {
        assertEquals(100, f1);
        assertEquals(id, f2);
        assertEquals(id, f3);
      }
    }
  }
}
