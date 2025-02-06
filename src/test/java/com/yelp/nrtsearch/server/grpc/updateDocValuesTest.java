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

import com.yelp.nrtsearch.server.config.IndexStartConfig;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class updateDocValuesTest {

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
              .setName("ad_bid_id")
              .setType(FieldType._ID)
              .setStoreDocValues(true)
              .setSearch(true)
              .build(),
          Field.newBuilder()
              .setName("ad_bid_floor")
              .setStoreDocValues(true)
              .setType(FieldType.INT)
              .build(),
          Field.newBuilder()
              .setName("ad_bid_floor_USD")
              .setStoreDocValues(true)
              .setType(FieldType.INT)
              .build(),
          Field.newBuilder()
              .setName("non_rtb_field")
              .setStoreDocValues(true)
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
    Thread.sleep(2000);
    verifyAddDocsPrenset(3, false, 3);

    primaryServer.addDocs(buildUpdateRequest(2).stream());
    primaryServer.commit(testPartialUpdateIndex);
    Thread.sleep(2000);

    verifyAddDocsPrenset(3, true, 2);

    // send more updates
    AddDocumentRequest.Builder request =
        AddDocumentRequest.newBuilder().setIndexName(testPartialUpdateIndex);

    request.putFields(
        "ad_bid_id", MultiValuedField.newBuilder().addValue(String.valueOf(1)).build());
    request.putFields(
        "ad_bid_floor_USD", MultiValuedField.newBuilder().addValue(String.valueOf(101)).build());
    request.putFields(
        "ALLOW_PARTIAL_UPDATE", MultiValuedField.newBuilder().addValue(String.valueOf(1)).build());

    primaryServer.addDocs(List.of(request.build()).stream());
    primaryServer.commit(testPartialUpdateIndex);
    Thread.sleep(1000);

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
      int id = Integer.parseInt(hit.getFieldsOrThrow("ad_bid_id").getFieldValue(0).getTextValue());
      int f1 = hit.getFieldsOrThrow("ad_bid_floor").getFieldValue(0).getIntValue();
      int f2 = hit.getFieldsOrThrow("ad_bid_floor_USD").getFieldValue(0).getIntValue();
      int f4 = hit.getFieldsOrThrow("non_rtb_field").getFieldValue(0).getIntValue();

      if(id == 1){
        assertEquals(101, f2);
        assertEquals(f1,1);
        assertEquals(f4,1);
      }
      if(id == 2){
        assertEquals(100, f1);
        assertEquals(f2,2);
        assertEquals(f4,2);
      }
      if(id == 3){
        assertEquals(f1,3);
        assertEquals(f2,3);
        assertEquals(f4,3);
      }
    }
  }

  public void simpleTest() throws Exception {
    primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.PRIMARY, 0, IndexStartConfig.IndexDataLocationType.LOCAL)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);
    primaryServer.addSimpleDocs("test_index", 1, 2, 3);
    primaryServer.commit("test_index");
    Thread.sleep(2000);
    primaryServer.verifySimpleDocs("test_index", 3);


  }

  private List<AddDocumentRequest> buildAddDocRequest(int N) {
    List<AddDocumentRequest> requests = new ArrayList<>();
    for (int i = 0; i < N; i++) {
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

  private List<AddDocumentRequest> buildUpdateRequest(int N) {
    List<AddDocumentRequest> requests = new ArrayList<>();
    AddDocumentRequest.Builder request =
        AddDocumentRequest.newBuilder().setIndexName(testPartialUpdateIndex);

    request.putFields(
        "ad_bid_id", MultiValuedField.newBuilder().addValue(String.valueOf(N)).build());
    request.putFields(
        "ad_bid_floor", MultiValuedField.newBuilder().addValue(String.valueOf(100)).build());
    request.putFields(
        "ALLOW_PARTIAL_UPDATE", MultiValuedField.newBuilder().addValue(String.valueOf(1)).build());
    requests.add(request.build());
    return requests;
  }

  private void verifyAddDocsPrenset(int N, boolean extraCheck, int idIn) {
    SearchResponse response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(testPartialUpdateIndex)
                    .addAllRetrieveFields(fields.stream().map(Field::getName).toList())
                    .setTopHits(N + 1)
                    .setStartHit(0)
                    .build());

    assertEquals(N, response.getHitsCount());
    for (Hit hit : response.getHitsList()) {
      int id = Integer.parseInt(hit.getFieldsOrThrow("ad_bid_id").getFieldValue(0).getTextValue());
      int f1 = hit.getFieldsOrThrow("ad_bid_floor").getFieldValue(0).getIntValue();
      int f2 = hit.getFieldsOrThrow("ad_bid_floor_USD").getFieldValue(0).getIntValue();
      int f4 = hit.getFieldsOrThrow("non_rtb_field").getFieldValue(0).getIntValue();

      if (id != idIn) {
        assertEquals(id, f1);
        assertEquals(id, f2);
        assertEquals(id, f4);
      } else {
        assertEquals(100, f1);
        assertEquals(id, f2);
        assertEquals(id, f4);
      }
    }
  }
}
