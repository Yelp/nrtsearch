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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AddFieldsIndexingTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static final List<Field> initialFields =
      List.of(
          Field.newBuilder()
              .setName("id")
              .setType(FieldType._ID)
              .setStoreDocValues(true)
              .setSearch(true)
              .build(),
          Field.newBuilder()
              .setName("field1")
              .setStoreDocValues(true)
              .setType(FieldType.INT)
              .build(),
          Field.newBuilder()
              .setName("field2")
              .setStoreDocValues(true)
              .setSearch(true)
              .setTokenize(true)
              .setType(FieldType.TEXT)
              .build());

  private static final List<Field> additionalFields =
      List.of(
          Field.newBuilder()
              .setName("field3")
              .setStoreDocValues(true)
              .setType(FieldType.INT)
              .build(),
          Field.newBuilder()
              .setName("field4")
              .setStoreDocValues(true)
              .setSearch(true)
              .setTokenize(true)
              .setType(FieldType.TEXT)
              .build());

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  private void addInitialDoc(TestServer testServer) {
    AddDocumentRequest addDocumentRequest =
        AddDocumentRequest.newBuilder()
            .setIndexName("test_index")
            .putFields("id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields("field1", MultiValuedField.newBuilder().addValue("10").build())
            .putFields("field2", MultiValuedField.newBuilder().addValue("first Vendor").build())
            .build();
    testServer.addDocs(Stream.of(addDocumentRequest));
  }

  private void addAdditionalDoc(TestServer testServer) {
    AddDocumentRequest addDocumentRequest =
        AddDocumentRequest.newBuilder()
            .setIndexName("test_index")
            .putFields("id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields("field1", MultiValuedField.newBuilder().addValue("20").build())
            .putFields("field2", MultiValuedField.newBuilder().addValue("second Vendor").build())
            .putFields("field3", MultiValuedField.newBuilder().addValue("22").build())
            .putFields("field4", MultiValuedField.newBuilder().addValue("Test Caption").build())
            .build();
    testServer.addDocs(Stream.of(addDocumentRequest));
  }

  private void verifyDocs(TestServer testServer) {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName("test_index")
            .addRetrieveFields("id")
            .setStartHit(0)
            .setTopHits(10)
            .setQuery(
                Query.newBuilder()
                    .setMatchQuery(
                        MatchQuery.newBuilder().setField("field2").setQuery("vendor").build())
                    .build())
            .build();
    SearchResponse response = testServer.getClient().getBlockingStub().search(request);
    assertEquals(2, response.getHitsCount());

    Set<String> responseIds =
        Set.of(
            response.getHits(0).getFieldsOrThrow("id").getFieldValue(0).getTextValue(),
            response.getHits(1).getFieldsOrThrow("id").getFieldValue(0).getTextValue());
    assertEquals(Set.of("1", "2"), responseIds);

    request =
        SearchRequest.newBuilder()
            .setIndexName("test_index")
            .addRetrieveFields("id")
            .setStartHit(0)
            .setTopHits(10)
            .setQuery(
                Query.newBuilder()
                    .setMatchQuery(
                        MatchQuery.newBuilder().setField("field4").setQuery("caption").build())
                    .build())
            .build();
    response = testServer.getClient().getBlockingStub().search(request);
    assertEquals(1, response.getHitsCount());

    responseIds =
        Set.of(response.getHits(0).getFieldsOrThrow("id").getFieldValue(0).getTextValue());
    assertEquals(Set.of("2"), responseIds);
  }

  @Test
  public void testAddFieldsPreIndexStart() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();
    primaryServer.createIndex("test_index");
    primaryServer.registerFields("test_index", initialFields);
    primaryServer.registerFields("test_index", additionalFields);
    primaryServer.startPrimaryIndex("test_index", -1, null);
    addInitialDoc(primaryServer);
    addAdditionalDoc(primaryServer);
    primaryServer.refresh("test_index");
    verifyDocs(primaryServer);
  }

  @Test
  public void testAddFieldsPostIndexStart() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();
    primaryServer.createIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);
    primaryServer.registerFields("test_index", initialFields);
    primaryServer.registerFields("test_index", additionalFields);
    addInitialDoc(primaryServer);
    addAdditionalDoc(primaryServer);
    primaryServer.refresh("test_index");
    verifyDocs(primaryServer);
  }

  @Test
  public void testAddFieldsSplitIndexStart() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();
    primaryServer.createIndex("test_index");
    primaryServer.registerFields("test_index", initialFields);
    primaryServer.startPrimaryIndex("test_index", -1, null);
    addInitialDoc(primaryServer);
    primaryServer.registerFields("test_index", additionalFields);
    addAdditionalDoc(primaryServer);
    primaryServer.refresh("test_index");
    verifyDocs(primaryServer);
  }
}
