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
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IgnoreAboveTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static final List<Field> fields =
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
              .setSearch(true)
              .setMultiValued(true)
              .setIgnoreAbove(12)
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
            .putFields("field1", MultiValuedField.newBuilder().addValue("first Vendor").build())
            .build();
    testServer.addDocs(Stream.of(addDocumentRequest));
  }

  private void addAdditionalDoc(TestServer testServer) {
    AddDocumentRequest addDocumentRequest =
        AddDocumentRequest.newBuilder()
            .setIndexName("test_index")
            .putFields("id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields(
                "field1",
                MultiValuedField.newBuilder()
                    .addValue("second Vendor")
                    .addValue("new Vendor")
                    .build())
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
                        MatchQuery.newBuilder().setField("field1").setQuery("first").build())
                    .build())
            .build();
    SearchResponse response = testServer.getClient().getBlockingStub().search(request);
    assertEquals(1, response.getHitsCount());
    request =
        SearchRequest.newBuilder()
            .setIndexName("test_index")
            .addRetrieveFields("id")
            .setStartHit(0)
            .setTopHits(10)
            .setQuery(
                Query.newBuilder()
                    .setMatchQuery(
                        MatchQuery.newBuilder().setField("field1").setQuery("second").build())
                    .build())
            .build();
    response = testServer.getClient().getBlockingStub().search(request);
    assertEquals(0, response.getHitsCount());
    request =
        SearchRequest.newBuilder()
            .setIndexName("test_index")
            .addRetrieveFields("id")
            .setStartHit(0)
            .setTopHits(10)
            .setQuery(
                Query.newBuilder()
                    .setMatchQuery(
                        MatchQuery.newBuilder().setField("field1").setQuery("new").build())
                    .build())
            .build();
    response = testServer.getClient().getBlockingStub().search(request);
    assertEquals(1, response.getHitsCount());
  }

  @Test
  public void testIgnoreAbove() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();
    primaryServer.createIndex("test_index");
    primaryServer.registerFields("test_index", fields);
    primaryServer.startPrimaryIndex("test_index", -1, null);
    addInitialDoc(primaryServer);
    addAdditionalDoc(primaryServer);
    primaryServer.refresh("test_index");
    verifyDocs(primaryServer);
  }
}
