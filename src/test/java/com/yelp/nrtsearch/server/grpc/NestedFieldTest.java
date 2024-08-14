/*
 * Copyright 2024 Yelp Inc.
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

import static org.junit.Assert.*;

import com.yelp.nrtsearch.server.config.IndexStartConfig;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NestedFieldTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  @Test
  public void testAddNestedFieldIsCompatible() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.PRIMARY, 0, IndexStartConfig.IndexDataLocationType.LOCAL)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);

    primaryServer.addSimpleDocs("test_index", 1, 2, 3);
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    primaryServer.verifySimpleDocs("test_index", 3);
    IndexState indexState = primaryServer.getGlobalState().getIndex("test_index");
    assertFalse(indexState.hasNestedChildFields());

    String nestedFieldJson =
        """
                {
                      "name": "nested_field",
                      "type": "OBJECT",
                      "search": true,
                      "nestedDoc": true,
                      "multiValued": true,
                      "childFields": [
                        {
                          "name": "name",
                          "type": "ATOM",
                          "search": true,
                          "storeDocValues": true
                        }
                      ]
                    }""";

    // add nested field
    primaryServer
        .getClient()
        .registerFields("{\"indexName\": \"test_index\", \"field\": [" + nestedFieldJson + "]}");
    indexState = primaryServer.getGlobalState().getIndex("test_index");
    assertTrue(indexState.hasNestedChildFields());

    // add more documents
    primaryServer.addSimpleDocs("test_index", 4, 5, 6);
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    primaryServer.verifySimpleDocs("test_index", 6);

    // add document with nested field
    AddDocumentRequest request = primaryServer.getSimpleDocRequest("test_index", 7);
    request =
        request.toBuilder()
            .putFields(
                "nested_field",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue("{\"name\": \"nested_1\"}")
                    .build())
            .build();
    primaryServer.addDocs(Stream.of(request));
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    primaryServer.verifySimpleDocs("test_index", 7);

    // query nested path
    SearchResponse searchResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName("test_index")
                    .setTopHits(10)
                    .addRetrieveFields("nested_field.name")
                    .setQuery(Query.newBuilder().build())
                    .setQueryNestedPath("nested_field")
                    .build());
    assertEquals(1, searchResponse.getHitsCount());
    assertEquals(
        "nested_1",
        searchResponse
            .getHits(0)
            .getFieldsOrThrow("nested_field.name")
            .getFieldValue(0)
            .getTextValue());
  }
}
