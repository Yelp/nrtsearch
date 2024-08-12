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
package com.yelp.nrtsearch.server.luceneserver.field;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Test;

public class IdFieldTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsIdStored.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    List<AddDocumentRequest> requestList = new ArrayList<>();
    requestList.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("1").build())
            .build());
    requestList.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("2").build())
            .build());
    requestList.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("3").build())
            .build());
    addDocuments(requestList.stream());
  }

  @Test
  public void testStoredFields() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(3)
                    .addRetrieveFields("id")
                    .setQuery(Query.newBuilder().build())
                    .build());
    assertEquals(3, response.getHitsCount());
    SearchResponse.Hit hit = response.getHits(0);
    assertEquals(1, hit.getFieldsOrThrow("id").getFieldValueCount());
    assertEquals("1", hit.getFieldsOrThrow("id").getFieldValue(0).getTextValue());

    hit = response.getHits(1);
    assertEquals(1, hit.getFieldsOrThrow("id").getFieldValueCount());
    assertEquals("2", hit.getFieldsOrThrow("id").getFieldValue(0).getTextValue());

    hit = response.getHits(2);
    assertEquals(1, hit.getFieldsOrThrow("id").getFieldValueCount());
    assertEquals("3", hit.getFieldsOrThrow("id").getFieldValue(0).getTextValue());
  }
}
