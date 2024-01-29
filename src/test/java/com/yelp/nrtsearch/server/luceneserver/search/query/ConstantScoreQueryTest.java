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
package com.yelp.nrtsearch.server.luceneserver.search.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.ConstantScoreQuery;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

public class ConstantScoreQueryTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/query/registerFieldsConstScore.json");
  }

  protected void initIndex(String name) throws Exception {
    List<String> textValues = List.of("t1 t2 t3", "t1 t3", "t4 t5 t6", "t2 t6 t7", "t1 t2 t8");
    List<AddDocumentRequest> docs = new ArrayList<>();
    int index = 0;
    for (String textValue : textValues) {
      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id", MultiValuedField.newBuilder().addValue(Integer.toString(index)).build())
              .putFields("text_field", MultiValuedField.newBuilder().addValue(textValue).build())
              .build();
      docs.add(request);
      index++;
    }
    addDocuments(docs.stream());
  }

  @Test
  public void testWithoutConstantScoreQuery() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .setQuery(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("t2")
                                    .build())
                            .build())
                    .build());
    assertIds(response, 0, 3, 4);
    for (Hit hit : response.getHitsList()) {
      assertNotEquals(1.0, hit.getScore());
    }
  }

  @Test
  public void testConstantScoreQuery() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .setQuery(
                        Query.newBuilder()
                            .setConstantScoreQuery(
                                ConstantScoreQuery.newBuilder()
                                    .setFilter(
                                        Query.newBuilder()
                                            .setMatchQuery(
                                                MatchQuery.newBuilder()
                                                    .setField("text_field")
                                                    .setQuery("t2")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build());
    assertIds(response, 0, 3, 4);
    for (Hit hit : response.getHitsList()) {
      assertEquals(1.0, hit.getScore(), 0);
    }
  }

  @Test
  public void testConstantScoreQueryWithBoost() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .setQuery(
                        Query.newBuilder()
                            .setConstantScoreQuery(
                                ConstantScoreQuery.newBuilder()
                                    .setFilter(
                                        Query.newBuilder()
                                            .setMatchQuery(
                                                MatchQuery.newBuilder()
                                                    .setField("text_field")
                                                    .setQuery("t2")
                                                    .build())
                                            .build())
                                    .build())
                            .setBoost(5.0f)
                            .build())
                    .build());
    assertIds(response, 0, 3, 4);
    for (Hit hit : response.getHitsList()) {
      assertEquals(5.0, hit.getScore(), 0);
    }
  }

  private void assertIds(SearchResponse response, int... ids) {
    Set<Integer> uniqueIds = new HashSet<>();
    for (int id : ids) {
      uniqueIds.add(id);
    }
    assertEquals(uniqueIds.size(), response.getHitsCount());

    Set<Integer> responseIds = new HashSet<>();
    for (Hit hit : response.getHitsList()) {
      responseIds.add(
          Integer.parseInt(hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue()));
    }
    assertEquals(uniqueIds, responseIds);
  }
}
