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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.ClassRule;
import org.junit.Test;

public class StartHitTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final String TEST_INDEX = "test_index";
  private static final int NUM_DOCS = 100;

  @Override
  protected List<String> getIndices() {
    return Collections.singletonList(TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/StartHitRegisterFields.json");
  }

  @Override
  protected void initIndex(String name) throws Exception {
    for (int i = 0; i < NUM_DOCS; ++i) {
      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(i))
                      .build())
              .putFields(
                  "int_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(i + 1))
                      .build())
              .build();
      addDocuments(Stream.of(request));
    }
  }

  @Test
  public void testNoHitOffset() {
    SearchResponse response = doScoreQuery(0, 10);
    assertScoreHits(response, 0, 10);
    response = doLargeHitsQuery(0, 10);
    assertLargeHits(response, 0, 10);
    response = doSortQuery(0, 10);
    assertSortHits(response, 0, 10);
  }

  @Test
  public void testHitOffset() {
    SearchResponse response = doScoreQuery(10, 20);
    assertScoreHits(response, 10, 10);
    response = doLargeHitsQuery(10, 20);
    assertLargeHits(response, 10, 10);
    response = doSortQuery(10, 20);
    assertSortHits(response, 10, 10);
  }

  @Test
  public void testHitsPastTopHits() {
    SearchResponse response = doScoreQuery(10, 15);
    assertScoreHits(response, 10, 5);
    response = doLargeHitsQuery(10, 15);
    assertLargeHits(response, 10, 5);
    response = doSortQuery(10, 15);
    assertSortHits(response, 10, 5);
  }

  @Test
  public void testStartPastTopHits() {
    SearchResponse response = doScoreQuery(10, 10);
    assertScoreHits(response, 10, 0);
    response = doLargeHitsQuery(10, 10);
    assertLargeHits(response, 10, 0);
    response = doSortQuery(10, 10);
    assertSortHits(response, 10, 0);
  }

  private void assertScoreHits(SearchResponse response, int startHit, int numHits) {
    assertEquals(numHits, response.getHitsCount());
    for (int i = 0; i < response.getHitsCount(); ++i) {
      int expectedId = NUM_DOCS - 1 - startHit - i;
      assertHit(response.getHits(i), expectedId, expectedId + 3);
    }
  }

  private void assertLargeHits(SearchResponse response, int startHit, int numHits) {
    assertEquals(numHits, response.getHitsCount());
    for (int i = 0; i < response.getHitsCount(); ++i) {
      assertHit(response.getHits(i), startHit + i, 1.0);
    }
  }

  private void assertSortHits(SearchResponse response, int startHit, int numHits) {
    assertEquals(numHits, response.getHitsCount());
    for (int i = 0; i < response.getHitsCount(); ++i) {
      assertHit(response.getHits(i), startHit + i, 0.0);
    }
  }

  private void assertHit(SearchResponse.Hit hit, int expectedId, double expectedScore) {
    assertEquals(
        String.valueOf(expectedId), hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
    assertEquals(expectedId + 1, hit.getFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
    assertEquals(expectedScore, hit.getScore(), 0);
  }

  private SearchResponse doScoreQuery(int startHit, int topHits) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setStartHit(startHit)
                .setTopHits(topHits)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("int_field")
                .setQuery(
                    Query.newBuilder()
                        .setFunctionScoreQuery(
                            FunctionScoreQuery.newBuilder()
                                .setQuery(
                                    Query.newBuilder()
                                        .setRangeQuery(
                                            RangeQuery.newBuilder()
                                                .setUpper("10000")
                                                .setLower("0")
                                                .setField("int_field")
                                                .build())
                                        .build())
                                .setScript(
                                    Script.newBuilder()
                                        .setLang("js")
                                        .setSource("int_field + 2")
                                        .build())
                                .build())
                        .build())
                .build());
  }

  private SearchResponse doLargeHitsQuery(int startHit, int topHits) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setStartHit(startHit)
                .setTopHits(topHits)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("int_field")
                .setQuery(Query.newBuilder().build())
                .build());
  }

  private SearchResponse doSortQuery(int startHit, int topHits) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setStartHit(startHit)
                .setTopHits(topHits)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("int_field")
                .setQuery(Query.newBuilder().build())
                .setQuerySort(
                    QuerySortField.newBuilder()
                        .setFields(
                            SortFields.newBuilder()
                                .addSortedFields(
                                    SortType.newBuilder().setFieldName("int_field").build())
                                .build())
                        .build())
                .build());
  }
}
