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
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.SearchResponse.SearchState;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Test;

public class SearchStateTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/registerFieldsBasic.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields(
                "vendor_name", MultiValuedField.newBuilder().addValue("first vendor").build())
            .putFields("long_field", MultiValuedField.newBuilder().addValue("5").build())
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields(
                "vendor_name",
                MultiValuedField.newBuilder().addValue("second vendor review").build())
            .putFields("long_field", MultiValuedField.newBuilder().addValue("10").build())
            .build();
    docs.add(request);
    addDocuments(docs.stream());
  }

  @Test
  public void testSearchStateNoHits() {
    long testStartTime = System.currentTimeMillis() / 1000;
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("vendor_name")
                                    .setTextValue("unknown_term")
                                    .build())
                            .build())
                    .build());
    SearchState searchState = response.getSearchState();
    assertTrue(searchState.getTimestamp() - testStartTime < 500);
    assertTrue(searchState.getSearcherVersion() > 0);
    assertEquals(0, searchState.getLastDocId());
    assertEquals(0, searchState.getLastScore(), 0);
    assertEquals(0, searchState.getLastFieldValuesCount());
  }

  @Test
  public void testSearchStateRelevance() {
    long testStartTime = System.currentTimeMillis() / 1000;
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("vendor_name")
                                    .setTextValue("vendor")
                                    .build())
                            .build())
                    .build());
    SearchState searchState = response.getSearchState();
    assertTrue(searchState.getTimestamp() - testStartTime < 500);
    assertTrue(searchState.getSearcherVersion() > 0);
    assertEquals(1, searchState.getLastDocId());
    assertEquals(0.0766057, searchState.getLastScore(), 0.0000001);
    assertEquals(0, searchState.getLastFieldValuesCount());
  }

  @Test
  public void testSearchStateSort() {
    long testStartTime = System.currentTimeMillis() / 1000;
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("vendor_name")
                                    .setTextValue("vendor")
                                    .build())
                            .build())
                    .setQuerySort(
                        QuerySortField.newBuilder()
                            .setFields(
                                SortFields.newBuilder()
                                    .addSortedFields(
                                        SortType.newBuilder().setFieldName("long_field").build())
                                    .build())
                            .build())
                    .build());
    SearchState searchState = response.getSearchState();
    assertTrue(searchState.getTimestamp() - testStartTime < 500);
    assertTrue(searchState.getSearcherVersion() > 0);
    assertEquals(1, searchState.getLastDocId());
    assertEquals(0, searchState.getLastScore(), 0);
    assertEquals(1, searchState.getLastFieldValuesCount());
    assertEquals("10", searchState.getLastFieldValues(0));
  }
}
