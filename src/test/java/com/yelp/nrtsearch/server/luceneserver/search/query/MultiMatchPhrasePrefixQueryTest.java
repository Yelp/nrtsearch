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
package com.yelp.nrtsearch.server.luceneserver.search.query;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.Analyzer;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.MultiMatchQuery;
import com.yelp.nrtsearch.server.grpc.MultiMatchQuery.MatchType;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

public class MultiMatchPhrasePrefixQueryTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/query/registerFieldsMMPPQ.json");
  }

  protected void initIndex(String name) throws Exception {
    List<String> textValues1 =
        new ArrayList<>(
            List.of(
                "t1 t2 p1",
                "t1 t2 p2",
                "t1 t2 p3",
                "t1 t2 r1 p1",
                "t1 t2 r2 p2",
                "t1 t3 p1",
                "t1 t3 p2",
                "t1 t3 p3",
                "t1 t3 p3",
                "t1 t3 r1 p3",
                "t1 t3 r2 p4"));
    List<String> textValues2 =
        new ArrayList<>(
            List.of(
                "t3 t1 p1",
                "t3 t1 p2",
                "t3 t1 p3",
                "t3 t1 r1 p1",
                "t3 t1 r2 p2",
                "t1 t2 p1",
                "t1 t2 p2",
                "t1 t2 p3",
                "t1 t2 p3",
                "t1 t2 r1 p3",
                "t1 t2 r2 p4"));
    List<AddDocumentRequest> docs = new ArrayList<>();
    for (int i = 0; i < textValues1.size(); ++i) {
      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id", MultiValuedField.newBuilder().addValue(Integer.toString(i)).build())
              .putFields(
                  "text_field1", MultiValuedField.newBuilder().addValue(textValues1.get(i)).build())
              .putFields(
                  "text_field2", MultiValuedField.newBuilder().addValue(textValues2.get(i)).build())
              .build();
      docs.add(request);
    }
    addDocuments(docs.stream());
  }

  @Test
  public void testMultiMatch() {
    SearchResponse response = doQuery("t1 t2 p", 0, 0, "text_field1");
    assertIds(response, 0, 1, 2);
    response = doQuery("t1 t2 p", 0, 0, "text_field2");
    assertIds(response, 5, 6, 7, 8);
    response = doQuery("t1 t2 p", 0, 0, "text_field1", "text_field2");
    assertIds(response, 0, 1, 2, 5, 6, 7, 8);
  }

  @Test
  public void testSlop() {
    SearchResponse response = doQuery("t1 t3 p", 1, 0, "text_field1", "text_field2");
    assertIds(response, 5, 6, 7, 8, 9, 10);
    response = doQuery("t1 t3 p", 2, 0, "text_field1", "text_field2");
    assertIds(response, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
  }

  @Test
  public void testMaxExpansions() {
    SearchResponse response = doQuery("t1 t2 p", 0, 1, "text_field1", "text_field2");
    assertIds(response, 0, 5);
  }

  @Test
  public void testFieldBoost() {
    SearchResponse response = doQuery("t1 t2 p", 0, 0, "text_field1", "text_field2");
    assertOrderedHits(response, 0, 1, 2, 5, 6, 7, 8);
    response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(20)
                    .addRetrieveFields("doc_id")
                    .setQuery(
                        Query.newBuilder()
                            .setMultiMatchQuery(
                                MultiMatchQuery.newBuilder()
                                    .setType(MatchType.PHRASE_PREFIX)
                                    .setQuery("t1 t2 p")
                                    .addFields("text_field1")
                                    .addFields("text_field2")
                                    .putFieldBoosts("text_field2", 2.0f)
                                    .build())
                            .build())
                    .build());
    assertOrderedHits(response, 5, 6, 7, 8, 0, 1, 2);
  }

  @Test
  public void testSetAnalyzer() {
    SearchResponse response = doAnalyzerQuery("t1 t2 p");
    assertIds(response);
    response = doAnalyzerQuery("t2 p");
    assertIds(response);
    response = doAnalyzerQuery("p");
    assertIds(response, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
  }

  private SearchResponse doAnalyzerQuery(String query) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(20)
                .addRetrieveFields("doc_id")
                .setQuery(
                    Query.newBuilder()
                        .setMultiMatchQuery(
                            MultiMatchQuery.newBuilder()
                                .setType(MatchType.PHRASE_PREFIX)
                                .setQuery(query)
                                .addFields("text_field1")
                                .addFields("text_field2")
                                .setAnalyzer(
                                    Analyzer.newBuilder().setPredefined("core.Keyword").build())
                                .build())
                        .build())
                .build());
  }

  @Test
  public void testMultiMatchAtom() {
    SearchResponse response = doQuery("t1 t3 p", 0, 0, "text_field1.atom", "text_field2.atom");
    assertIds(response, 5, 6, 7, 8);
    response = doQuery("t3 t1", 0, 0, "text_field1.atom", "text_field2.atom");
    assertIds(response, 0, 1, 2, 3, 4);
    response = doQuery("t1 t2 p", 0, 0, "text_field1.atom", "text_field2.atom");
    assertIds(response, 0, 1, 2, 5, 6, 7, 8);
    response = doQuery("t1 t2", 0, 0, "text_field1.atom", "text_field2.atom");
    assertIds(response, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    response = doQuery("p", 0, 0, "text_field1.atom", "text_field2.atom");
    assertIds(response);
  }

  @Test
  public void testMultiMatchMixed() {
    SearchResponse response = doQuery("t1 t3 p", 0, 0, "text_field1", "text_field2.atom");
    assertIds(response, 5, 6, 7, 8);
    response = doQuery("t3 t1 p", 0, 0, "text_field1", "text_field2.atom");
    assertIds(response, 0, 1, 2);
    response = doQuery("t2 p", 0, 0, "text_field1", "text_field2.atom");
    assertIds(response, 0, 1, 2);
  }

  private SearchResponse doQuery(String query, int slop, int maxExpansions, String... fields) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(20)
                .addRetrieveFields("doc_id")
                .setQuery(
                    Query.newBuilder()
                        .setMultiMatchQuery(
                            MultiMatchQuery.newBuilder()
                                .setType(MatchType.PHRASE_PREFIX)
                                .setQuery(query)
                                .setSlop(slop)
                                .setMaxExpansions(maxExpansions)
                                .addAllFields(Arrays.asList(fields))
                                .build())
                        .build())
                .build());
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

  private void assertOrderedHits(SearchResponse searchResponse, int... ids) {
    assertEquals(ids.length, searchResponse.getHitsCount());
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      assertEquals(
          ids[i],
          Integer.parseInt(
              searchResponse
                  .getHits(i)
                  .getFieldsOrThrow("doc_id")
                  .getFieldValue(0)
                  .getTextValue()));
    }
  }
}
