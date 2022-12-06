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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.Analyzer;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.MatchPhrasePrefixQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

public class MatchPhrasePrefixQueryTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/query/registerFieldsMPPQ.json");
  }

  protected void initIndex(String name) throws Exception {
    List<String> textValues =
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
            "t1 t3 r2 p4");
    List<AddDocumentRequest> docs = new ArrayList<>();
    int index = 0;
    for (String textValue : textValues) {
      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id", MultiValuedField.newBuilder().addValue(Integer.toString(index)).build())
              .putFields(
                  "double_field",
                  MultiValuedField.newBuilder().addValue(Integer.toString(index + 1)).build())
              .putFields("text_field", MultiValuedField.newBuilder().addValue(textValue).build())
              .build();
      docs.add(request);
      index++;
    }
    addDocuments(docs.stream());
  }

  @Test
  public void testTextPhrasePrefixQuery() {
    SearchResponse response = doTextQuery("t1 t2 p", 0, 0);
    assertIds(response, 0, 1, 2);
  }

  @Test
  public void testTextSlop() {
    SearchResponse response = doTextQuery("t1 t2 p", 1, 0);
    assertIds(response, 0, 1, 2, 3, 4);
    response = doTextQuery("t1 r", 1, 0);
    assertIds(response, 3, 4, 9, 10);
  }

  @Test
  public void testTextMaxExpansions() {
    SearchResponse response = doTextQuery("t1 t2 p", 0, 1);
    assertIds(response, 0);
    response = doTextQuery("t1 t2 r", 0, 1);
    assertIds(response, 3);
    response = doTextQuery("t1 t3 r", 0, 1);
    assertIds(response, 9);
    // first sorted term is t1, so no match
    response = doTextQuery("t1 t", 0, 1);
    assertIds(response);
  }

  @Test
  public void testTextPrefixOnly() {
    SearchResponse response = doTextQuery("p", 0, 0);
    assertIds(response, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    response = doTextQuery("r", 0, 0);
    assertIds(response, 3, 4, 9, 10);
  }

  @Test
  public void testTextNotSearchable() {
    try {
      doQuery("text_field.not_searchable", "t1 t2 p", 0, 0);
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Field text_field.not_searchable is not searchable"));
    }
  }

  @Test
  public void testTextNoPositions() {
    try {
      doQuery("text_field.no_positions", "t1 t2 p", 0, 0);
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "MatchPhrasePrefixQuery field text_field.no_positions not indexed with positions"));
    }
  }

  @Test
  public void testTextNoPositionsPrefixOnly() {
    SearchResponse response = doQuery("text_field.no_positions", "p", 0, 0);
    assertIds(response, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    response = doQuery("text_field.no_positions", "r", 0, 0);
    assertIds(response, 3, 4, 9, 10);
  }

  @Test
  public void testTextNoQueryTerms() {
    SearchResponse response = doTextQuery("", 0, 0);
    assertIds(response);
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

  @Test
  public void testAtomPhrasePrefixQuery() {
    SearchResponse response = doQuery("text_field.atom_field", "t1 t2 p", 0, 0);
    assertIds(response, 0, 1, 2);
    response = doQuery("text_field.atom_field", "t1 t2", 0, 0);
    assertIds(response, 0, 1, 2, 3, 4);
    response = doQuery("text_field.atom_field", "t2", 0, 0);
    assertIds(response);
  }

  @Test
  public void testAtomMaxExpansions() {
    SearchResponse response = doQuery("text_field.atom_field", "t1 t2 p", 0, 1);
    assertIds(response, 0);
    response = doQuery("text_field.atom_field", "t1 t2 r", 0, 1);
    assertIds(response, 3);
    response = doQuery("text_field.atom_field", "t1 t3 r", 0, 1);
    assertIds(response, 9);
    response = doQuery("text_field.atom_field", "t1 t", 0, 1);
    assertIds(response, 0);
  }

  private SearchResponse doTextQuery(String text, int slop, int maxExpansions) {
    return doQuery("text_field", text, slop, maxExpansions);
  }

  private SearchResponse doAnalyzerQuery(String text) {
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
                        .setMatchPhrasePrefixQuery(
                            MatchPhrasePrefixQuery.newBuilder()
                                .setField("text_field")
                                .setQuery(text)
                                .setAnalyzer(
                                    Analyzer.newBuilder().setPredefined("core.Keyword").build())
                                .build())
                        .build())
                .build());
  }

  private SearchResponse doQuery(String field, String text, int slop, int maxExpansions) {
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
                        .setMatchPhrasePrefixQuery(
                            MatchPhrasePrefixQuery.newBuilder()
                                .setField(field)
                                .setSlop(slop)
                                .setQuery(text)
                                .setMaxExpansions(maxExpansions)
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
}
