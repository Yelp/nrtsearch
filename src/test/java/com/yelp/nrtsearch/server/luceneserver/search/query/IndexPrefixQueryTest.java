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

import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
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

public class IndexPrefixQueryTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/query/registerFieldsIndexPrefix.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    docs.add(createDocument("0", "apple computer", "An apple computer device"));
    docs.add(createDocument("1", "apple fruit", "A fresh apple fruit"));
    docs.add(createDocument("2", "application software", "Various applications"));
    docs.add(createDocument("3", "banana", "Yellow banana"));
    docs.add(createDocument("4", "computer desk", "A desk for computer"));

    addDocuments(docs.stream());
  }

  private AddDocumentRequest createDocument(String doc_id, String text_field, String description) {
    return AddDocumentRequest.newBuilder()
        .setIndexName(DEFAULT_TEST_INDEX)
        .putFields("doc_id", MultiValuedField.newBuilder().addValue(doc_id).build())
        .putFields("text_field", MultiValuedField.newBuilder().addValue(text_field).build())
        .putFields("description", MultiValuedField.newBuilder().addValue(description).build())
        .build();
  }

  @Test
  public void testBasicPrefixSearch() {
    SearchResponse response = doQuery("text_field", "app");
    assertIds(response, 0, 1, 2);

    response = doQuery("text_field", "appl");
    assertIds(response, 0, 1, 2);

    response = doQuery("text_field", "comp");
    assertIds(response, 0, 4);
  }

  @Test
  public void testShortPrefix() {
    SearchResponse response = doQuery("text_field", "a");
    assertIds(response, 0, 1, 2);
  }

  @Test
  public void testLongPrefix() {
    SearchResponse response = doQuery("text_field", "comput");
    assertIds(response, 0, 4);
  }

  @Test
  public void testNonPrefixField() {
    SearchResponse response = doQuery("description", "device");
    assertIds(response, 0);
  }

  @Test
  public void testExactMatch() {
    SearchResponse response = doQuery("text_field", "banana");
    assertIds(response, 3);
  }

  @Test
  public void testNoMatches() {
    SearchResponse response = doQuery("text_field", "xyz");
    assertIds(response);
  }

  private SearchResponse doQuery(String field, String prefix) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .setQuery(
                    Query.newBuilder()
                        .setPrefixQuery(
                            PrefixQuery.newBuilder().setField(field).setPrefix(prefix).build())
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
