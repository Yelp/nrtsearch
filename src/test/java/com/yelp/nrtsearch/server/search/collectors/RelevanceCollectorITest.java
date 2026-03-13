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
package com.yelp.nrtsearch.server.search.collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.LastHitInfo;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

/** Integration tests for RelevanceCollector, including searchAfter (cursor) pagination. */
public class RelevanceCollectorITest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final int NUM_DOCS = 5;
  private static final int PAGE_SIZE = 2;

  @Override
  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  @Override
  protected com.yelp.nrtsearch.server.grpc.FieldDefRequest getIndexDef(String name)
      throws IOException {
    return getFieldsFromResourceFile("/registerFieldsBasic.json");
  }

  @Override
  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    for (int i = 1; i <= NUM_DOCS; i++) {
      docs.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(i))
                      .build())
              .putFields(
                  "vendor_name",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue("vendor doc " + i)
                      .build())
              .putFields(
                  "long_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(i * 10))
                      .build())
              .build());
    }
    addDocuments(docs.stream());
  }

  private static Query vendorQuery() {
    return Query.newBuilder()
        .setTermQuery(TermQuery.newBuilder().setField("vendor_name").setTextValue("vendor").build())
        .build();
  }

  private static SearchRequest newSearchRequest(int topHits, LastHitInfo searchAfter) {
    SearchRequest.Builder builder =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(topHits)
            .addRetrieveFields("doc_id")
            .setQuery(vendorQuery());
    if (searchAfter != null) {
      builder.setSearchAfter(searchAfter);
    }
    return builder.build();
  }

  private static List<String> getDocIds(SearchResponse response) {
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < response.getHitsCount(); i++) {
      ids.add(response.getHits(i).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
    }
    return ids;
  }

  @Test
  public void testRelevanceSearchAfterFirstPage() {
    SearchRequest request = newSearchRequest(PAGE_SIZE, null);
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);

    assertEquals(NUM_DOCS, response.getTotalHits().getValue());
    assertEquals(PAGE_SIZE, response.getHitsCount());

    LastHitInfo lastHitInfo = response.getSearchState().getLastHitInfo();
    assertEquals(response.getSearchState().getLastDocId(), lastHitInfo.getLastDocId());
    assertEquals(response.getSearchState().getLastScore(), lastHitInfo.getLastScore(), 1e-6);
  }

  @Test
  public void testRelevanceSearchAfterPaginationNoOverlap() {
    Set<String> allSeenIds = new HashSet<>();
    LastHitInfo searchAfter = null;
    int pageCount = 0;
    int totalHits = 0;

    while (true) {
      SearchRequest request = newSearchRequest(PAGE_SIZE, searchAfter);
      SearchResponse response = getGrpcServer().getBlockingStub().search(request);
      int hitCount = response.getHitsCount();
      totalHits += hitCount;
      pageCount++;

      if (pageCount == 1) {
        assertEquals(
            "First page should have full total", NUM_DOCS, response.getTotalHits().getValue());
      }

      List<String> pageIds = getDocIds(response);
      for (String id : pageIds) {
        assertTrue("Duplicate doc_id across pages: " + id, allSeenIds.add(id));
      }

      if (hitCount == 0) {
        break;
      }
      searchAfter = response.getSearchState().getLastHitInfo();
    }

    assertEquals(NUM_DOCS, allSeenIds.size());
    assertEquals(NUM_DOCS, totalHits);
    assertTrue(pageCount >= 2);
  }

  @Test
  public void testRelevanceSearchAfterSecondPageDifferentFromFirst() {
    SearchRequest firstRequest = newSearchRequest(PAGE_SIZE, null);
    SearchResponse firstResponse = getGrpcServer().getBlockingStub().search(firstRequest);
    assertEquals(PAGE_SIZE, firstResponse.getHitsCount());

    List<String> firstPageIds = getDocIds(firstResponse);
    LastHitInfo lastHitInfo = firstResponse.getSearchState().getLastHitInfo();

    SearchRequest secondRequest = newSearchRequest(PAGE_SIZE, lastHitInfo);
    SearchResponse secondResponse = getGrpcServer().getBlockingStub().search(secondRequest);

    List<String> secondPageIds = getDocIds(secondResponse);
    for (String id : secondPageIds) {
      assertFalse(
          "Second page should not contain first page doc_id: " + id, firstPageIds.contains(id));
    }
  }

  @Test
  public void testRelevanceSearchAfterAfterLastReturnsZeroHits() {
    SearchRequest request = newSearchRequest(NUM_DOCS + 10, null);
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    assertEquals(NUM_DOCS, response.getHitsCount());

    LastHitInfo lastHitInfo = response.getSearchState().getLastHitInfo();
    SearchRequest afterLastRequest = newSearchRequest(PAGE_SIZE, lastHitInfo);
    SearchResponse afterLastResponse = getGrpcServer().getBlockingStub().search(afterLastRequest);

    assertEquals(0, afterLastResponse.getHitsCount());
  }
}
