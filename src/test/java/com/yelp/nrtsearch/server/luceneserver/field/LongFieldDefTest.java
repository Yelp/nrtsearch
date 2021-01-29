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
package com.yelp.nrtsearch.server.luceneserver.field;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Test;

public class LongFieldDefTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final String fieldName = "long_field";
  private static final List<String> values =
      Arrays.asList(
          String.valueOf(Long.MIN_VALUE), "1", "10", "20", "30", String.valueOf(Long.MAX_VALUE));

  private Map<String, AddDocumentRequest.MultiValuedField> getFieldsMapForOneDocument(
      String value) {
    Map<String, AddDocumentRequest.MultiValuedField> fieldsMap = new HashMap<>();
    fieldsMap.put(
        fieldName, AddDocumentRequest.MultiValuedField.newBuilder().addValue(value).build());
    return fieldsMap;
  }

  private List<AddDocumentRequest> buildDocuments(String indexName) {
    List<AddDocumentRequest> documentRequests = new ArrayList<>();
    for (String value : values) {
      documentRequests.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(indexName)
              .putAllFields(getFieldsMapForOneDocument(value))
              .build());
    }
    return documentRequests;
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsLong.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    List<AddDocumentRequest> documents = buildDocuments(name);
    addDocuments(documents.stream());
  }

  private SearchResponse getSearchResponse(Query query) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setQuery(query)
                .setTopHits(10)
                .addRetrieveFields(fieldName)
                .build());
  }

  @Test
  public void testRangeQuery() {
    // Both bounds defined

    // Both inclusive
    RangeQuery rangeQuery =
        RangeQuery.newBuilder().setField(fieldName).setLower("1").setUpper("30").build();
    assertRangeQuery(rangeQuery, 1L, 10L, 20L, 30L);

    // Lower exclusive, upper inclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(fieldName)
            .setLower("1")
            .setUpper("30")
            .setLowerExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, 10L, 20L, 30L);

    // Lower inclusive, upper exclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(fieldName)
            .setLower("1")
            .setUpper("30")
            .setUpperExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, 1L, 10L, 20L);

    // Both exclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(fieldName)
            .setLower("1")
            .setUpper("30")
            .setLowerExclusive(true)
            .setUpperExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, 10L, 20L);

    // Only upper bound defined

    // Both inclusive
    rangeQuery = RangeQuery.newBuilder().setField(fieldName).setUpper("20").build();
    assertRangeQuery(rangeQuery, Long.MIN_VALUE, 1L, 10L, 20L);

    // Lower exclusive, upper inclusive
    rangeQuery =
        RangeQuery.newBuilder().setField(fieldName).setUpper("20").setLowerExclusive(true).build();
    assertRangeQuery(rangeQuery, 1L, 10L, 20L);

    // Lower inclusive, upper exclusive
    rangeQuery =
        RangeQuery.newBuilder().setField(fieldName).setUpper("20").setUpperExclusive(true).build();
    assertRangeQuery(rangeQuery, Long.MIN_VALUE, 1L, 10L);

    // Both exclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(fieldName)
            .setUpper("20")
            .setLowerExclusive(true)
            .setUpperExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, 1L, 10L);

    // Only lower bound defined

    // Both inclusive
    rangeQuery = RangeQuery.newBuilder().setField(fieldName).setLower("10").build();
    assertRangeQuery(rangeQuery, 10L, 20L, 30L, Long.MAX_VALUE);

    // Lower exclusive, upper inclusive
    rangeQuery =
        RangeQuery.newBuilder().setField(fieldName).setLower("10").setLowerExclusive(true).build();
    assertRangeQuery(rangeQuery, 20L, 30L, Long.MAX_VALUE);

    // Lower inclusive, upper exclusive
    rangeQuery =
        RangeQuery.newBuilder().setField(fieldName).setLower("10").setUpperExclusive(true).build();
    assertRangeQuery(rangeQuery, 10L, 20L, 30L);

    // Both exclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(fieldName)
            .setLower("10")
            .setLowerExclusive(true)
            .setUpperExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, 20L, 30L);
  }

  private void assertRangeQuery(RangeQuery rangeQuery, Long... expectedValues) {
    Query query = Query.newBuilder().setRangeQuery(rangeQuery).build();
    SearchResponse searchResponse = getSearchResponse(query);
    assertEquals(expectedValues.length, searchResponse.getHitsCount());
    List<Long> actualValues =
        searchResponse.getHitsList().stream()
            .map(hit -> hit.getFieldsMap().get(fieldName).getFieldValueList().get(0).getLongValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.asList(expectedValues), actualValues);
  }
}
