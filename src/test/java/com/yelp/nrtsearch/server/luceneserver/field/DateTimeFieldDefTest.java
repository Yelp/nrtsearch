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
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

public class DateTimeFieldDefTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsDateTime.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    AddDocumentRequest docWithTimestamp1 =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields(
                "timestamp_epoch_millis",
                MultiValuedField.newBuilder().addValue("1611742000").build())
            .putFields(
                "timestamp_string_format",
                MultiValuedField.newBuilder().addValue("2021-01-27 10:06:40").build())
            .build();
    AddDocumentRequest docWithTimestamp2 =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields(
                "timestamp_epoch_millis",
                MultiValuedField.newBuilder().addValue("1610742000").build())
            .putFields(
                "timestamp_string_format",
                MultiValuedField.newBuilder().addValue("2021-01-15 20:20:00").build())
            .build();
    docs.add(docWithTimestamp1);
    docs.add(docWithTimestamp2);
    addDocuments(docs.stream());
  }

  @Test
  public void testDateTimeRangeQueryEpochMillis() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setRangeQuery(
                    RangeQuery.newBuilder()
                        .setField("timestamp_epoch_millis")
                        .setLower("1610741000")
                        .setUpper("1610743000")
                        .build())
                .build(),
            List.of("doc_id"));
    assertFields(response, "2");
  }

  @Test
  public void testDateTimeRangeQueryStringDateFormat() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setRangeQuery(
                    RangeQuery.newBuilder()
                        .setField("timestamp_string_format")
                        .setLower("2021-01-27 10:05:40")
                        .setUpper("2021-01-27 10:07:40")
                        .build())
                .build(),
            List.of("doc_id"));
    assertFields(response, "1");
  }

  private SearchResponse doQuery(Query query, List<String> fields) {
    return doQueryWithNestedPath(query, fields, "");
  }

  private SearchResponse doQueryWithNestedPath(
      Query query, List<String> fields, String queryNestedPath) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addAllRetrieveFields(fields)
                .setQuery(query)
                .setQueryNestedPath(queryNestedPath)
                .build());
  }

  private void assertFields(SearchResponse response, String... expectedIds) {
    assertDataFields(response, "doc_id", expectedIds);
  }

  private void assertDataFields(
      SearchResponse response, String fieldName, String... expectedValues) {
    Set<String> seenSet = new HashSet<>();
    for (SearchResponse.Hit hit : response.getHitsList()) {
      String id = hit.getFieldsOrThrow(fieldName).getFieldValue(0).getTextValue();
      seenSet.add(id);
    }
    Set<String> expectedSet = new HashSet<>(Arrays.asList(expectedValues));
    assertEquals(seenSet, expectedSet);
  }
}
