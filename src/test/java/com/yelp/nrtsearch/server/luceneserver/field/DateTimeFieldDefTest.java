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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
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

  protected void initIndex(String indexName) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    docs.add(buildDocument(indexName, "0", String.valueOf(Long.MIN_VALUE), "1900-01-27 10:06:40"));
    docs.add(buildDocument(indexName, "1", "1611742000", "2021-01-27 10:06:40"));
    docs.add(buildDocument(indexName, "2", "1610742000", "2021-01-15 20:20:00"));
    docs.add(buildDocument(indexName, "3", "1612742000", "2021-02-15 20:20:00"));
    docs.add(buildDocument(indexName, "4", "1613742000", "2021-03-15 20:20:00"));
    docs.add(buildDocument(indexName, "5", String.valueOf(Long.MAX_VALUE), "2099-01-15 20:20:00"));
    addDocuments(docs.stream());
  }

  private AddDocumentRequest buildDocument(
      String indexName, String docId, String timestampMillis, String timestampFormatted) {
    return AddDocumentRequest.newBuilder()
        .setIndexName(indexName)
        .putFields("doc_id", MultiValuedField.newBuilder().addValue(docId).build())
        .putFields(
            "timestamp_epoch_millis",
            MultiValuedField.newBuilder().addValue(timestampMillis).build())
        .putFields(
            "timestamp_string_format",
            MultiValuedField.newBuilder().addValue(timestampFormatted).build())
        .build();
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

  @Test
  public void testIndexInvalidEpochMillisDateTime() throws Exception {

    String dateTimeField = "timestamp_epoch_millis";
    String dateTimeValue = "definitely not a long";
    String dateTimeFormat = "epoch_millis";

    List<AddDocumentRequest> docs = new ArrayList<>();
    AddDocumentRequest docWithTimestamp =
        AddDocumentRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields(dateTimeField, MultiValuedField.newBuilder().addValue(dateTimeValue).build())
            .build();

    docs.add(docWithTimestamp);
    try {
      addDocuments(docs.stream());
    } catch (Exception e) {
      assertEquals(
          formatAddDocumentsExceptionMessage(dateTimeField, dateTimeValue, dateTimeFormat),
          e.getMessage());
    }
  }

  @Test
  public void testIndexInvalidStringDateTime() throws Exception {

    String dateTimeField = "timestamp_string_format";
    String dateTimeValue = "1610742000";
    String dateTimeFormat = "yyyy-MM-dd HH:mm:ss";

    List<AddDocumentRequest> docs = new ArrayList<>();
    AddDocumentRequest docWithTimestamp =
        AddDocumentRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields(dateTimeField, MultiValuedField.newBuilder().addValue(dateTimeValue).build())
            .build();

    docs.add(docWithTimestamp);
    try {
      addDocuments(docs.stream());
    } catch (RuntimeException e) {
      assertEquals(
          formatAddDocumentsExceptionMessage(dateTimeField, dateTimeValue, dateTimeFormat),
          e.getMessage());
    }
  }

  @Test
  public void testRangeQueryEpochMillisInvalidFormat() {

    String dateTimeValueLower = "I'm not a long";
    String dateTimeValueUpper = "34234234.4234234";

    try {
      doQuery(
          Query.newBuilder()
              .setRangeQuery(
                  RangeQuery.newBuilder()
                      .setField("timestamp_epoch_millis")
                      .setLower(dateTimeValueLower)
                      .setUpper(dateTimeValueUpper)
                      .build())
              .build(),
          List.of("doc_id"));
    } catch (RuntimeException e) {
      assertEquals(
          String.format(
              "UNKNOWN: error while trying to execute search for index test_index. check logs for full searchRequest.\n"
                  + "For input string: \"%s\"",
              dateTimeValueLower),
          e.getMessage());
    }
  }

  @Test
  public void testRangeQueryStringDateTimeInvalidFormat() {

    String dateTimeValueLower = "34234234.4234234";
    String dateTimeValueUpepr = "I'm not a correct date string";

    try {
      doQuery(
          Query.newBuilder()
              .setRangeQuery(
                  RangeQuery.newBuilder()
                      .setField("timestamp_string_format")
                      .setLower(dateTimeValueLower)
                      .setUpper(dateTimeValueUpepr)
                      .build())
              .build(),
          List.of("doc_id"));
    } catch (RuntimeException e) {
      assertEquals(
          String.format(
              "UNKNOWN: error while trying to execute search for index test_index. check logs for full searchRequest.\n"
                  + "Text '%s' could not be parsed at index 0",
              dateTimeValueLower),
          e.getMessage());
    }
  }

  @Test
  public void testRangeQueryWithCombinationOfSpecifiedBoundsAndExclusive() {
    String dateFieldName = "timestamp_epoch_millis";

    // Both bounds defined

    // Both inclusive
    RangeQuery rangeQuery =
        RangeQuery.newBuilder()
            .setField(dateFieldName)
            .setLower("1610742000")
            .setUpper("1613742000")
            .build();
    assertRangeQuery(rangeQuery, "2", "1", "3", "4");

    // Lower exclusive, upper inclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(dateFieldName)
            .setLower("1610742000")
            .setUpper("1613742000")
            .setLowerExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "1", "3", "4");

    // Lower inclusive, upper exclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(dateFieldName)
            .setLower("1610742000")
            .setUpper("1613742000")
            .setUpperExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "2", "1", "3");

    // Both exclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(dateFieldName)
            .setLower("1610742000")
            .setUpper("1613742000")
            .setLowerExclusive(true)
            .setUpperExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "1", "3");

    // Only upper bound defined

    // Both inclusive
    rangeQuery = RangeQuery.newBuilder().setField(dateFieldName).setUpper("1612742000").build();
    assertRangeQuery(rangeQuery, "0", "2", "1", "3");

    // Lower exclusive, upper inclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(dateFieldName)
            .setUpper("1612742000")
            .setLowerExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "2", "1", "3");

    // Lower inclusive, upper exclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(dateFieldName)
            .setUpper("1612742000")
            .setUpperExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "0", "2", "1");

    // Both exclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(dateFieldName)
            .setUpper("1612742000")
            .setLowerExclusive(true)
            .setUpperExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "2", "1");

    // Only lower bound defined

    // Both inclusive
    rangeQuery = RangeQuery.newBuilder().setField(dateFieldName).setLower("1611742000").build();
    assertRangeQuery(rangeQuery, "1", "3", "4", "5");

    // Lower exclusive, upper inclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(dateFieldName)
            .setLower("1611742000")
            .setLowerExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "3", "4", "5");

    // Lower inclusive, upper exclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(dateFieldName)
            .setLower("1611742000")
            .setUpperExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "1", "3", "4");

    // Both exclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(dateFieldName)
            .setLower("1611742000")
            .setLowerExclusive(true)
            .setUpperExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "3", "4");
  }

  private void assertRangeQuery(RangeQuery rangeQuery, String... expectedIds) {
    String idFieldName = "doc_id";
    Query query = Query.newBuilder().setRangeQuery(rangeQuery).build();
    SearchResponse searchResponse = doQuery(query, List.of(idFieldName));
    assertEquals(expectedIds.length, searchResponse.getHitsCount());
    List<String> actualValues =
        searchResponse.getHitsList().stream()
            .map(
                hit ->
                    hit.getFieldsMap().get(idFieldName).getFieldValueList().get(0).getTextValue())
            .sorted()
            .collect(Collectors.toList());
    List<String> expected = Arrays.asList(expectedIds);
    expected.sort(Comparator.comparing(Function.identity()));
    assertEquals(expected, actualValues);
  }

  private SearchResponse doQuery(Query query, List<String> fields) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addAllRetrieveFields(fields)
                .setQuery(query)
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

  private String formatAddDocumentsExceptionMessage(
      String dateTimeField, String dateTimeValue, String dateTimeFormat) {
    return String.format(
        "io.grpc.StatusRuntimeException: INTERNAL: error while trying to addDocuments \n"
            + "java.lang.Exception: java.lang.IllegalArgumentException: %s "
            + "could not parse %s as date_time with format %s",
        dateTimeField, dateTimeValue, dateTimeFormat);
  }
}
