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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.StatusRuntimeException;
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
    docs.add(
        buildDocument(
            indexName,
            "0",
            String.valueOf(Long.MIN_VALUE),
            "1900-01-27T10:06:40",
            "1900-01-27 10:06:40"));
    docs.add(
        buildDocument(
            indexName, "1", "1611742000", "2021-01-27T10:06:40.000", "2021-01-27 10:06:40"));
    docs.add(buildDocument(indexName, "2", "1610742000", "2021-01-15", "2021-01-15 20:20:00"));
    docs.add(buildDocument(indexName, "3", "1612742000", "2021-02-15", "2021-02-15 20:20:00"));
    docs.add(buildDocument(indexName, "4", "1613742000", "2021-03-15", "2021-03-15 20:20:00"));
    docs.add(
        buildDocument(
            indexName,
            "5",
            String.valueOf(Long.MAX_VALUE),
            "2099-01-15T20:20:00",
            "2099-01-15 20:20:00"));
    docs.add(
        buildDocument(indexName, "6", "1683771201", "2023-05-11T02:13:21", "2023-05-11 02:13:21"));
    addDocuments(docs.stream());
  }

  private AddDocumentRequest buildDocument(
      String indexName,
      String docId,
      String timestampMillis,
      String strictDateOptionalTime,
      String timestampFormatted) {
    return AddDocumentRequest.newBuilder()
        .setIndexName(indexName)
        .putFields("doc_id", MultiValuedField.newBuilder().addValue(docId).build())
        .putFields(
            "timestamp_epoch_millis",
            MultiValuedField.newBuilder().addValue(timestampMillis).build())
        .putFields(
            "timestamp_strict_date_optional_time",
            MultiValuedField.newBuilder().addValue(strictDateOptionalTime).build())
        .putFields(
            "timestamp_string_format",
            MultiValuedField.newBuilder().addValue(timestampFormatted).build())
        .putFields("single_stored", MultiValuedField.newBuilder().addValue(timestampMillis).build())
        .putFields("stored_only", MultiValuedField.newBuilder().addValue(timestampMillis).build())
        .putFields(
            "multi_stored",
            MultiValuedField.newBuilder()
                .addValue(timestampMillis)
                .addValue(String.valueOf(Long.parseLong(timestampMillis) + 2))
                .build())
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
  public void testDateTimeRangeQueryDateOptionalTime() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setRangeQuery(
                    RangeQuery.newBuilder()
                        .setField("timestamp_strict_date_optional_time")
                        .setLower("2023-05-11")
                        .setUpper("2023-05-11T03:00:01.001")
                        .build())
                .build(),
            List.of("doc_id"));
    assertFields(response, "6");
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
  public void testIndexInvalidStrictDateOptionalTime() {

    String dateTimeField = "timestamp_strict_date_optional_time";
    String dateTimeFormat = "strict_date_optional_time";

    String dateTimeValue = "2023-05-11T03:00:01.001+02:00Z"; // the UTC offset is not supported
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

    dateTimeValue = "2023-05-11T03:00:01.001+02:00"; // other offset is not supported
    docs = new ArrayList<>();
    docWithTimestamp =
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

    dateTimeValue = "2023-05-1T03:00:01.001"; // date components without 0 padding are not supported
    docs = new ArrayList<>();
    docWithTimestamp =
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

    dateTimeValue = "2023-05-99T03:00:01.001"; // invalid date value
    docs = new ArrayList<>();
    docWithTimestamp =
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

    String dateTimeValueLower = "2023-01-01";
    String dateTimeValueUpper = "2023-05-11T03:00:01.001+02:00"; // offset is not supported

    try {
      doQuery(
          Query.newBuilder()
              .setRangeQuery(
                  RangeQuery.newBuilder()
                      .setField("timestamp_strict_date_optional_time")
                      .setLower(dateTimeValueLower)
                      .setUpper(dateTimeValueUpper)
                      .build())
              .build(),
          List.of("doc_id"));
    } catch (RuntimeException e) {
      assertEquals(
          String.format(
              "UNKNOWN: error while trying to execute search for index test_index. check logs for full searchRequest.\n"
                  + "Text \'%s\' could not be parsed, unparsed text found at index 23",
              dateTimeValueUpper),
          e.getMessage());
    }
  }

  @Test
  public void testRangeQueryDateOptionalTime() {

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
    String dateTimeValueUpper = "I'm not a correct date string";

    try {
      doQuery(
          Query.newBuilder()
              .setRangeQuery(
                  RangeQuery.newBuilder()
                      .setField("timestamp_string_format")
                      .setLower(dateTimeValueLower)
                      .setUpper(dateTimeValueUpper)
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
    assertRangeQuery(rangeQuery, "1", "3", "4", "5", "6");

    // Lower exclusive, upper inclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(dateFieldName)
            .setLower("1611742000")
            .setLowerExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "3", "4", "5", "6");

    // Lower inclusive, upper exclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(dateFieldName)
            .setLower("1611742000")
            .setUpperExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "1", "3", "4", "6");

    // Both exclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(dateFieldName)
            .setLower("1611742000")
            .setLowerExclusive(true)
            .setUpperExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "3", "4", "6");
  }

  @Test
  public void testTermQuery() {
    TermQuery termQuery =
        TermQuery.newBuilder()
            .setField("timestamp_epoch_millis")
            .setTextValue("1611742000")
            .build();
    assertTermQuery(termQuery, "1");
    termQuery =
        TermQuery.newBuilder().setField("timestamp_epoch_millis").setLongValue(1611742000).build();
    assertTermQuery(termQuery, "1");
  }

  @Test
  public void testTermQuery_stringFormat() {
    TermQuery termQuery =
        TermQuery.newBuilder()
            .setField("timestamp_string_format")
            .setTextValue("2021-02-15 20:20:00")
            .build();
    assertTermQuery(termQuery, "3");
    termQuery =
        TermQuery.newBuilder()
            .setField("timestamp_string_format")
            .setLongValue(1613420400000L)
            .build();
    assertTermQuery(termQuery, "3");
  }

  @Test
  public void testTermQuery_single() {
    TermQuery termQuery =
        TermQuery.newBuilder().setField("single_stored").setTextValue("1611742000").build();
    assertTermQuery(termQuery, "1");
    termQuery = TermQuery.newBuilder().setField("single_stored").setLongValue(1611742000).build();
    assertTermQuery(termQuery, "1");
  }

  @Test
  public void testTermQuery_multi() {
    TermQuery termQuery =
        TermQuery.newBuilder().setField("multi_stored").setTextValue("1613742002").build();
    assertTermQuery(termQuery, "4");
    termQuery = TermQuery.newBuilder().setField("multi_stored").setLongValue(1613742002).build();
    assertTermQuery(termQuery, "4");
  }

  @Test
  public void testTermQuery_notSearchable() {
    try {
      TermQuery termQuery =
          TermQuery.newBuilder().setField("stored_only").setTextValue("1611742000").build();
      assertTermQuery(termQuery);
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "Field stored_only is not searchable, which is required for TermQuery / TermInSetQuery"));
    }
  }

  @Test
  public void testTermInSetQuery() {
    TermInSetQuery termInSetQuery =
        TermInSetQuery.newBuilder()
            .setField("timestamp_epoch_millis")
            .setTextTerms(
                TermInSetQuery.TextTerms.newBuilder()
                    .addTerms("1611742000")
                    .addTerms("1612742000")
                    .build())
            .build();
    assertTermInSetQuery(termInSetQuery, "1", "3");
    termInSetQuery =
        TermInSetQuery.newBuilder()
            .setField("timestamp_epoch_millis")
            .setLongTerms(
                TermInSetQuery.LongTerms.newBuilder()
                    .addTerms(1611742000)
                    .addTerms(1612742000)
                    .build())
            .build();
    assertTermInSetQuery(termInSetQuery, "1", "3");
  }

  @Test
  public void testTermInSetQuery_stringFormat() {
    TermInSetQuery termInSetQuery =
        TermInSetQuery.newBuilder()
            .setField("timestamp_string_format")
            .setTextTerms(
                TermInSetQuery.TextTerms.newBuilder()
                    .addTerms("2021-02-15 20:20:00")
                    .addTerms("2021-03-15 20:20:00")
                    .build())
            .build();
    assertTermInSetQuery(termInSetQuery, "3", "4");
    termInSetQuery =
        TermInSetQuery.newBuilder()
            .setField("timestamp_string_format")
            .setLongTerms(
                TermInSetQuery.LongTerms.newBuilder()
                    .addTerms(1613420400000L)
                    .addTerms(1615839600000L)
                    .build())
            .build();
    assertTermInSetQuery(termInSetQuery, "3", "4");
  }

  @Test
  public void testTermInSetQuery_single() {
    TermInSetQuery termInSetQuery =
        TermInSetQuery.newBuilder()
            .setField("single_stored")
            .setTextTerms(
                TermInSetQuery.TextTerms.newBuilder()
                    .addTerms("1611742000")
                    .addTerms("1612742000")
                    .build())
            .build();
    assertTermInSetQuery(termInSetQuery, "1", "3");
    termInSetQuery =
        TermInSetQuery.newBuilder()
            .setField("single_stored")
            .setLongTerms(
                TermInSetQuery.LongTerms.newBuilder()
                    .addTerms(1611742000)
                    .addTerms(1612742000)
                    .build())
            .build();
    assertTermInSetQuery(termInSetQuery, "1", "3");
  }

  @Test
  public void testTermInSetQuery_multi() {
    TermInSetQuery termInSetQuery =
        TermInSetQuery.newBuilder()
            .setField("multi_stored")
            .setTextTerms(
                TermInSetQuery.TextTerms.newBuilder()
                    .addTerms("1612742002")
                    .addTerms("1613742002")
                    .build())
            .build();
    assertTermInSetQuery(termInSetQuery, "3", "4");
    termInSetQuery =
        TermInSetQuery.newBuilder()
            .setField("multi_stored")
            .setLongTerms(
                TermInSetQuery.LongTerms.newBuilder()
                    .addTerms(1612742002)
                    .addTerms(1613742002)
                    .build())
            .build();
    assertTermInSetQuery(termInSetQuery, "3", "4");
  }

  @Test
  public void testTermInSetQuery_notSearchable() {
    try {
      TermInSetQuery termInSetQuery =
          TermInSetQuery.newBuilder()
              .setField("stored_only")
              .setTextTerms(
                  TermInSetQuery.TextTerms.newBuilder()
                      .addTerms("1611742000")
                      .addTerms("1612742000")
                      .build())
              .build();
      assertTermInSetQuery(termInSetQuery);
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "Field stored_only is not searchable, which is required for TermQuery / TermInSetQuery"));
    }
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

  private void assertTermQuery(TermQuery termQuery, String... expectedIds) {
    String idFieldName = "doc_id";
    Query query = Query.newBuilder().setTermQuery(termQuery).build();
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

  private void assertTermInSetQuery(TermInSetQuery termInSetQuery, String... expectedIds) {
    String idFieldName = "doc_id";
    Query query = Query.newBuilder().setTermInSetQuery(termInSetQuery).build();
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
            + "%s could not parse %s as date_time with format %s",
        dateTimeField, dateTimeValue, dateTimeFormat);
  }
}
