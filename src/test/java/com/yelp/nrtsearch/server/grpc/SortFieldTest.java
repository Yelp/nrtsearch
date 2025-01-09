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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.type.LatLng;
import com.yelp.nrtsearch.server.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;
import org.junit.Test;

public class SortFieldTest extends ServerTestCase {
  private static final String TEST_INDEX = "test_index";
  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  protected List<String> getIndices() {
    return Collections.singletonList(TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/SortFieldRegisterFields.json");
  }

  @Override
  protected void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndexOrThrow(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    int count = 0;
    for (int i = 0; i < NUM_DOCS; ++i) {
      int intValue = ((i + 10) % NUM_DOCS) - 10;
      long longValue = ((i + 66) % NUM_DOCS) * 2 - 10;
      float floatValue = ((i + 33) % NUM_DOCS) * 1.25f - 10.0f;
      double doubleValue = ((i + 90) % NUM_DOCS) * 2.75 - 10.0;
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
                      .addValue(String.valueOf(intValue))
                      .build())
              .putFields(
                  "multi_int_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(intValue))
                      .addValue(String.valueOf(intValue + 2))
                      .build())
              .putFields(
                  "long_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(longValue))
                      .build())
              .putFields(
                  "multi_long_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(longValue))
                      .addValue(String.valueOf(longValue + 2))
                      .build())
              .putFields(
                  "float_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(floatValue))
                      .build())
              .putFields(
                  "multi_float_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(floatValue))
                      .addValue(String.valueOf(floatValue + 2.0f))
                      .build())
              .putFields(
                  "double_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(doubleValue))
                      .build())
              .putFields(
                  "multi_double_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(doubleValue))
                      .addValue(String.valueOf(doubleValue + 2.0))
                      .build())
              .putFields(
                  "lat_lon_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(44.9985 + ((i % NUM_DOCS) / 1000d)))
                      .addValue(String.valueOf(-99.9985 - ((i % NUM_DOCS) / 1000d)))
                      .build())
              .putFields(
                  "nested_object_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(
                          OBJECT_MAPPER.writeValueAsString(
                              Map.of(
                                  "nested_lat_lon_field",
                                  List.of(
                                      44.9985 + ((i % NUM_DOCS) / 1000d),
                                      -99.9985 - ((i % NUM_DOCS) / 1000d)))))
                      .build())
              .build();
      addDocuments(Stream.of(request));
      count++;
      if ((count % SEGMENT_CHUNK) == 0) {
        writer.commit();
      }
    }
  }

  @Test
  public void testSortIntField() {
    sortIntField("int_field");
  }

  @Test
  public void testSortMultiIntField() {
    sortIntField("multi_int_field");
  }

  private void sortIntField(String fieldName) {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName(fieldName).build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("90", "91", "92", "93", "94");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Integer> expectedSort = Arrays.asList(-10, -9, -8, -7, -6);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i).intValue(),
          hit.getSortedFieldsOrThrow(fieldName).getFieldValue(0).getIntValue());

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortMultiIntField_max() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder()
                            .setFieldName("multi_int_field")
                            .setSelector(Selector.MAX)
                            .build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("90", "91", "92", "93", "94");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Integer> expectedSort = Arrays.asList(-8, -7, -6, -5, -4);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i).intValue(),
          hit.getSortedFieldsOrThrow("multi_int_field").getFieldValue(0).getIntValue());

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testReverseSortIntField() {
    reverseSortIntField("int_field");
  }

  @Test
  public void testReverseSortMultiIntField() {
    reverseSortIntField("multi_int_field");
  }

  private void reverseSortIntField(String fieldName) {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName(fieldName).setReverse(true).build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("89", "88", "87", "86", "85");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Integer> expectedSort = Arrays.asList(89, 88, 87, 86, 85);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i).intValue(),
          hit.getSortedFieldsOrThrow(fieldName).getFieldValue(0).getIntValue());

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortLongField() {
    sortLongField("long_field");
  }

  @Test
  public void testSortMultiLongField() {
    sortLongField("multi_long_field");
  }

  private void sortLongField(String fieldName) {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName(fieldName).build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("34", "35", "36", "37", "38");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Long> expectedSort = Arrays.asList(-10L, -8L, -6L, -4L, -2L);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i).longValue(),
          hit.getSortedFieldsOrThrow(fieldName).getFieldValue(0).getLongValue());

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortMultiLongField_max() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder()
                            .setFieldName("multi_long_field")
                            .setSelector(Selector.MAX)
                            .build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("34", "35", "36", "37", "38");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Long> expectedSort = Arrays.asList(-8L, -6L, -4L, -2L, 0L);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i).longValue(),
          hit.getSortedFieldsOrThrow("multi_long_field").getFieldValue(0).getLongValue());

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testReverseSortLongField() {
    reverseSortLongField("long_field");
  }

  @Test
  public void testReverseSortMultiLongField() {
    reverseSortLongField("multi_long_field");
  }

  private void reverseSortLongField(String fieldName) {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName(fieldName).setReverse(true).build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("33", "32", "31", "30", "29");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Long> expectedSort = Arrays.asList(188L, 186L, 184L, 182L, 180L);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i).longValue(),
          hit.getSortedFieldsOrThrow(fieldName).getFieldValue(0).getLongValue());

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortFloatField() {
    sortFloatField("float_field");
  }

  @Test
  public void testSortMultiFloatField() {
    sortFloatField("multi_float_field");
  }

  private void sortFloatField(String fieldName) {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName(fieldName).build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("67", "68", "69", "70", "71");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Float> expectedSort = Arrays.asList(-10.0F, -8.75F, -7.50F, -6.25F, -5.0F);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow(fieldName).getFieldValue(0).getFloatValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortMultiFloatField_max() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder()
                            .setFieldName("multi_float_field")
                            .setSelector(Selector.MAX)
                            .build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("67", "68", "69", "70", "71");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Float> expectedSort = Arrays.asList(-8.0F, -6.75F, -5.50F, -4.25F, -3.0F);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("multi_float_field").getFieldValue(0).getFloatValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testReverseSortFloatField() {
    reverseSortFloatField("float_field");
  }

  @Test
  public void testReverseSortMultiFloatField() {
    reverseSortFloatField("multi_float_field");
  }

  private void reverseSortFloatField(String fieldName) {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName(fieldName).setReverse(true).build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("66", "65", "64", "63", "62");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Float> expectedSort = Arrays.asList(113.75F, 112.50F, 111.25F, 110.0F, 108.75F);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow(fieldName).getFieldValue(0).getFloatValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortDoubleField() {
    sortDoubleField("double_field");
  }

  @Test
  public void testSortMultiDoubleField() {
    sortDoubleField("multi_double_field");
  }

  private void sortDoubleField(String fieldName) {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName(fieldName).build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("10", "11", "12", "13", "14");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort = Arrays.asList(-10.0, -7.25, -4.50, -1.75, 1.0);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow(fieldName).getFieldValue(0).getDoubleValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortMultiDoubleField_max() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder()
                            .setFieldName("multi_double_field")
                            .setSelector(Selector.MAX)
                            .build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("10", "11", "12", "13", "14");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort = Arrays.asList(-8.0, -5.25, -2.50, 0.25, 3.0);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("multi_double_field").getFieldValue(0).getDoubleValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testReverseSortDoubleField() {
    reverseSortDoubleField("double_field");
  }

  @Test
  public void testReverseSortMultiDoubleField() {
    reverseSortDoubleField("multi_double_field");
  }

  private void reverseSortDoubleField(String fieldName) {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName(fieldName).setReverse(true).build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("9", "8", "7", "6", "5");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort = Arrays.asList(262.25, 259.5, 256.75, 254.0, 251.25);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow(fieldName).getFieldValue(0).getDoubleValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortIndexVirtualField() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("index_virtual_field").build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("34", "35", "36", "37", "67");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort = Arrays.asList(117.5, 126.0, 134.5, 143.0, 148.0);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("index_virtual_field").getFieldValue(0).getDoubleValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testReverseSortIndexVirtualField() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder()
                            .setFieldName("index_virtual_field")
                            .setReverse(true)
                            .build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("33", "32", "31", "30", "29");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort = Arrays.asList(709.0, 700.5, 692.0, 683.5, 675.0);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("index_virtual_field").getFieldValue(0).getDoubleValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortQueryVirtualField() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("query_virtual_field").build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQueryWithVirtual(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("10", "11", "12", "13", "14");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort = Arrays.asList(-30.0, -29.25, -28.5, -27.75, -27.0);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("query_virtual_field").getFieldValue(0).getDoubleValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(7, hit.getFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getFieldsOrThrow("query_virtual_field").getFieldValue(0).getDoubleValue(),
          0.001);
    }
  }

  @Test
  public void testReverseSortQueryVirtualField() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder()
                            .setFieldName("query_virtual_field")
                            .setReverse(true)
                            .build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQueryWithVirtual(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("9", "8", "7", "6", "5");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort = Arrays.asList(244.25, 243.5, 242.75, 242.0, 241.25);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("query_virtual_field").getFieldValue(0).getDoubleValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(7, hit.getFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getFieldsOrThrow("query_virtual_field").getFieldValue(0).getDoubleValue(),
          0.001);
    }
  }

  @Test
  public void testSortAtomDocId() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName("doc_id").build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());
    List<String> expectedIds = Arrays.asList("0", "1", "10", "11", "12");
    assertFields(expectedIds, searchResponse.getHitsList());
  }

  @Test
  public void testSortAtomDocIdSearchAfter() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName("doc_id").build())
                    .build())
            .build();
    LastHitInfo searchAfter = LastHitInfo.newBuilder().addLastFieldValues("1").build();
    SearchResponse searchResponse = dosSortQuerySearchAfter(querySortField, searchAfter);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("1", "10", "11", "12", "13");
    assertFields(expectedIds, searchResponse.getHitsList());
  }

  @Test
  public void testSortDocId() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName("docid").build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("0", "1", "2", "3", "4");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Integer> expectedSort = Arrays.asList(1, 3, 5, 7, 9);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i).intValue(),
          hit.getSortedFieldsOrThrow("docid").getFieldValue(0).getIntValue());
      assertEquals(expectedSort.get(i).intValue(), hit.getLuceneDocId());

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testReverseSortDocId() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("docid").setReverse(true).build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("99", "98", "97", "96", "95");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Integer> expectedSort = Arrays.asList(199, 197, 195, 193, 191);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i).intValue(),
          hit.getSortedFieldsOrThrow("docid").getFieldValue(0).getIntValue());
      assertEquals(expectedSort.get(i).intValue(), hit.getLuceneDocId());

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortScore() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName("score").build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQueryWithScore(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("9", "8", "7", "6", "5");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Float> expectedSort = Arrays.asList(402.25F, 397.5F, 392.75F, 388.0F, 383.25F);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("score").getFieldValue(0).getFloatValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testReverseSortScore() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("score").setReverse(true).build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQueryWithScore(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("34", "35", "36", "37", "38");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Float> expectedSort = Arrays.asList(46.0F, 50.75F, 55.5F, 60.25F, 65.0F);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("score").getFieldValue(0).getFloatValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortVirtualFieldNeedsScore() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("query_virtual_field").build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQueryWithVirtualUsingScore(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("9", "8", "7", "6", "5");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort = Arrays.asList(-393.25, -389.5, -385.75, -382.0, -378.25);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("query_virtual_field").getFieldValue(0).getDoubleValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(7, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortLanLonDistance() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder()
                            .setFieldName("lat_lon_field")
                            .setOrigin(Point.newBuilder().setLatitude(45).setLongitude(-100)))
                    .build())
            .build();
    SearchResponse searchResponse = doSortQueryWithGeoRadius(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("2", "1", "3", "0", "4");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort =
        Arrays.asList(
            68.09342498718514,
            68.09451648799866,
            204.27675784229385,
            204.27936772550663,
            340.4630661331218);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("lat_lon_field").getFieldValue(0).getDoubleValue(),
          0.00001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(7, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortLanLonDistanceInMiles() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder()
                            .setFieldName("lat_lon_field")
                            .setOrigin(Point.newBuilder().setLatitude(45).setLongitude(-100))
                            .setUnit("mi"))
                    .build())
            .build();
    SearchResponse searchResponse = doSortQueryWithGeoRadius(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("2", "1", "3", "0", "4");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort =
        Arrays.asList(
            0.0423112926678107,
            0.042311970894972524,
            0.12693169256684328,
            0.12693331427308682,
            0.21155394131591618);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("lat_lon_field").getFieldValue(0).getDoubleValue(),
          0.00000000001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(7, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortLanLonDistanceInInnerHit() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder()
                            .setFieldName("nested_object_field.nested_lat_lon_field")
                            .setOrigin(Point.newBuilder().setLatitude(45).setLongitude(-100)))
                    .build())
            .build();

    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(5)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_field")
                    .addRetrieveFields("long_field")
                    .addRetrieveFields("float_field")
                    .addRetrieveFields("double_field")
                    .addRetrieveFields("index_virtual_field")
                    .addRetrieveFields("lat_lon_field")
                    .setQuery(
                        Query.newBuilder()
                            .setGeoRadiusQuery(
                                GeoRadiusQuery.newBuilder()
                                    .setCenter(
                                        LatLng.newBuilder()
                                            .setLatitude(45.0)
                                            .setLongitude(-100.0)
                                            .build())
                                    .setField("lat_lon_field")
                                    .setRadius("0.8 mi"))
                            .build())
                    .putInnerHits(
                        "inner",
                        InnerHit.newBuilder()
                            .setQueryNestedPath("nested_object_field")
                            .setQuerySort(querySortField)
                            .setTopHits(1)
                            .build())
                    .build());

    assertEquals(5, searchResponse.getHitsCount());

    // We don't set querySort at the top level, now ordered by (score, id)
    List<String> expectedIds = Arrays.asList("0", "1", "2", "3", "4");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort =
        Arrays.asList(
            204.27936772550663,
            68.09451648799866,
            68.09342498718514,
            204.27675784229385,
            340.4630661331218);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      var innerHit = hit.getInnerHitsOrThrow("inner").getHits(0);
      assertEquals(0, hit.getSortedFieldsCount());
      assertEquals(1, innerHit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          innerHit
              .getSortedFieldsOrThrow("nested_object_field.nested_lat_lon_field")
              .getFieldValue(0)
              .getDoubleValue(),
          0.00001);

      assertEquals(1.0, hit.getScore(), 0);
      assertEquals(7, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortIntFieldWithSearchAfter() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName("int_field").build())
                    .build())
            .build();
    LastHitInfo searchAfter = LastHitInfo.newBuilder().addLastFieldValues("-9").build();
    SearchResponse searchResponse = dosSortQuerySearchAfter(querySortField, searchAfter);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("91", "92", "93", "94", "95");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Integer> expectedSort = Arrays.asList(-9, -8, -7, -6, -5);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i).intValue(),
          hit.getSortedFieldsOrThrow("int_field").getFieldValue(0).getIntValue());

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortDocIdWithSearchAfter() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName("docid").build())
                    .build())
            .build();
    LastHitInfo searchAfter =
        LastHitInfo.newBuilder().setLastDocId(3).addLastFieldValues("3").build();
    SearchResponse searchResponse = dosSortQuerySearchAfter(querySortField, searchAfter);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("2", "3", "4", "5", "6");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Integer> expectedSort = Arrays.asList(5, 7, 9, 11, 13);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i).intValue(),
          hit.getSortedFieldsOrThrow("docid").getFieldValue(0).getIntValue());
      assertEquals(expectedSort.get(i).intValue(), hit.getLuceneDocId());

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortVirtualFieldWithSearchAfter() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("query_virtual_field").build())
                    .build())
            .build();
    LastHitInfo searchAfter =
        LastHitInfo.newBuilder().setLastDocId(23).addLastFieldValues("-29.25").build();
    SearchResponse searchResponse =
        doSortQueryWithVirtualWithSearchAfter(querySortField, searchAfter);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("12", "13", "14", "15", "16");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort = Arrays.asList(-28.5, -27.75, -27.0, -26.25, -25.5);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("query_virtual_field").getFieldValue(0).getDoubleValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(7, hit.getFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getFieldsOrThrow("query_virtual_field").getFieldValue(0).getDoubleValue(),
          0.001);
    }
  }

  @Test
  public void testSortLatLonDistanceWithSearchAfter() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder()
                            .setFieldName("lat_lon_field")
                            .setOrigin(Point.newBuilder().setLatitude(45).setLongitude(-100)))
                    .build())
            .build();
    LastHitInfo searchAfter =
        LastHitInfo.newBuilder().addLastFieldValues("68.09451648799867").build();
    SearchResponse searchResponse =
        doSortQueryWithGeoRadiusWithSearchAfter(querySortField, searchAfter);
    assertEquals(3, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("3", "0", "4");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort =
        Arrays.asList(204.27675784229385, 204.27936772550663, 340.4630661331218);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("lat_lon_field").getFieldValue(0).getDoubleValue(),
          0.00001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(7, hit.getFieldsCount());
    }
  }

  private SearchResponse dosSortQuerySearchAfter(
      QuerySortField querySortField, LastHitInfo searchAfter) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setStartHit(0)
                .setTopHits(5)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("int_field")
                .addRetrieveFields("long_field")
                .addRetrieveFields("float_field")
                .addRetrieveFields("double_field")
                .addRetrieveFields("index_virtual_field")
                .setQuery(Query.newBuilder().build())
                .setQuerySort(querySortField)
                .setSearchAfter(searchAfter)
                .build());
  }

  private SearchResponse doSortQuery(QuerySortField querySortField) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setStartHit(0)
                .setTopHits(5)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("int_field")
                .addRetrieveFields("long_field")
                .addRetrieveFields("float_field")
                .addRetrieveFields("double_field")
                .addRetrieveFields("index_virtual_field")
                .setQuery(Query.newBuilder().build())
                .setQuerySort(querySortField)
                .build());
  }

  private SearchResponse doSortQueryWithVirtualWithSearchAfter(
      QuerySortField querySortField, LastHitInfo searchAfter) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setStartHit(0)
                .setTopHits(5)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("int_field")
                .addRetrieveFields("long_field")
                .addRetrieveFields("float_field")
                .addRetrieveFields("double_field")
                .addRetrieveFields("index_virtual_field")
                .addRetrieveFields("query_virtual_field")
                .setQuery(Query.newBuilder().build())
                .setQuerySort(querySortField)
                .addVirtualFields(
                    VirtualField.newBuilder()
                        .setName("query_virtual_field")
                        .setScript(
                            Script.newBuilder()
                                .setLang("js")
                                .setSource("-int_field * 2.0 + double_field")
                                .build())
                        .build())
                .setSearchAfter(searchAfter)
                .build());
  }

  private SearchResponse doSortQueryWithVirtual(QuerySortField querySortField) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setStartHit(0)
                .setTopHits(5)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("int_field")
                .addRetrieveFields("long_field")
                .addRetrieveFields("float_field")
                .addRetrieveFields("double_field")
                .addRetrieveFields("index_virtual_field")
                .addRetrieveFields("query_virtual_field")
                .setQuery(Query.newBuilder().build())
                .setQuerySort(querySortField)
                .addVirtualFields(
                    VirtualField.newBuilder()
                        .setName("query_virtual_field")
                        .setScript(
                            Script.newBuilder()
                                .setLang("js")
                                .setSource("-int_field * 2.0 + double_field")
                                .build())
                        .build())
                .build());
  }

  private SearchResponse doSortQueryWithGeoRadiusWithSearchAfter(
      QuerySortField querySortField, LastHitInfo searchAfter) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setStartHit(0)
                .setTopHits(3)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("int_field")
                .addRetrieveFields("long_field")
                .addRetrieveFields("float_field")
                .addRetrieveFields("double_field")
                .addRetrieveFields("index_virtual_field")
                .addRetrieveFields("lat_lon_field")
                .setQuery(
                    Query.newBuilder()
                        .setGeoRadiusQuery(
                            GeoRadiusQuery.newBuilder()
                                .setCenter(
                                    LatLng.newBuilder()
                                        .setLatitude(45.0)
                                        .setLongitude(-100.0)
                                        .build())
                                .setField("lat_lon_field")
                                .setRadius("0.8 mi"))
                        .build())
                .setQuerySort(querySortField)
                .setSearchAfter(searchAfter)
                .build());
  }

  private SearchResponse doSortQueryWithGeoRadius(QuerySortField querySortField) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setStartHit(0)
                .setTopHits(5)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("int_field")
                .addRetrieveFields("long_field")
                .addRetrieveFields("float_field")
                .addRetrieveFields("double_field")
                .addRetrieveFields("index_virtual_field")
                .addRetrieveFields("lat_lon_field")
                .setQuery(
                    Query.newBuilder()
                        .setGeoRadiusQuery(
                            GeoRadiusQuery.newBuilder()
                                .setCenter(
                                    LatLng.newBuilder()
                                        .setLatitude(45.0)
                                        .setLongitude(-100.0)
                                        .build())
                                .setField("lat_lon_field")
                                .setRadius("0.8 mi"))
                        .build())
                .setQuerySort(querySortField)
                .build());
  }

  private SearchResponse doSortQueryWithScore(QuerySortField querySortField) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setStartHit(0)
                .setTopHits(5)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("int_field")
                .addRetrieveFields("long_field")
                .addRetrieveFields("float_field")
                .addRetrieveFields("double_field")
                .addRetrieveFields("index_virtual_field")
                .setQuery(
                    Query.newBuilder()
                        .setFunctionScoreQuery(
                            FunctionScoreQuery.newBuilder()
                                .setQuery(
                                    Query.newBuilder()
                                        .setRangeQuery(
                                            RangeQuery.newBuilder()
                                                .setUpper("100")
                                                .setLower("0")
                                                .setField("int_field")
                                                .build())
                                        .build())
                                .setScript(
                                    Script.newBuilder()
                                        .setLang("js")
                                        .setSource("double_field + long_field")
                                        .build())
                                .build())
                        .build())
                .setQuerySort(querySortField)
                .build());
  }

  private SearchResponse doSortQueryWithVirtualUsingScore(QuerySortField querySortField) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setStartHit(0)
                .setTopHits(5)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("int_field")
                .addRetrieveFields("long_field")
                .addRetrieveFields("float_field")
                .addRetrieveFields("double_field")
                .addRetrieveFields("index_virtual_field")
                .addRetrieveFields("query_virtual_field")
                .setQuery(
                    Query.newBuilder()
                        .setFunctionScoreQuery(
                            FunctionScoreQuery.newBuilder()
                                .setQuery(
                                    Query.newBuilder()
                                        .setRangeQuery(
                                            RangeQuery.newBuilder()
                                                .setUpper("100")
                                                .setLower("0")
                                                .setField("int_field")
                                                .build())
                                        .build())
                                .setScript(
                                    Script.newBuilder()
                                        .setLang("js")
                                        .setSource("double_field + long_field")
                                        .build())
                                .build())
                        .build())
                .addVirtualFields(
                    VirtualField.newBuilder()
                        .setName("query_virtual_field")
                        .setScript(
                            Script.newBuilder()
                                .setLang("js")
                                .setSource("-_score + int_field")
                                .build())
                        .build())
                .setQuerySort(querySortField)
                .build());
  }

  private void assertFields(List<String> ids, List<SearchResponse.Hit> hits) {
    assertEquals(ids.size(), hits.size());
    for (int i = 0; i < ids.size(); ++i) {
      String idStr = hits.get(i).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue();
      assertEquals(ids.get(i), idStr);
      int id = Integer.parseInt(idStr);
      int expectedInt = ((id + 10) % NUM_DOCS) - 10;
      assertEquals(
          expectedInt, hits.get(i).getFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
      long expectedLong = ((id + 66) % NUM_DOCS) * 2 - 10;
      assertEquals(
          expectedLong, hits.get(i).getFieldsOrThrow("long_field").getFieldValue(0).getLongValue());
      float expectedFloat = ((id + 33) % NUM_DOCS) * 1.25F - 10.0F;
      assertEquals(
          expectedFloat,
          hits.get(i).getFieldsOrThrow("float_field").getFieldValue(0).getFloatValue(),
          0.001);
      double expectedDouble = ((id + 90) % NUM_DOCS) * 2.75 - 10.0;
      assertEquals(
          expectedDouble,
          hits.get(i).getFieldsOrThrow("double_field").getFieldValue(0).getDoubleValue(),
          0.001);
      double expectedIndexVirtual = expectedFloat * 2.0 + expectedLong * 3;
      assertEquals(
          expectedIndexVirtual,
          hits.get(i).getFieldsOrThrow("index_virtual_field").getFieldValue(0).getDoubleValue(),
          0.001);
    }
  }
}
