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
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
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
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    int count = 0;
    for (int i = 0; i < NUM_DOCS; ++i) {
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
                      .addValue(String.valueOf((i + 10) % NUM_DOCS))
                      .build())
              .putFields(
                  "long_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(((i + 66) % NUM_DOCS) * 2))
                      .build())
              .putFields(
                  "float_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(((i + 33) % NUM_DOCS) * 1.25))
                      .build())
              .putFields(
                  "double_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(((i + 90) % NUM_DOCS) * 2.75))
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
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName("int_field").build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("90", "91", "92", "93", "94");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Integer> expectedSort = Arrays.asList(0, 1, 2, 3, 4);
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
  public void testReverseSortIntField() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("int_field").setReverse(true).build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("89", "88", "87", "86", "85");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Integer> expectedSort = Arrays.asList(99, 98, 97, 96, 95);
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
  public void testSortLongField() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName("long_field").build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("34", "35", "36", "37", "38");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Long> expectedSort = Arrays.asList(0L, 2L, 4L, 6L, 8L);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i).longValue(),
          hit.getSortedFieldsOrThrow("long_field").getFieldValue(0).getLongValue());

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testReverseSortLongField() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("long_field").setReverse(true).build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("33", "32", "31", "30", "29");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Long> expectedSort = Arrays.asList(198L, 196L, 194L, 192L, 190L);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i).longValue(),
          hit.getSortedFieldsOrThrow("long_field").getFieldValue(0).getLongValue());

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortFloatField() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName("float_field").build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("67", "68", "69", "70", "71");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Float> expectedSort = Arrays.asList(0.0F, 1.25F, 2.50F, 3.75F, 5.0F);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("float_field").getFieldValue(0).getFloatValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testReverseSortFloatField() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("float_field").setReverse(true).build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("66", "65", "64", "63", "62");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Float> expectedSort = Arrays.asList(123.75F, 122.50F, 121.25F, 120.0F, 118.75F);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("float_field").getFieldValue(0).getFloatValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testSortDoubleField() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(SortType.newBuilder().setFieldName("double_field").build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("10", "11", "12", "13", "14");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort = Arrays.asList(0.0, 2.75, 5.50, 8.25, 11.0);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("double_field").getFieldValue(0).getDoubleValue(),
          0.001);

      assertEquals(0.0, hit.getScore(), 0);
      assertEquals(6, hit.getFieldsCount());
    }
  }

  @Test
  public void testReverseSortDoubleField() {
    QuerySortField querySortField =
        QuerySortField.newBuilder()
            .setFields(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("double_field").setReverse(true).build())
                    .build())
            .build();
    SearchResponse searchResponse = doSortQuery(querySortField);
    assertEquals(5, searchResponse.getHitsCount());

    List<String> expectedIds = Arrays.asList("9", "8", "7", "6", "5");
    assertFields(expectedIds, searchResponse.getHitsList());

    List<Double> expectedSort = Arrays.asList(272.25, 269.5, 266.75, 264.0, 261.25);
    for (int i = 0; i < searchResponse.getHitsCount(); ++i) {
      var hit = searchResponse.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          expectedSort.get(i),
          hit.getSortedFieldsOrThrow("double_field").getFieldValue(0).getDoubleValue(),
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

    List<Double> expectedSort = Arrays.asList(167.5, 176.0, 184.5, 193.0, 198.0);
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

    List<Double> expectedSort = Arrays.asList(759.0, 750.5, 742.0, 733.5, 725.0);
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

    List<Double> expectedSort = Arrays.asList(-40.0, -39.25, -38.5, -37.75, -37.0);
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

    List<Double> expectedSort = Arrays.asList(234.25, 233.5, 232.75, 232.0, 231.25);
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

    List<Float> expectedSort = Arrays.asList(422.25F, 417.5F, 412.75F, 408.0F, 403.25F);
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

    List<Float> expectedSort = Arrays.asList(66.0F, 70.75F, 75.5F, 80.25F, 85.0F);
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

    List<Double> expectedSort = Arrays.asList(-403.25, -399.5, -395.75, -392.0, -388.25);
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
      int expectedInt = (id + 10) % NUM_DOCS;
      assertEquals(
          expectedInt, hits.get(i).getFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
      long expectedLong = ((id + 66) % NUM_DOCS) * 2;
      assertEquals(
          expectedLong, hits.get(i).getFieldsOrThrow("long_field").getFieldValue(0).getLongValue());
      float expectedFloat = ((id + 33) % NUM_DOCS) * 1.25F;
      assertEquals(
          expectedFloat,
          hits.get(i).getFieldsOrThrow("float_field").getFieldValue(0).getFloatValue(),
          0.001);
      double expectedDouble = ((id + 90) % NUM_DOCS) * 2.75;
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
