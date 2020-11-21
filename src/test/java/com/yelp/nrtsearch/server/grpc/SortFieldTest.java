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

import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;
import org.junit.Test;

public class SortFieldTest extends ServerTestCase {
  private static final String TEST_INDEX = "test_index";
  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;

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

    List<Integer> expectedSort = Arrays.asList(0, 1, 2, 3, 4);
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

    List<Integer> expectedSort = Arrays.asList(99, 98, 97, 96, 95);
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
