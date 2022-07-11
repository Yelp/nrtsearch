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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.TotalHits.Relation;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Test;

public class TotalHitsThresholdTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/registerFieldsHitsThreshold.json");
  }

  protected SettingsRequest getSettings(String name) {
    return SettingsRequest.newBuilder()
        .setIndexName(name)
        .setIndexSort(
            SortFields.newBuilder()
                .addSortedFields(
                    SortType.newBuilder().setFieldName("int_field").setReverse(true).build())
                .build())
        .build();
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      docs.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id", MultiValuedField.newBuilder().addValue(String.valueOf(i + 10)).build())
              .putFields(
                  "int_field", MultiValuedField.newBuilder().addValue(String.valueOf(i)).build())
              .putFields(
                  "text_field", MultiValuedField.newBuilder().addValue("term" + (i / 3)).build())
              .build());
    }
    addDocuments(docs.stream());
  }

  @Test
  public void testHitsThresholdScore() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("text_field")
                                    .setTextValue("term1")
                                    .build())
                            .build())
                    .setTopHits(3)
                    .setTotalHitsThreshold(2)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_field")
                    .addRetrieveFields("text_field")
                    .build());

    assertEquals(3, response.getHitsCount());
    assertEquals(3, response.getTotalHits().getValue());
    assertEquals(Relation.GREATER_THAN_OR_EQUAL_TO, response.getTotalHits().getRelation());
    assertScoreHit(5, "term1", response.getHits(0));
    assertScoreHit(4, "term1", response.getHits(1));
    assertScoreHit(3, "term1", response.getHits(2));
  }

  @Test
  public void testScoreDefaultThreshold() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("text_field")
                                    .setTextValue("term1")
                                    .build())
                            .build())
                    .setTopHits(3)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_field")
                    .addRetrieveFields("text_field")
                    .build());

    assertEquals(3, response.getHitsCount());
    assertEquals(3, response.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, response.getTotalHits().getRelation());
    assertScoreHit(5, "term1", response.getHits(0));
    assertScoreHit(4, "term1", response.getHits(1));
    assertScoreHit(3, "term1", response.getHits(2));
  }

  @Test
  public void testHitsThresholdAdditionalCollectorScore() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("text_field")
                                    .setTextValue("term1")
                                    .build())
                            .build())
                    .setTopHits(3)
                    .setTotalHitsThreshold(2)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_field")
                    .addRetrieveFields("text_field")
                    .putCollectors(
                        "test_collection",
                        Collector.newBuilder()
                            .setTopHitsCollector(
                                TopHitsCollector.newBuilder()
                                    .setTopHits(2)
                                    .addRetrieveFields("doc_id")
                                    .addRetrieveFields("int_field")
                                    .setQuerySort(
                                        QuerySortField.newBuilder()
                                            .setFields(
                                                SortFields.newBuilder()
                                                    .addSortedFields(
                                                        SortType.newBuilder()
                                                            .setFieldName("int_field")
                                                            .setReverse(true)
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build());

    assertEquals(3, response.getHitsCount());
    assertEquals(3, response.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, response.getTotalHits().getRelation());
    assertScoreHit(5, "term1", response.getHits(0));
    assertScoreHit(4, "term1", response.getHits(1));
    assertScoreHit(3, "term1", response.getHits(2));

    HitsResult hitsResult = response.getCollectorResultsOrThrow("test_collection").getHitsResult();
    assertEquals(2, hitsResult.getHitsCount());
    assertEquals(3, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertHit(5, hitsResult.getHits(0));
    assertHit(4, hitsResult.getHits(1));
  }

  @Test
  public void testHitsThresholdSort() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setQuery(Query.newBuilder().build())
                    .setTopHits(3)
                    .setTotalHitsThreshold(5)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_field")
                    .setQuerySort(
                        QuerySortField.newBuilder()
                            .setFields(
                                SortFields.newBuilder()
                                    .addSortedFields(
                                        SortType.newBuilder()
                                            .setFieldName("int_field")
                                            .setReverse(true)
                                            .build())
                                    .build())
                            .build())
                    .build());

    assertEquals(3, response.getHitsCount());
    assertEquals(6, response.getTotalHits().getValue());
    assertEquals(Relation.GREATER_THAN_OR_EQUAL_TO, response.getTotalHits().getRelation());
    assertHit(9, response.getHits(0));
    assertHit(8, response.getHits(1));
    assertHit(7, response.getHits(2));
  }

  @Test
  public void testHitsThresholdDifferentSort() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setQuery(Query.newBuilder().build())
                    .setTopHits(3)
                    .setTotalHitsThreshold(5)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_field")
                    .setQuerySort(
                        QuerySortField.newBuilder()
                            .setFields(
                                SortFields.newBuilder()
                                    .addSortedFields(
                                        SortType.newBuilder()
                                            .setFieldName("int_field")
                                            .setReverse(false)
                                            .build())
                                    .build())
                            .build())
                    .build());

    assertEquals(3, response.getHitsCount());
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, response.getTotalHits().getRelation());
    assertHit(0, response.getHits(0));
    assertHit(1, response.getHits(1));
    assertHit(2, response.getHits(2));
  }

  @Test
  public void testHitsThresholdAdditionalCollectorSameSort() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setQuery(Query.newBuilder().build())
                    .setTopHits(3)
                    .setTotalHitsThreshold(5)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_field")
                    .setQuerySort(
                        QuerySortField.newBuilder()
                            .setFields(
                                SortFields.newBuilder()
                                    .addSortedFields(
                                        SortType.newBuilder()
                                            .setFieldName("int_field")
                                            .setReverse(true)
                                            .build())
                                    .build())
                            .build())
                    .putCollectors(
                        "test_collection",
                        Collector.newBuilder()
                            .setTopHitsCollector(
                                TopHitsCollector.newBuilder()
                                    .setTopHits(7)
                                    .addRetrieveFields("doc_id")
                                    .addRetrieveFields("int_field")
                                    .setQuerySort(
                                        QuerySortField.newBuilder()
                                            .setFields(
                                                SortFields.newBuilder()
                                                    .addSortedFields(
                                                        SortType.newBuilder()
                                                            .setFieldName("int_field")
                                                            .setReverse(true)
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build());

    assertEquals(3, response.getHitsCount());
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, response.getTotalHits().getRelation());
    assertHit(9, response.getHits(0));
    assertHit(8, response.getHits(1));
    assertHit(7, response.getHits(2));

    HitsResult hitsResult = response.getCollectorResultsOrThrow("test_collection").getHitsResult();
    assertEquals(7, hitsResult.getHitsCount());
    assertEquals(10, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertHit(9, hitsResult.getHits(0));
    assertHit(8, hitsResult.getHits(1));
    assertHit(7, hitsResult.getHits(2));
    assertHit(6, hitsResult.getHits(3));
    assertHit(5, hitsResult.getHits(4));
    assertHit(4, hitsResult.getHits(5));
    assertHit(3, hitsResult.getHits(6));
  }

  @Test
  public void testHitsThresholdAdditionalCollectorDifferentSort() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setQuery(Query.newBuilder().build())
                    .setTopHits(3)
                    .setTotalHitsThreshold(5)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_field")
                    .setQuerySort(
                        QuerySortField.newBuilder()
                            .setFields(
                                SortFields.newBuilder()
                                    .addSortedFields(
                                        SortType.newBuilder()
                                            .setFieldName("int_field")
                                            .setReverse(true)
                                            .build())
                                    .build())
                            .build())
                    .putCollectors(
                        "test_collection",
                        Collector.newBuilder()
                            .setTopHitsCollector(
                                TopHitsCollector.newBuilder()
                                    .setTopHits(7)
                                    .addRetrieveFields("doc_id")
                                    .addRetrieveFields("int_field")
                                    .setQuerySort(
                                        QuerySortField.newBuilder()
                                            .setFields(
                                                SortFields.newBuilder()
                                                    .addSortedFields(
                                                        SortType.newBuilder()
                                                            .setFieldName("int_field")
                                                            .setReverse(false)
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build());

    assertEquals(3, response.getHitsCount());
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, response.getTotalHits().getRelation());
    assertHit(9, response.getHits(0));
    assertHit(8, response.getHits(1));
    assertHit(7, response.getHits(2));

    HitsResult hitsResult = response.getCollectorResultsOrThrow("test_collection").getHitsResult();
    assertEquals(7, hitsResult.getHitsCount());
    assertEquals(10, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertHit(0, hitsResult.getHits(0));
    assertHit(1, hitsResult.getHits(1));
    assertHit(2, hitsResult.getHits(2));
    assertHit(3, hitsResult.getHits(3));
    assertHit(4, hitsResult.getHits(4));
    assertHit(5, hitsResult.getHits(5));
    assertHit(6, hitsResult.getHits(6));
  }

  private void assertScoreHit(int sortValue, String term, Hit hit) {
    assertEquals(
        String.valueOf(sortValue + 10),
        hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
    assertEquals(sortValue, hit.getFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
    assertEquals(term, hit.getFieldsOrThrow("text_field").getFieldValue(0).getTextValue());
  }

  private void assertHit(int sortValue, Hit hit) {
    assertEquals(
        String.valueOf(sortValue + 10),
        hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
    assertEquals(sortValue, hit.getFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
    assertEquals(sortValue, hit.getSortedFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
  }
}
