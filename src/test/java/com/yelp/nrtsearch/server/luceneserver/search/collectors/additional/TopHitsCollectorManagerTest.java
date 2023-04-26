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
package com.yelp.nrtsearch.server.luceneserver.search.collectors.additional;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.CollectorResult.CollectorResultsCase;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.HitsResult;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.SortFields;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.grpc.TopHitsCollector;
import com.yelp.nrtsearch.server.grpc.TotalHits.Relation;
import com.yelp.nrtsearch.server.grpc.VirtualField;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.script.js.JsScriptEngine;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;
import org.junit.Test;

public class TopHitsCollectorManagerTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;
  private static final List<String> ALL_FIELDS =
      Arrays.asList("doc_id", "int_field", "int_field_2", "value");

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/collection/top_hits.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    // create a shuffled list of ids
    List<Integer> idList = new ArrayList<>();
    for (int i = 0; i < NUM_DOCS; ++i) {
      idList.add(i);
    }
    Collections.shuffle(idList);

    // add documents one chunk at a time to ensure multiple index segments
    List<AddDocumentRequest> requestChunk = new ArrayList<>();
    for (Integer id : idList) {
      requestChunk.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "int_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(NUM_DOCS - id))
                      .build())
              .putFields(
                  "int_field_2",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "value",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id + 2))
                      .build())
              .build());

      if (requestChunk.size() == SEGMENT_CHUNK) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }
  }

  @Test
  public void testTopHitsRelevance() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setQuery(
                Query.newBuilder()
                    .setFunctionScoreQuery(
                        FunctionScoreQuery.newBuilder()
                            .setScript(
                                Script.newBuilder()
                                    .setLang(JsScriptEngine.LANG)
                                    .setSource("int_field_2*3")
                                    .build())
                            .build())
                    .build())
            .putCollectors(
                "test_collector",
                Collector.newBuilder()
                    .setTopHitsCollector(
                        TopHitsCollector.newBuilder()
                            .setStartHit(0)
                            .setTopHits(5)
                            .addAllRetrieveFields(ALL_FIELDS)
                            .build())
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);

    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, response.getTotalHits().getRelation());
    assertEquals(0, response.getHitsCount());

    assertEquals(1, response.getCollectorResultsMap().size());
    CollectorResult collectorResult = response.getCollectorResultsOrThrow("test_collector");
    assertEquals(CollectorResultsCase.HITSRESULT, collectorResult.getCollectorResultsCase());

    HitsResult hitsResult = collectorResult.getHitsResult();
    assertEquals(100, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 99, 98, 97, 96, 95);
    assertScores(hitsResult, 297, 294, 291, 288, 285);
    // by default, no explain
    assertExplain(hitsResult, "", "", "", "", "");
  }

  @Test
  public void testTopHitsRelevanceWithExplain() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setQuery(
                Query.newBuilder()
                    .setFunctionScoreQuery(
                        FunctionScoreQuery.newBuilder()
                            .setScript(
                                Script.newBuilder()
                                    .setLang(JsScriptEngine.LANG)
                                    .setSource("int_field_2*3")
                                    .build())
                            .build())
                    .build())
            .putCollectors(
                "test_collector",
                Collector.newBuilder()
                    .setTopHitsCollector(
                        TopHitsCollector.newBuilder()
                            .setStartHit(0)
                            .setTopHits(5)
                            .addAllRetrieveFields(ALL_FIELDS)
                            .setExplain(true)
                            .build())
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);

    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, response.getTotalHits().getRelation());
    assertEquals(0, response.getHitsCount());

    assertEquals(1, response.getCollectorResultsMap().size());
    CollectorResult collectorResult = response.getCollectorResultsOrThrow("test_collector");
    assertEquals(CollectorResultsCase.HITSRESULT, collectorResult.getCollectorResultsCase());

    HitsResult hitsResult = collectorResult.getHitsResult();
    assertEquals(100, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 99, 98, 97, 96, 95);
    assertScores(hitsResult, 297, 294, 291, 288, 285);
    assertExplain(
        hitsResult,
        "297.0 = weight(FunctionScoreQuery(*:*, scored by expr(int_field_2*3))), result of:\n"
            + "  297.0 = int_field_2*3, computed from:\n"
            + "    99.0 = double(int_field_2)\n",
        "294.0 = weight(FunctionScoreQuery(*:*, scored by expr(int_field_2*3))), result of:\n"
            + "  294.0 = int_field_2*3, computed from:\n"
            + "    98.0 = double(int_field_2)\n",
        "291.0 = weight(FunctionScoreQuery(*:*, scored by expr(int_field_2*3))), result of:\n"
            + "  291.0 = int_field_2*3, computed from:\n"
            + "    97.0 = double(int_field_2)\n",
        "288.0 = weight(FunctionScoreQuery(*:*, scored by expr(int_field_2*3))), result of:\n"
            + "  288.0 = int_field_2*3, computed from:\n"
            + "    96.0 = double(int_field_2)\n",
        "285.0 = weight(FunctionScoreQuery(*:*, scored by expr(int_field_2*3))), result of:\n"
            + "  285.0 = int_field_2*3, computed from:\n"
            + "    95.0 = double(int_field_2)\n");
  }

  @Test
  public void testTopHitsStartHit() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setQuery(
                Query.newBuilder()
                    .setFunctionScoreQuery(
                        FunctionScoreQuery.newBuilder()
                            .setScript(
                                Script.newBuilder()
                                    .setLang(JsScriptEngine.LANG)
                                    .setSource("int_field_2*3")
                                    .build())
                            .build())
                    .build())
            .putCollectors(
                "test_collector",
                Collector.newBuilder()
                    .setTopHitsCollector(
                        TopHitsCollector.newBuilder()
                            .setStartHit(5)
                            .setTopHits(10)
                            .addAllRetrieveFields(ALL_FIELDS)
                            .build())
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);

    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, response.getTotalHits().getRelation());
    assertEquals(0, response.getHitsCount());

    assertEquals(1, response.getCollectorResultsMap().size());
    CollectorResult collectorResult = response.getCollectorResultsOrThrow("test_collector");
    assertEquals(CollectorResultsCase.HITSRESULT, collectorResult.getCollectorResultsCase());

    HitsResult hitsResult = collectorResult.getHitsResult();
    assertEquals(100, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 94, 93, 92, 91, 90);
    assertScores(hitsResult, 282, 279, 276, 273, 270);
  }

  @Test
  public void testTopHitsVirtualField() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setQuery(
                Query.newBuilder()
                    .setFunctionScoreQuery(
                        FunctionScoreQuery.newBuilder()
                            .setScript(
                                Script.newBuilder()
                                    .setLang(JsScriptEngine.LANG)
                                    .setSource("int_field_2*3")
                                    .build())
                            .build())
                    .build())
            .addVirtualFields(
                VirtualField.newBuilder()
                    .setName("virtual")
                    .setScript(
                        Script.newBuilder()
                            .setLang(JsScriptEngine.LANG)
                            .setSource("int_field*2")
                            .build())
                    .build())
            .putCollectors(
                "test_collector",
                Collector.newBuilder()
                    .setTopHitsCollector(
                        TopHitsCollector.newBuilder()
                            .setStartHit(0)
                            .setTopHits(3)
                            .addAllRetrieveFields(Arrays.asList("doc_id", "virtual"))
                            .build())
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    assertEquals(0, response.getHitsCount());

    HitsResult hitsResult = response.getCollectorResultsOrThrow("test_collector").getHitsResult();
    assertEquals(100, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(3, hitsResult.getHitsCount());
    assertEquals(
        "99", hitsResult.getHits(0).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
    assertEquals(
        2, hitsResult.getHits(0).getFieldsOrThrow("virtual").getFieldValue(0).getDoubleValue(), 0);
    assertEquals(
        "98", hitsResult.getHits(1).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
    assertEquals(
        4, hitsResult.getHits(1).getFieldsOrThrow("virtual").getFieldValue(0).getDoubleValue(), 0);
    assertEquals(
        "97", hitsResult.getHits(2).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
    assertEquals(
        6, hitsResult.getHits(2).getFieldsOrThrow("virtual").getFieldValue(0).getDoubleValue(), 0);
  }

  @Test
  public void testTopHitsVirtualFieldWithScore() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setQuery(
                Query.newBuilder()
                    .setFunctionScoreQuery(
                        FunctionScoreQuery.newBuilder()
                            .setScript(
                                Script.newBuilder()
                                    .setLang(JsScriptEngine.LANG)
                                    .setSource("int_field_2*3")
                                    .build())
                            .build())
                    .build())
            .addVirtualFields(
                VirtualField.newBuilder()
                    .setName("virtual")
                    .setScript(
                        Script.newBuilder()
                            .setLang(JsScriptEngine.LANG)
                            .setSource("_score*2")
                            .build())
                    .build())
            .putCollectors(
                "test_collector",
                Collector.newBuilder()
                    .setTopHitsCollector(
                        TopHitsCollector.newBuilder()
                            .setStartHit(0)
                            .setTopHits(3)
                            .addAllRetrieveFields(Arrays.asList("doc_id", "virtual"))
                            .build())
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    assertEquals(0, response.getHitsCount());

    HitsResult hitsResult = response.getCollectorResultsOrThrow("test_collector").getHitsResult();
    assertEquals(100, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(3, hitsResult.getHitsCount());
    assertEquals(
        "99", hitsResult.getHits(0).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
    assertEquals(
        594,
        hitsResult.getHits(0).getFieldsOrThrow("virtual").getFieldValue(0).getDoubleValue(),
        0);
    assertEquals(
        "98", hitsResult.getHits(1).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
    assertEquals(
        588,
        hitsResult.getHits(1).getFieldsOrThrow("virtual").getFieldValue(0).getDoubleValue(),
        0);
    assertEquals(
        "97", hitsResult.getHits(2).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
    assertEquals(
        582,
        hitsResult.getHits(2).getFieldsOrThrow("virtual").getFieldValue(0).getDoubleValue(),
        0);
  }

  @Test
  public void testTopHitsSort() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setQuery(Query.newBuilder().build())
            .putCollectors(
                "test_collector",
                Collector.newBuilder()
                    .setTopHitsCollector(
                        TopHitsCollector.newBuilder()
                            .setStartHit(0)
                            .setTopHits(5)
                            .addAllRetrieveFields(ALL_FIELDS)
                            .setQuerySort(
                                QuerySortField.newBuilder()
                                    .setFields(
                                        SortFields.newBuilder()
                                            .addSortedFields(
                                                SortType.newBuilder()
                                                    .setFieldName("int_field")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);

    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, response.getTotalHits().getRelation());
    assertEquals(0, response.getHitsCount());

    assertEquals(1, response.getCollectorResultsMap().size());
    CollectorResult collectorResult = response.getCollectorResultsOrThrow("test_collector");
    assertEquals(CollectorResultsCase.HITSRESULT, collectorResult.getCollectorResultsCase());

    HitsResult hitsResult = collectorResult.getHitsResult();
    assertEquals(100, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 99, 98, 97, 96, 95);
    assertSort(hitsResult, 1, 2, 3, 4, 5);
  }

  @Test
  public void testTopHitsSortVirtual() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setQuery(Query.newBuilder().build())
            .addVirtualFields(
                VirtualField.newBuilder()
                    .setName("virtual")
                    .setScript(
                        Script.newBuilder()
                            .setLang(JsScriptEngine.LANG)
                            .setSource("int_field*-1")
                            .build())
                    .build())
            .putCollectors(
                "test_collector",
                Collector.newBuilder()
                    .setTopHitsCollector(
                        TopHitsCollector.newBuilder()
                            .setStartHit(0)
                            .setTopHits(5)
                            .addAllRetrieveFields(ALL_FIELDS)
                            .setQuerySort(
                                QuerySortField.newBuilder()
                                    .setFields(
                                        SortFields.newBuilder()
                                            .addSortedFields(
                                                SortType.newBuilder()
                                                    .setFieldName("virtual")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);

    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, response.getTotalHits().getRelation());
    assertEquals(0, response.getHitsCount());

    assertEquals(1, response.getCollectorResultsMap().size());
    CollectorResult collectorResult = response.getCollectorResultsOrThrow("test_collector");
    assertEquals(CollectorResultsCase.HITSRESULT, collectorResult.getCollectorResultsCase());

    HitsResult hitsResult = collectorResult.getHitsResult();
    assertEquals(100, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 0, 1, 2, 3, 4);
    assertSortVirtual(hitsResult, -100, -99, -98, -97, -96);
  }

  private void assertHits(HitsResult hitsResult, int... ids) {
    assertEquals(hitsResult.getHitsCount(), ids.length);
    for (int i = 0; i < ids.length; ++i) {
      Hit hit = hitsResult.getHits(i);
      assertEquals(
          String.valueOf(ids[i]), hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
      assertEquals(
          NUM_DOCS - ids[i], hit.getFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
      assertEquals(ids[i], hit.getFieldsOrThrow("int_field_2").getFieldValue(0).getIntValue());
      assertEquals(ids[i] + 2, hit.getFieldsOrThrow("value").getFieldValue(0).getLongValue());
    }
  }

  private void assertScores(HitsResult hitsResult, double... scores) {
    assertEquals(hitsResult.getHitsCount(), scores.length);
    for (int i = 0; i < scores.length; ++i) {
      Hit hit = hitsResult.getHits(i);
      assertEquals(scores[i], hit.getScore(), 0);
    }
  }

  private void assertSort(HitsResult hitsResult, int... sort) {
    assertEquals(hitsResult.getHitsCount(), sort.length);
    for (int i = 0; i < sort.length; ++i) {
      Hit hit = hitsResult.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(sort[i], hit.getSortedFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
    }
  }

  private void assertSortVirtual(HitsResult hitsResult, double... sort) {
    assertEquals(hitsResult.getHitsCount(), sort.length);
    for (int i = 0; i < sort.length; ++i) {
      Hit hit = hitsResult.getHits(i);
      assertEquals(1, hit.getSortedFieldsCount());
      assertEquals(
          sort[i], hit.getSortedFieldsOrThrow("virtual").getFieldValue(0).getDoubleValue(), 0);
    }
  }

  private void assertExplain(HitsResult hitsResult, String... explains) {
    assertEquals(hitsResult.getHitsCount(), explains.length);
    for (int i = 0; i < explains.length; ++i) {
      assertEquals(hitsResult.getHits(i).getExplain(), explains[i]);
    }
  }
}
