/*
 * Copyright 2023 Yelp Inc.
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.Int32Value;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.BucketOrder;
import com.yelp.nrtsearch.server.grpc.BucketOrder.OrderType;
import com.yelp.nrtsearch.server.grpc.BucketResult.Bucket;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.ExistsQuery;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.MaxCollector;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.script.js.JsScriptEngine;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;
import org.junit.Test;

public class MaxCollectorManagerTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private static final Script FIELD_SCRIPT =
      Script.newBuilder().setLang(JsScriptEngine.LANG).setSource("value_field").build();
  private static final Script SCORE_SCRIPT =
      Script.newBuilder().setLang(JsScriptEngine.LANG).setSource("_score").build();
  private static final Query MATCH_ALL_SCORE_QUERY =
      Query.newBuilder()
          .setFunctionScoreQuery(
              FunctionScoreQuery.newBuilder()
                  .setQuery(Query.newBuilder().build())
                  .setScript(FIELD_SCRIPT))
          .build();
  private static final Query MATCH_SOME_SCORE_QUERY =
      Query.newBuilder()
          .setFunctionScoreQuery(
              FunctionScoreQuery.newBuilder()
                  .setQuery(
                      Query.newBuilder()
                          .setTermQuery(
                              TermQuery.newBuilder().setField("int_field").setIntValue(3).build())
                          .build())
                  .setScript(FIELD_SCRIPT))
          .build();
  private static final Query MATCH_ALL_QUERY =
      Query.newBuilder()
          .setExistsQuery(ExistsQuery.newBuilder().setField("value_field").build())
          .build();
  private static final Query MATCH_SOME_QUERY =
      Query.newBuilder()
          .setTermQuery(TermQuery.newBuilder().setField("int_field").setIntValue(3).build())
          .build();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/collection/max.json");
  }

  protected void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    getGlobalState()
        .getIndexStateManager(DEFAULT_TEST_INDEX)
        .updateLiveSettings(
            IndexLiveSettings.newBuilder().setSliceMaxSegments(Int32Value.of(1)).build());

    List<AddDocumentRequest> docs = new ArrayList<>();
    int doc_id = 0;
    for (int i = 1; i <= 5; ++i) {
      for (int j = 1; j <= 20; ++j) {
        AddDocumentRequest request =
            AddDocumentRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .putFields(
                    "doc_id",
                    MultiValuedField.newBuilder().addValue(Integer.toString(doc_id)).build())
                .putFields(
                    "int_field",
                    MultiValuedField.newBuilder().addValue(Integer.toString(i)).build())
                .putFields(
                    "value_field",
                    MultiValuedField.newBuilder().addValue(Integer.toString(i * j)).build())
                .build();
        docs.add(request);
        doc_id++;
      }
    }

    // write docs in random order with 10 per segment
    Collections.shuffle(docs);
    for (int i = 0; i < 10; ++i) {
      List<AddDocumentRequest> chunk = new ArrayList<>();
      for (int j = 0; j < 10; ++j) {
        chunk.add(docs.get((10 * i) + j));
      }
      addDocuments(chunk.stream());
      writer.commit();
    }
  }

  @Test
  public void testMaxCollectorAllDocs() {
    maxCollectorAllDocs(MATCH_ALL_QUERY, FIELD_SCRIPT);
  }

  @Test
  public void testMaxCollectorAllDocs_score() {
    maxCollectorAllDocs(MATCH_ALL_SCORE_QUERY, SCORE_SCRIPT);
  }

  private void maxCollectorAllDocs(Query query, Script script) {
    SearchResponse response =
        doQuery(
            query,
            Collector.newBuilder()
                .setMax(MaxCollector.newBuilder().setScript(script).build())
                .build());
    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(
        100.0,
        response.getCollectorResultsOrThrow("test_collector").getDoubleResult().getValue(),
        0);
  }

  @Test
  public void testMaxCollectorSomeDocs() {
    maxCollectorSomeDocs(MATCH_SOME_QUERY, FIELD_SCRIPT);
  }

  @Test
  public void testMaxCollectorSomeDocs_score() {
    maxCollectorSomeDocs(MATCH_SOME_SCORE_QUERY, SCORE_SCRIPT);
  }

  private void maxCollectorSomeDocs(Query query, Script script) {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .setQuery(query)
                    .putCollectors(
                        "test_collector",
                        Collector.newBuilder()
                            .setMax(MaxCollector.newBuilder().setScript(script).build())
                            .build())
                    .build());
    assertEquals(20, response.getTotalHits().getValue());
    assertEquals(
        60.0,
        response.getCollectorResultsOrThrow("test_collector").getDoubleResult().getValue(),
        0);
  }

  @Test
  public void testNestedMaxCollector() {
    nestedMaxCollector(MATCH_ALL_QUERY, FIELD_SCRIPT);
  }

  @Test
  public void testNestedMaxCollector_score() {
    nestedMaxCollector(MATCH_ALL_SCORE_QUERY, SCORE_SCRIPT);
  }

  private void nestedMaxCollector(Query query, Script script) {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .setQuery(query)
                    .putCollectors(
                        "test_collector",
                        Collector.newBuilder()
                            .setTerms(
                                TermsCollector.newBuilder()
                                    .setField("int_field")
                                    .setSize(10)
                                    .build())
                            .putNestedCollectors(
                                "nested_collector",
                                Collector.newBuilder()
                                    .setMax(MaxCollector.newBuilder().setScript(script).build())
                                    .build())
                            .build())
                    .build());
    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(
        5,
        response.getCollectorResultsOrThrow("test_collector").getBucketResult().getBucketsCount());

    Map<String, Double> bucketValues = new HashMap<>();
    for (Bucket bucket :
        response.getCollectorResultsOrThrow("test_collector").getBucketResult().getBucketsList()) {
      bucketValues.put(
          bucket.getKey(),
          bucket.getNestedCollectorResultsOrThrow("nested_collector").getDoubleResult().getValue());
      assertEquals(20, bucket.getCount());
    }
    assertEquals(Map.of("1", 20.0, "2", 40.0, "3", 60.0, "4", 80.0, "5", 100.0), bucketValues);
  }

  @Test
  public void testNestedOrderMaxCollector() {
    nestedOrderMaxCollector(MATCH_ALL_QUERY, FIELD_SCRIPT);
  }

  @Test
  public void testNestedOrderMaxCollector_score() {
    nestedOrderMaxCollector(MATCH_ALL_SCORE_QUERY, SCORE_SCRIPT);
  }

  private void nestedOrderMaxCollector(Query query, Script script) {
    SearchResponse response = doNestedOrderQuery(OrderType.DESC, query, script);
    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(
        5,
        response.getCollectorResultsOrThrow("test_collector").getBucketResult().getBucketsCount());

    List<String> keyOrder = new ArrayList<>();
    List<Double> sortValues = new ArrayList<>();
    for (Bucket bucket :
        response.getCollectorResultsOrThrow("test_collector").getBucketResult().getBucketsList()) {
      keyOrder.add(bucket.getKey());
      sortValues.add(
          bucket.getNestedCollectorResultsOrThrow("nested_collector").getDoubleResult().getValue());
      assertEquals(20, bucket.getCount());
    }
    assertEquals(List.of("5", "4", "3", "2", "1"), keyOrder);
    assertEquals(List.of(100.0, 80.0, 60.0, 40.0, 20.0), sortValues);

    response = doNestedOrderQuery(OrderType.ASC, query, script);
    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(
        5,
        response.getCollectorResultsOrThrow("test_collector").getBucketResult().getBucketsCount());

    keyOrder = new ArrayList<>();
    sortValues = new ArrayList<>();
    for (Bucket bucket :
        response.getCollectorResultsOrThrow("test_collector").getBucketResult().getBucketsList()) {
      keyOrder.add(bucket.getKey());
      sortValues.add(
          bucket.getNestedCollectorResultsOrThrow("nested_collector").getDoubleResult().getValue());
      assertEquals(20, bucket.getCount());
    }
    assertEquals(List.of("1", "2", "3", "4", "5"), keyOrder);
    assertEquals(List.of(20.0, 40.0, 60.0, 80.0, 100.0), sortValues);
  }

  private SearchResponse doNestedOrderQuery(OrderType orderType, Query query, Script script) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setTopHits(10)
                .setQuery(query)
                .putCollectors(
                    "test_collector",
                    Collector.newBuilder()
                        .setTerms(
                            TermsCollector.newBuilder()
                                .setField("int_field")
                                .setSize(10)
                                .setOrder(
                                    BucketOrder.newBuilder()
                                        .setKey("nested_collector")
                                        .setOrder(orderType)
                                        .build())
                                .build())
                        .putNestedCollectors(
                            "nested_collector",
                            Collector.newBuilder()
                                .setMax(MaxCollector.newBuilder().setScript(script).build())
                                .build())
                        .build())
                .build());
  }

  @Test
  public void testNoHits() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setTermQuery(TermQuery.newBuilder().setField("int_field").setIntValue(10).build())
                .build(),
            Collector.newBuilder()
                .setMax(MaxCollector.newBuilder().setScript(FIELD_SCRIPT).build())
                .build());
    assertEquals(0, response.getTotalHits().getValue());
    assertEquals(
        MaxCollectorManager.UNSET_VALUE,
        response.getCollectorResultsOrThrow("test_collector").getDoubleResult().getValue(),
        0);
  }

  @Test
  public void testNoValueSource() {
    try {
      doQuery(
          MATCH_ALL_QUERY,
          Collector.newBuilder().setMax(MaxCollector.newBuilder().build()).build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Unknown value source: VALUESOURCE_NOT_SET"));
    }
  }

  @Test
  public void testWithNestedCollector() {
    try {
      doQuery(
          MATCH_ALL_QUERY,
          Collector.newBuilder()
              .setMax(MaxCollector.newBuilder().setScript(FIELD_SCRIPT).build())
              .putNestedCollectors(
                  "nested_collector",
                  Collector.newBuilder()
                      .setMax(MaxCollector.newBuilder().setScript(FIELD_SCRIPT).build())
                      .build())
              .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("MaxCollector cannot have nested collectors"));
    }
  }

  private SearchResponse doQuery(Query query, Collector collector) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setTopHits(10)
                .setQuery(query)
                .putCollectors("test_collector", collector)
                .build());
  }
}
