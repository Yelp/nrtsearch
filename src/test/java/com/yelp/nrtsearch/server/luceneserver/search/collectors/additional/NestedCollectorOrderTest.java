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
import com.yelp.nrtsearch.server.grpc.BucketResult;
import com.yelp.nrtsearch.server.grpc.BucketResult.Bucket;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.CommitRequest;
import com.yelp.nrtsearch.server.grpc.ExistsQuery;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.MaxCollector;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.script.js.JsScriptEngine;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.Test;

public class NestedCollectorOrderTest extends ServerTestCase {
  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/collection/nested_order.json");
  }

  protected void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests to ensure we have multiple index slices
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
                .putFields(
                    "value_field_2",
                    MultiValuedField.newBuilder().addValue(Integer.toString(-i * j)).build())
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
      getGrpcServer()
          .getBlockingStub()
          .commit(CommitRequest.newBuilder().setIndexName(DEFAULT_TEST_INDEX).build());
    }
  }

  @Test
  public void testNestedOrder() {
    Collector collector = getCollector(OrderType.DESC, 5);
    SearchResponse response = doQuery(collector);
    assertResponse(
        response, 5, 5, 0, List.of("1", "2", "3", "4", "5"), List.of(-1.0, -2.0, -3.0, -4.0, -5.0));

    collector = getCollector(OrderType.ASC, 5);
    response = doQuery(collector);
    assertResponse(
        response, 5, 5, 0, List.of("5", "4", "3", "2", "1"), List.of(-5.0, -4.0, -3.0, -2.0, -1.0));
  }

  @Test
  public void testNestedOrderSubset() {
    Collector collector = getCollector(OrderType.DESC, 2);
    SearchResponse response = doQuery(collector);
    assertResponse(response, 5, 2, 60, List.of("1", "2"), List.of(-1.0, -2.0));

    collector = getCollector(OrderType.ASC, 2);
    response = doQuery(collector);
    assertResponse(response, 5, 2, 60, List.of("5", "4"), List.of(-5.0, -4.0));
  }

  @Test
  public void testNestedOrderGreaterSize() {
    Collector collector = getCollector(OrderType.DESC, 10);
    SearchResponse response = doQuery(collector);
    assertResponse(
        response, 5, 5, 0, List.of("1", "2", "3", "4", "5"), List.of(-1.0, -2.0, -3.0, -4.0, -5.0));

    collector = getCollector(OrderType.ASC, 10);
    response = doQuery(collector);
    assertResponse(
        response, 5, 5, 0, List.of("5", "4", "3", "2", "1"), List.of(-5.0, -4.0, -3.0, -2.0, -1.0));
  }

  @Test
  public void testNestedOrderWithAdditionalCollector() {
    Collector additonalCollector =
        Collector.newBuilder()
            .setMax(
                MaxCollector.newBuilder()
                    .setScript(
                        Script.newBuilder()
                            .setLang(JsScriptEngine.LANG)
                            .setSource("value_field")
                            .build())
                    .build())
            .build();

    Collector collector = getCollector(OrderType.DESC, 5);
    collector = collector.toBuilder().putNestedCollectors("additional", additonalCollector).build();
    SearchResponse response = doQuery(collector);
    assertResponse(
        response, 5, 5, 0, List.of("1", "2", "3", "4", "5"), List.of(-1.0, -2.0, -3.0, -4.0, -5.0));
    assertAdditionalCollector(response, List.of(20.0, 40.0, 60.0, 80.0, 100.0));

    collector = getCollector(OrderType.ASC, 5);
    collector = collector.toBuilder().putNestedCollectors("additional", additonalCollector).build();
    response = doQuery(collector);
    assertResponse(
        response, 5, 5, 0, List.of("5", "4", "3", "2", "1"), List.of(-5.0, -4.0, -3.0, -2.0, -1.0));
    assertAdditionalCollector(response, List.of(100.0, 80.0, 60.0, 40.0, 20.0));
  }

  @Test
  public void testRangeNestedOrder() {
    Collector collector = getCollector(OrderType.DESC, 3);
    SearchResponse response = doRangeQuery(collector);
    assertResponse(response, 3, 3, 0, List.of("2", "3", "4"), List.of(-2.0, -3.0, -4.0));

    collector = getCollector(OrderType.ASC, 3);
    response = doRangeQuery(collector);
    assertResponse(response, 3, 3, 0, List.of("4", "3", "2"), List.of(-4.0, -3.0, -2.0));
  }

  @Test
  public void testRangeNestedOrderSubset() {
    Collector collector = getCollector(OrderType.DESC, 2);
    SearchResponse response = doRangeQuery(collector);
    assertResponse(response, 3, 2, 20, List.of("2", "3"), List.of(-2.0, -3.0));

    collector = getCollector(OrderType.ASC, 2);
    response = doRangeQuery(collector);
    assertResponse(response, 3, 2, 20, List.of("4", "3"), List.of(-4.0, -3.0));
  }

  @Test
  public void testRangeNestedOrderGreaterSize() {
    Collector collector = getCollector(OrderType.DESC, 10);
    SearchResponse response = doRangeQuery(collector);
    assertResponse(response, 3, 3, 0, List.of("2", "3", "4"), List.of(-2.0, -3.0, -4.0));

    collector = getCollector(OrderType.ASC, 10);
    response = doRangeQuery(collector);
    assertResponse(response, 3, 3, 0, List.of("4", "3", "2"), List.of(-4.0, -3.0, -2.0));
  }

  @Test
  public void testRangeNestedOrderWithAdditionalCollector() {
    Collector additonalCollector =
        Collector.newBuilder()
            .setMax(
                MaxCollector.newBuilder()
                    .setScript(
                        Script.newBuilder()
                            .setLang(JsScriptEngine.LANG)
                            .setSource("value_field")
                            .build())
                    .build())
            .build();

    Collector collector = getCollector(OrderType.DESC, 3);
    collector = collector.toBuilder().putNestedCollectors("additional", additonalCollector).build();
    SearchResponse response = doRangeQuery(collector);
    assertResponse(response, 3, 3, 0, List.of("2", "3", "4"), List.of(-2.0, -3.0, -4.0));
    assertAdditionalCollector(response, List.of(40.0, 60.0, 80.0));

    collector = getCollector(OrderType.ASC, 3);
    collector = collector.toBuilder().putNestedCollectors("additional", additonalCollector).build();
    response = doRangeQuery(collector);
    assertResponse(response, 3, 3, 0, List.of("4", "3", "2"), List.of(-4.0, -3.0, -2.0));
    assertAdditionalCollector(response, List.of(80.0, 60.0, 40.0));
  }

  @Test
  public void testOrderCollectorNotExist() {
    Collector collector =
        Collector.newBuilder()
            .setTerms(
                TermsCollector.newBuilder()
                    .setField("int_field")
                    .setSize(10)
                    .setOrder(
                        BucketOrder.newBuilder()
                            .setKey("nested_collector")
                            .setOrder(OrderType.ASC)
                            .build())
                    .build())
            .build();
    try {
      doQuery(collector);
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Nested collector not found: nested_collector"));
    }
  }

  private Collector getCollector(OrderType orderType, int size) {
    return Collector.newBuilder()
        .setTerms(
            TermsCollector.newBuilder()
                .setField("int_field")
                .setSize(size)
                .setOrder(
                    BucketOrder.newBuilder().setKey("nested_collector").setOrder(orderType).build())
                .build())
        .putNestedCollectors(
            "nested_collector",
            Collector.newBuilder()
                .setMax(
                    MaxCollector.newBuilder()
                        .setScript(
                            Script.newBuilder()
                                .setLang(JsScriptEngine.LANG)
                                .setSource("value_field_2")
                                .build())
                        .build())
                .build())
        .build();
  }

  private SearchResponse doQuery(Collector collector) {
    return doQuery(
        Query.newBuilder()
            .setExistsQuery(ExistsQuery.newBuilder().setField("doc_id").build())
            .build(),
        collector);
  }

  private SearchResponse doRangeQuery(Collector collector) {
    return doQuery(
        Query.newBuilder()
            .setRangeQuery(
                RangeQuery.newBuilder().setField("int_field").setLower("2").setUpper("4").build())
            .build(),
        collector);
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

  private void assertResponse(
      SearchResponse response,
      int totalBuckets,
      int bucketCount,
      int otherCount,
      List<String> keys,
      List<Double> nestedValues) {
    assertEquals(1, response.getCollectorResultsCount());
    BucketResult result = response.getCollectorResultsOrThrow("test_collector").getBucketResult();
    assertEquals(totalBuckets, result.getTotalBuckets());
    assertEquals(bucketCount, result.getBucketsCount());
    assertEquals(otherCount, result.getTotalOtherCounts());

    assertEquals(bucketCount, keys.size());
    assertEquals(bucketCount, nestedValues.size());

    for (int i = 0; i < bucketCount; ++i) {
      Bucket bucket = result.getBuckets(i);
      assertEquals(keys.get(i), bucket.getKey());
      assertEquals(20, bucket.getCount());
      assertEquals(
          nestedValues.get(i),
          bucket.getNestedCollectorResultsOrThrow("nested_collector").getDoubleResult().getValue(),
          0);
    }
  }

  private void assertAdditionalCollector(SearchResponse response, List<Double> values) {
    BucketResult result = response.getCollectorResultsOrThrow("test_collector").getBucketResult();
    assertEquals(result.getBucketsCount(), values.size());
    for (int i = 0; i < values.size(); ++i) {
      assertEquals(
          values.get(i),
          result
              .getBuckets(i)
              .getNestedCollectorResultsOrThrow("additional")
              .getDoubleResult()
              .getValue(),
          0);
    }
  }
}
