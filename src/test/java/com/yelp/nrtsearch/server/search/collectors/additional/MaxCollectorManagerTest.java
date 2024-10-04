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
package com.yelp.nrtsearch.server.search.collectors.additional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.grpc.BucketOrder;
import com.yelp.nrtsearch.server.grpc.BucketOrder.OrderType;
import com.yelp.nrtsearch.server.grpc.BucketResult.Bucket;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.MaxCollector;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.utils.MinMaxUtilTest;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class MaxCollectorManagerTest extends MinMaxUtilTest {
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
}
