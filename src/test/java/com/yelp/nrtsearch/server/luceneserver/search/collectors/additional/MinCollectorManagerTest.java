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

import static org.junit.Assert.*;

import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.utils.MinMaxUtilTest;
import io.grpc.StatusRuntimeException;
import java.util.*;
import org.junit.Test;

public class MinCollectorManagerTest extends MinMaxUtilTest {
  @Test
  public void testMinCollectorAllDocs() {
    minCollectorAllDocs(MATCH_ALL_QUERY, FIELD_SCRIPT);
  }

  @Test
  public void testMinCollectorAllDocs_score() {
    minCollectorAllDocs(MATCH_ALL_SCORE_QUERY, SCORE_SCRIPT);
  }

  private void minCollectorAllDocs(Query query, Script script) {
    SearchResponse response =
        doQuery(
            query,
            Collector.newBuilder()
                .setMin(MinCollector.newBuilder().setScript(script).build())
                .build());
    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(
        1.0, response.getCollectorResultsOrThrow("test_collector").getDoubleResult().getValue(), 0);
  }

  @Test
  public void testMinCollectorSomeDocs() {
    minCollectorSomeDocs(MATCH_SOME_QUERY, FIELD_SCRIPT);
  }

  @Test
  public void testMinCollectorSomeDocs_score() {
    minCollectorSomeDocs(MATCH_SOME_SCORE_QUERY, SCORE_SCRIPT);
  }

  private void minCollectorSomeDocs(Query query, Script script) {
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
                            .setMin(MinCollector.newBuilder().setScript(script).build())
                            .build())
                    .build());
    assertEquals(20, response.getTotalHits().getValue());
    assertEquals(
        3.0, response.getCollectorResultsOrThrow("test_collector").getDoubleResult().getValue(), 0);
  }

  @Test
  public void testNestedMinCollector() {
    nestedMinCollector(MATCH_ALL_QUERY, FIELD_SCRIPT);
  }

  @Test
  public void testNestedMinCollector_score() {
    nestedMinCollector(MATCH_ALL_SCORE_QUERY, SCORE_SCRIPT);
  }

  private void nestedMinCollector(Query query, Script script) {
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
                                    .setMin(MinCollector.newBuilder().setScript(script).build())
                                    .build())
                            .build())
                    .build());
    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(
        5,
        response.getCollectorResultsOrThrow("test_collector").getBucketResult().getBucketsCount());

    Map<String, Double> bucketValues = new HashMap<>();
    for (BucketResult.Bucket bucket :
        response.getCollectorResultsOrThrow("test_collector").getBucketResult().getBucketsList()) {
      bucketValues.put(
          bucket.getKey(),
          bucket.getNestedCollectorResultsOrThrow("nested_collector").getDoubleResult().getValue());
      assertEquals(20, bucket.getCount());
    }
    assertEquals(Map.of("1", 1.0, "2", 2.0, "3", 3.0, "4", 4.0, "5", 5.0), bucketValues);
  }

  @Test
  public void testNestedOrderMinCollector() {
    nestedOrderMinCollector(MATCH_ALL_QUERY, FIELD_SCRIPT);
  }

  @Test
  public void testNestedOrderMinCollector_score() {
    nestedOrderMinCollector(MATCH_ALL_SCORE_QUERY, SCORE_SCRIPT);
  }

  private void nestedOrderMinCollector(Query query, Script script) {
    SearchResponse response = doNestedOrderQuery(BucketOrder.OrderType.DESC, query, script);
    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(
        5,
        response.getCollectorResultsOrThrow("test_collector").getBucketResult().getBucketsCount());

    List<String> keyOrder = new ArrayList<>();
    List<Double> sortValues = new ArrayList<>();
    for (BucketResult.Bucket bucket :
        response.getCollectorResultsOrThrow("test_collector").getBucketResult().getBucketsList()) {
      keyOrder.add(bucket.getKey());
      sortValues.add(
          bucket.getNestedCollectorResultsOrThrow("nested_collector").getDoubleResult().getValue());
      assertEquals(20, bucket.getCount());
    }
    assertEquals(List.of("5", "4", "3", "2", "1"), keyOrder);
    assertEquals(List.of(5.0, 4.0, 3.0, 2.0, 1.0), sortValues);

    response = doNestedOrderQuery(BucketOrder.OrderType.ASC, query, script);
    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(
        5,
        response.getCollectorResultsOrThrow("test_collector").getBucketResult().getBucketsCount());

    keyOrder = new ArrayList<>();
    sortValues = new ArrayList<>();
    for (BucketResult.Bucket bucket :
        response.getCollectorResultsOrThrow("test_collector").getBucketResult().getBucketsList()) {
      keyOrder.add(bucket.getKey());
      sortValues.add(
          bucket.getNestedCollectorResultsOrThrow("nested_collector").getDoubleResult().getValue());
      assertEquals(20, bucket.getCount());
    }
    assertEquals(List.of("1", "2", "3", "4", "5"), keyOrder);
    assertEquals(List.of(1.0, 2.0, 3.0, 4.0, 5.0), sortValues);
  }

  private SearchResponse doNestedOrderQuery(
      BucketOrder.OrderType orderType, Query query, Script script) {
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
                                .setMin(MinCollector.newBuilder().setScript(script).build())
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
                .setMin(MinCollector.newBuilder().setScript(FIELD_SCRIPT).build())
                .build());
    assertEquals(0, response.getTotalHits().getValue());
    assertEquals(
        MinCollectorManager.UNSET_VALUE,
        response.getCollectorResultsOrThrow("test_collector").getDoubleResult().getValue(),
        0);
  }

  @Test
  public void testNoValueSource() {
    try {
      doQuery(
          MATCH_ALL_QUERY,
          Collector.newBuilder().setMin(MinCollector.newBuilder().build()).build());
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
              .setMin(MinCollector.newBuilder().setScript(FIELD_SCRIPT).build())
              .putNestedCollectors(
                  "nested_collector",
                  Collector.newBuilder()
                      .setMin(MinCollector.newBuilder().setScript(FIELD_SCRIPT).build())
                      .build())
              .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("MinCollector cannot have nested collectors"));
    }
  }
}
