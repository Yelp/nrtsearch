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
package com.yelp.nrtsearch.server.luceneserver.search.collectors.additional;

import static com.yelp.nrtsearch.server.collectors.BucketOrder.COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.BucketOrder;
import com.yelp.nrtsearch.server.grpc.BucketOrder.OrderType;
import com.yelp.nrtsearch.server.grpc.BucketResult;
import com.yelp.nrtsearch.server.grpc.BucketResult.Bucket;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.HitsResult;
import com.yelp.nrtsearch.server.grpc.MaxCollector;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.grpc.TopHitsCollector;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.script.js.JsScriptEngine;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;

public abstract class TermsCollectorManagerTestsBase extends ServerTestCase {
  static final int NUM_DOCS = 100;
  static final int SEGMENT_CHUNK = 10;
  static final String VALUE_FIELD = "value";
  static final String VALUE_ORDER_FIELD = "value_order";
  static final String VALUE_MULTI_FIELD = "value_multi";
  static final String VALUE_MULTI_ORDER_FIELD = "value_multi_order";

  protected abstract AddDocumentRequest getIndexRequest(String index, int id);

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    // add documents one chunk at a time to ensure multiple index segments
    List<AddDocumentRequest> requestChunk = new ArrayList<>();
    for (int id = 0; id < NUM_DOCS; ++id) {
      requestChunk.add(getIndexRequest(name, id));

      if (requestChunk.size() == SEGMENT_CHUNK) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }
  }

  static class ExpectedValues {
    public Set<String> labels;
    public double count;

    public ExpectedValues(Set<String> labels, double count) {
      this.labels = labels;
      this.count = count;
    }
  }

  static class ExpectedWithNestedValue {
    public String label;
    public double count;
    public double nested;

    public ExpectedWithNestedValue(String label, double count, double nested) {
      this.label = label;
      this.count = count;
      this.nested = nested;
    }
  }

  static final ExpectedValues[] EXPECTED_ORDER_DESC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("-1"), 45),
        new ExpectedValues(Set.of("9"), 10),
        new ExpectedValues(Set.of("8"), 9),
        new ExpectedValues(Set.of("7"), 8),
        new ExpectedValues(Set.of("6"), 7),
        new ExpectedValues(Set.of("5"), 6),
        new ExpectedValues(Set.of("4"), 5),
        new ExpectedValues(Set.of("3"), 4),
        new ExpectedValues(Set.of("2"), 3),
        new ExpectedValues(Set.of("1"), 2),
        new ExpectedValues(Set.of("0"), 1)
      };
  static final ExpectedValues[] EXPECTED_ORDER_ASC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("0"), 1),
        new ExpectedValues(Set.of("1"), 2),
        new ExpectedValues(Set.of("2"), 3),
        new ExpectedValues(Set.of("3"), 4),
        new ExpectedValues(Set.of("4"), 5),
        new ExpectedValues(Set.of("5"), 6),
        new ExpectedValues(Set.of("6"), 7),
        new ExpectedValues(Set.of("7"), 8),
        new ExpectedValues(Set.of("8"), 9),
        new ExpectedValues(Set.of("9"), 10),
        new ExpectedValues(Set.of("-1"), 45)
      };
  static final ExpectedValues[] EXPECTED_SUBSET_ORDER_DESC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("-1"), 45), new ExpectedValues(Set.of("9"), 10)
      };
  static final ExpectedValues[] EXPECTED_SUBSET_ORDER_ASC =
      new ExpectedValues[] {new ExpectedValues(Set.of("0"), 1), new ExpectedValues(Set.of("1"), 2)};
  static final ExpectedValues[] EXPECTED_MULTI_ORDER_DESC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("-1"), 90),
        new ExpectedValues(Set.of("0"), 23),
        new ExpectedValues(Set.of("2"), 21),
        new ExpectedValues(Set.of("1"), 17),
        new ExpectedValues(Set.of("9"), 10),
        new ExpectedValues(Set.of("8"), 9),
        new ExpectedValues(Set.of("7"), 8),
        new ExpectedValues(Set.of("6"), 7),
        new ExpectedValues(Set.of("5"), 6),
        new ExpectedValues(Set.of("4"), 5),
        new ExpectedValues(Set.of("3"), 4)
      };
  static final ExpectedValues[] EXPECTED_MULTI_ORDER_ASC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("3"), 4),
        new ExpectedValues(Set.of("4"), 5),
        new ExpectedValues(Set.of("5"), 6),
        new ExpectedValues(Set.of("6"), 7),
        new ExpectedValues(Set.of("7"), 8),
        new ExpectedValues(Set.of("8"), 9),
        new ExpectedValues(Set.of("9"), 10),
        new ExpectedValues(Set.of("1"), 17),
        new ExpectedValues(Set.of("2"), 21),
        new ExpectedValues(Set.of("0"), 23),
        new ExpectedValues(Set.of("-1"), 90)
      };
  static final ExpectedValues[] EXPECTED_SUBSET_MULTI_ORDER_DESC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("-1"), 90), new ExpectedValues(Set.of("0"), 23)
      };
  static final ExpectedValues[] EXPECTED_SUBSET_MULTI_ORDER_ASC =
      new ExpectedValues[] {new ExpectedValues(Set.of("3"), 4), new ExpectedValues(Set.of("4"), 5)};
  static final ExpectedValues[] EXPECTED_RANGE_ORDER_DESC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("-1"), 44),
        new ExpectedValues(Set.of("9"), 8),
        new ExpectedValues(Set.of("8"), 7),
        new ExpectedValues(Set.of("7"), 6),
        new ExpectedValues(Set.of("6"), 5),
        new ExpectedValues(Set.of("5"), 4),
        new ExpectedValues(Set.of("4"), 3),
        new ExpectedValues(Set.of("3"), 2)
      };
  static final ExpectedValues[] EXPECTED_RANGE_ORDER_ASC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("3"), 2),
        new ExpectedValues(Set.of("4"), 3),
        new ExpectedValues(Set.of("5"), 4),
        new ExpectedValues(Set.of("6"), 5),
        new ExpectedValues(Set.of("7"), 6),
        new ExpectedValues(Set.of("8"), 7),
        new ExpectedValues(Set.of("9"), 8),
        new ExpectedValues(Set.of("-1"), 44)
      };
  static final ExpectedValues[] EXPECTED_RANGE_SUBSET_ORDER_DESC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("-1"), 44), new ExpectedValues(Set.of("9"), 8)
      };
  static final ExpectedValues[] EXPECTED_RANGE_SUBSET_ORDER_ASC =
      new ExpectedValues[] {new ExpectedValues(Set.of("3"), 2), new ExpectedValues(Set.of("4"), 3)};
  static final ExpectedValues[] EXPECTED_RANGE_MULTI_ORDER_DESC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("-1"), 88),
        new ExpectedValues(Set.of("0"), 15),
        new ExpectedValues(Set.of("2"), 11),
        new ExpectedValues(Set.of("1"), 9),
        new ExpectedValues(Set.of("9"), 8),
        new ExpectedValues(Set.of("8"), 7),
        new ExpectedValues(Set.of("7"), 6),
        new ExpectedValues(Set.of("6"), 5),
        new ExpectedValues(Set.of("5"), 4),
        new ExpectedValues(Set.of("4"), 3),
        new ExpectedValues(Set.of("3"), 2)
      };
  static final ExpectedValues[] EXPECTED_RANGE_MULTI_ORDER_ASC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("3"), 2),
        new ExpectedValues(Set.of("4"), 3),
        new ExpectedValues(Set.of("5"), 4),
        new ExpectedValues(Set.of("6"), 5),
        new ExpectedValues(Set.of("7"), 6),
        new ExpectedValues(Set.of("8"), 7),
        new ExpectedValues(Set.of("9"), 8),
        new ExpectedValues(Set.of("1"), 9),
        new ExpectedValues(Set.of("2"), 11),
        new ExpectedValues(Set.of("0"), 15),
        new ExpectedValues(Set.of("-1"), 88)
      };
  static final ExpectedValues[] EXPECTED_SUBSET_RANGE_MULTI_ORDER_DESC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("-1"), 88), new ExpectedValues(Set.of("0"), 15)
      };
  static final ExpectedValues[] EXPECTED_SUBSET_RANGE_MULTI_ORDER_ASC =
      new ExpectedValues[] {new ExpectedValues(Set.of("3"), 2), new ExpectedValues(Set.of("4"), 3)};

  static final ExpectedWithNestedValue[] EXPECTED_WITH_NESTED_DESC =
      new ExpectedWithNestedValue[] {
        new ExpectedWithNestedValue("9", 8, 90),
        new ExpectedWithNestedValue("-1", 35, 89),
        new ExpectedWithNestedValue("8", 7, 80),
        new ExpectedWithNestedValue("7", 6, 70),
        new ExpectedWithNestedValue("6", 5, 60),
        new ExpectedWithNestedValue("5", 4, 50),
        new ExpectedWithNestedValue("4", 3, 40),
        new ExpectedWithNestedValue("3", 2, 30)
      };
  static final ExpectedWithNestedValue[] EXPECTED_WITH_NESTED_ASC =
      new ExpectedWithNestedValue[] {
        new ExpectedWithNestedValue("3", 2, 30),
        new ExpectedWithNestedValue("4", 3, 40),
        new ExpectedWithNestedValue("5", 4, 50),
        new ExpectedWithNestedValue("6", 5, 60),
        new ExpectedWithNestedValue("7", 6, 70),
        new ExpectedWithNestedValue("8", 7, 80),
        new ExpectedWithNestedValue("-1", 35, 89),
        new ExpectedWithNestedValue("9", 8, 90)
      };

  void testTermsCollectionOrder() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setField(VALUE_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.DESC).build())
            .setSize(11)
            .build();
    SearchResponse response = doQuery(terms);
    assertOrderedResponse(response, 11, 11, 0, EXPECTED_ORDER_DESC);

    terms =
        TermsCollector.newBuilder()
            .setField(VALUE_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.ASC).build())
            .setSize(11)
            .build();
    response = doQuery(terms);
    assertOrderedResponse(response, 11, 11, 0, EXPECTED_ORDER_ASC);
  }

  void testTermsCollectionSubsetOrder() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setField(VALUE_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.DESC).build())
            .setSize(2)
            .build();
    SearchResponse response = doQuery(terms);
    assertOrderedResponse(response, 11, 2, 45, EXPECTED_SUBSET_ORDER_DESC);

    terms =
        TermsCollector.newBuilder()
            .setField(VALUE_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.ASC).build())
            .setSize(2)
            .build();
    response = doQuery(terms);
    assertOrderedResponse(response, 11, 2, 97, EXPECTED_SUBSET_ORDER_ASC);
  }

  void testTermsCollectionGreaterSizeOrder() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setField(VALUE_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.DESC).build())
            .setSize(20)
            .build();
    SearchResponse response = doQuery(terms);
    assertOrderedResponse(response, 11, 11, 0, EXPECTED_ORDER_DESC);

    terms =
        TermsCollector.newBuilder()
            .setField(VALUE_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.ASC).build())
            .setSize(20)
            .build();
    response = doQuery(terms);
    assertOrderedResponse(response, 11, 11, 0, EXPECTED_ORDER_ASC);
  }

  void testTermsMultiCollectionOrder() {
    testTermsMultiCollectionOrder(EXPECTED_MULTI_ORDER_DESC, EXPECTED_MULTI_ORDER_ASC);
  }

  void testTermsMultiCollectionOrder(
      ExpectedValues[] expectedValuesDesc, ExpectedValues[] expectedValuesAsc) {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setField(VALUE_MULTI_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.DESC).build())
            .setSize(11)
            .build();
    SearchResponse response = doQuery(terms);
    assertOrderedResponse(response, 11, 11, 0, expectedValuesDesc);

    terms =
        TermsCollector.newBuilder()
            .setField(VALUE_MULTI_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.ASC).build())
            .setSize(11)
            .build();
    response = doQuery(terms);
    assertOrderedResponse(response, 11, 11, 0, expectedValuesAsc);
  }

  void testTermsMultiCollectionSubsetOrder() {
    testTermsMultiCollectionSubsetOrder(
        EXPECTED_SUBSET_MULTI_ORDER_DESC, 87, EXPECTED_SUBSET_MULTI_ORDER_ASC, 191);
  }

  void testTermsMultiCollectionSubsetOrder(
      ExpectedValues[] expectedValuesDesc,
      int otherCountDesc,
      ExpectedValues[] expectedValuesAsc,
      int otherCountAsc) {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setField(VALUE_MULTI_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.DESC).build())
            .setSize(2)
            .build();
    SearchResponse response = doQuery(terms);
    assertOrderedResponse(response, 11, 2, otherCountDesc, expectedValuesDesc);

    terms =
        TermsCollector.newBuilder()
            .setField(VALUE_MULTI_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.ASC).build())
            .setSize(2)
            .build();
    response = doQuery(terms);
    assertOrderedResponse(response, 11, 2, otherCountAsc, expectedValuesAsc);
  }

  void testTermsMultiCollectionGreaterSizeOrder() {
    testTermsMultiCollectionGreaterSizeOrder(EXPECTED_MULTI_ORDER_DESC, EXPECTED_MULTI_ORDER_ASC);
  }

  void testTermsMultiCollectionGreaterSizeOrder(
      ExpectedValues[] expectedValuesDesc, ExpectedValues[] expectedValuesAsc) {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setField(VALUE_MULTI_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.DESC).build())
            .setSize(20)
            .build();
    SearchResponse response = doQuery(terms);
    assertOrderedResponse(response, 11, 11, 0, expectedValuesDesc);

    terms =
        TermsCollector.newBuilder()
            .setField(VALUE_MULTI_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.ASC).build())
            .setSize(20)
            .build();
    response = doQuery(terms);
    assertOrderedResponse(response, 11, 11, 0, expectedValuesAsc);
  }

  void testTermsRangeOrder() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setField(VALUE_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.DESC).build())
            .setSize(8)
            .build();
    SearchResponse response = doOrderRangeQuery(terms);
    assertOrderedResponse(response, 8, 8, 0, EXPECTED_RANGE_ORDER_DESC);

    terms =
        TermsCollector.newBuilder()
            .setField(VALUE_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.ASC).build())
            .setSize(8)
            .build();
    response = doOrderRangeQuery(terms);
    assertOrderedResponse(response, 8, 8, 0, EXPECTED_RANGE_ORDER_ASC);
  }

  void testTermsRangeSubsetOrder() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setField(VALUE_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.DESC).build())
            .setSize(2)
            .build();
    SearchResponse response = doOrderRangeQuery(terms);
    assertOrderedResponse(response, 8, 2, 27, EXPECTED_RANGE_SUBSET_ORDER_DESC);

    terms =
        TermsCollector.newBuilder()
            .setField(VALUE_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.ASC).build())
            .setSize(2)
            .build();
    response = doOrderRangeQuery(terms);
    assertOrderedResponse(response, 8, 2, 74, EXPECTED_RANGE_SUBSET_ORDER_ASC);
  }

  void testTermsMultiRangeOrder() {
    testTermsMultiRangeOrder(EXPECTED_RANGE_MULTI_ORDER_DESC, EXPECTED_RANGE_MULTI_ORDER_ASC);
  }

  void testTermsMultiRangeOrder(
      ExpectedValues[] expectedValuesDesc, ExpectedValues[] expectedValuesAsc) {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setField(VALUE_MULTI_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.DESC).build())
            .setSize(11)
            .build();
    SearchResponse response = doOrderRangeQuery(terms);
    assertOrderedResponse(response, 11, 11, 0, expectedValuesDesc);

    terms =
        TermsCollector.newBuilder()
            .setField(VALUE_MULTI_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.ASC).build())
            .setSize(11)
            .build();
    response = doOrderRangeQuery(terms);
    assertOrderedResponse(response, 11, 11, 0, expectedValuesAsc);
  }

  void testTermsMultiRangeSubsetOrder() {
    testTermsMultiRangeSubsetOrder(
        EXPECTED_SUBSET_RANGE_MULTI_ORDER_DESC, 55, EXPECTED_SUBSET_RANGE_MULTI_ORDER_ASC, 153);
  }

  void testTermsMultiRangeSubsetOrder(
      ExpectedValues[] expectedValuesDesc,
      int otherCountDesc,
      ExpectedValues[] expectedValuesAsc,
      int otherCountAsc) {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setField(VALUE_MULTI_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.DESC).build())
            .setSize(2)
            .build();
    SearchResponse response = doOrderRangeQuery(terms);
    assertOrderedResponse(response, 11, 2, otherCountDesc, expectedValuesDesc);

    terms =
        TermsCollector.newBuilder()
            .setField(VALUE_MULTI_ORDER_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.ASC).build())
            .setSize(2)
            .build();
    response = doOrderRangeQuery(terms);
    assertOrderedResponse(response, 11, 2, otherCountAsc, expectedValuesAsc);
  }

  void testOrderByNestedCollector() {
    TermsCollector terms =
        TermsCollector.newBuilder().setField(VALUE_ORDER_FIELD).setSize(11).build();
    testOrderByNestedCollector(terms);
  }

  void testOrderByNestedCollector(TermsCollector terms) {
    terms =
        terms
            .toBuilder()
            .setOrder(BucketOrder.newBuilder().setKey("nested").setOrder(OrderType.DESC).build())
            .build();
    SearchResponse response = doNestedOrderQuery(terms);
    assertNestedOrderResult(response, 8, 8, 0, EXPECTED_WITH_NESTED_DESC);

    terms =
        terms
            .toBuilder()
            .setOrder(BucketOrder.newBuilder().setKey("nested").setOrder(OrderType.ASC).build())
            .build();
    response = doNestedOrderQuery(terms);
    assertNestedOrderResult(response, 8, 8, 0, EXPECTED_WITH_NESTED_ASC);
  }

  void assertResponse(
      SearchResponse response,
      int totalBuckets,
      int bucketCount,
      int otherCount,
      ExpectedValues... expectedValues) {
    assertEquals(1, response.getCollectorResultsCount());
    BucketResult result = response.getCollectorResultsOrThrow("test_collector").getBucketResult();
    assertEquals(totalBuckets, result.getTotalBuckets());
    assertEquals(bucketCount, result.getBucketsCount());
    assertEquals(otherCount, result.getTotalOtherCounts());

    int sum = 0;
    for (ExpectedValues v : expectedValues) {
      sum += v.labels.size();
    }
    assertEquals(sum, bucketCount);

    int valuesIndex = 0;
    for (ExpectedValues v : expectedValues) {
      Set<String> valueSet = new HashSet<>();
      for (int i = 0; i < v.labels.size(); ++i) {
        valueSet.add(result.getBuckets(valuesIndex).getKey());
        assertEquals(v.count, result.getBuckets(valuesIndex).getCount(), 0);
        valuesIndex++;
      }
      assertEquals(v.labels, valueSet);
    }
  }

  void assertOrderedResponse(
      SearchResponse response,
      int totalBuckets,
      int bucketCount,
      int otherCount,
      ExpectedValues... expectedValues) {
    assertEquals(1, response.getCollectorResultsCount());
    BucketResult result = response.getCollectorResultsOrThrow("test_collector").getBucketResult();
    assertEquals(totalBuckets, result.getTotalBuckets());
    assertEquals(bucketCount, result.getBucketsCount());
    assertEquals(otherCount, result.getTotalOtherCounts());

    int sum = 0;
    for (ExpectedValues v : expectedValues) {
      sum += v.labels.size();
    }
    assertEquals(sum, bucketCount);

    assertEquals(expectedValues.length, result.getBucketsCount());
    for (int i = 0; i < expectedValues.length; ++i) {
      assertEquals(extractLabel(expectedValues[i].labels), result.getBuckets(i).getKey());
      assertEquals(expectedValues[i].count, result.getBuckets(i).getCount(), 0);
    }
  }

  private String extractLabel(Set<String> labelSet) {
    assertEquals(1, labelSet.size());
    for (String label : labelSet) {
      return label;
    }
    throw new IllegalArgumentException("Invalid label set");
  }

  SearchResponse doQuery(TermsCollector terms) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .putCollectors("test_collector", Collector.newBuilder().setTerms(terms).build())
                .build());
  }

  SearchResponse doRangeQuery(TermsCollector terms) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .setQuery(
                    Query.newBuilder()
                        .setRangeQuery(
                            RangeQuery.newBuilder()
                                .setField("int_field")
                                .setLower("41")
                                .setUpper("50")
                                .build())
                        .build())
                .putCollectors("test_collector", Collector.newBuilder().setTerms(terms).build())
                .build());
  }

  SearchResponse doOrderRangeQuery(TermsCollector terms) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .setQuery(
                    Query.newBuilder()
                        .setRangeQuery(
                            RangeQuery.newBuilder()
                                .setField("int_field")
                                .setLower("21")
                                .setUpper("100")
                                .build())
                        .build())
                .putCollectors("test_collector", Collector.newBuilder().setTerms(terms).build())
                .build());
  }

  SearchResponse doNestedQuery(TermsCollector terms) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .putCollectors(
                    "test_collector",
                    Collector.newBuilder()
                        .setTerms(terms)
                        .putNestedCollectors(
                            "nested",
                            Collector.newBuilder()
                                .setTopHitsCollector(
                                    TopHitsCollector.newBuilder()
                                        .setStartHit(0)
                                        .setTopHits(5)
                                        .addRetrieveFields("doc_id")
                                        .build())
                                .build())
                        .build())
                .build());
  }

  void assertNestedResult(SearchResponse response) {
    assertEquals(1, response.getCollectorResultsCount());
    BucketResult result = response.getCollectorResultsOrThrow("test_collector").getBucketResult();
    assertEquals(3, result.getTotalBuckets());
    assertEquals(3, result.getBucketsCount());

    for (Bucket bucket : result.getBucketsList()) {
      assertEquals(1, bucket.getNestedCollectorResultsCount());
      HitsResult hitsResult = bucket.getNestedCollectorResultsOrThrow("nested").getHitsResult();
      assertTrue(hitsResult.getTotalHits().getValue() > 0);
      assertTrue(hitsResult.getHitsCount() > 0);
      for (Hit hit : hitsResult.getHitsList()) {
        assertTrue(hit.containsFields("doc_id"));
      }
    }
  }

  SearchResponse doNestedOrderQuery(TermsCollector terms) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .setQuery(
                    Query.newBuilder()
                        .setRangeQuery(
                            RangeQuery.newBuilder()
                                .setField("int_field")
                                .setLower("21")
                                .setUpper("90")
                                .build())
                        .build())
                .putCollectors(
                    "test_collector",
                    Collector.newBuilder()
                        .setTerms(terms)
                        .putNestedCollectors(
                            "nested",
                            Collector.newBuilder()
                                .setMax(
                                    MaxCollector.newBuilder()
                                        .setScript(
                                            Script.newBuilder()
                                                .setLang(JsScriptEngine.LANG)
                                                .setSource("doc_id.int")
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build());
  }

  void assertNestedOrderResult(
      SearchResponse response,
      int totalBuckets,
      int bucketCount,
      int otherCount,
      ExpectedWithNestedValue... expectedWithNestedValues) {
    assertEquals(1, response.getCollectorResultsCount());
    BucketResult result = response.getCollectorResultsOrThrow("test_collector").getBucketResult();
    assertEquals(totalBuckets, result.getTotalBuckets());
    assertEquals(bucketCount, result.getBucketsCount());
    assertEquals(otherCount, result.getTotalOtherCounts());

    assertEquals(expectedWithNestedValues.length, bucketCount);

    for (int i = 0; i < expectedWithNestedValues.length; ++i) {
      assertEquals(expectedWithNestedValues[i].label, result.getBuckets(i).getKey());
      assertEquals(expectedWithNestedValues[i].count, result.getBuckets(i).getCount(), 0);
      assertEquals(
          expectedWithNestedValues[i].nested,
          result
              .getBuckets(i)
              .getNestedCollectorResultsOrThrow("nested")
              .getDoubleResult()
              .getValue(),
          0);
    }
  }
}
