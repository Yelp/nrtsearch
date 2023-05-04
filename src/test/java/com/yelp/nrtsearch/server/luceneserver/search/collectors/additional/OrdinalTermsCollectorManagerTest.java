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

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.BucketOrder;
import com.yelp.nrtsearch.server.grpc.BucketOrder.OrderType;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

public class OrdinalTermsCollectorManagerTest extends TermsCollectorManagerTestsBase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  static final ExpectedValues[] ORDINAL_EXPECTED_MULTI_ORDER_DESC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("-1"), 45),
        new ExpectedValues(Set.of("0"), 22),
        new ExpectedValues(Set.of("2"), 18),
        new ExpectedValues(Set.of("1"), 15),
        new ExpectedValues(Set.of("9"), 10),
        new ExpectedValues(Set.of("8"), 9),
        new ExpectedValues(Set.of("7"), 8),
        new ExpectedValues(Set.of("6"), 7),
        new ExpectedValues(Set.of("5"), 6),
        new ExpectedValues(Set.of("4"), 5),
        new ExpectedValues(Set.of("3"), 4)
      };
  static final ExpectedValues[] ORDINAL_EXPECTED_MULTI_ORDER_ASC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("3"), 4),
        new ExpectedValues(Set.of("4"), 5),
        new ExpectedValues(Set.of("5"), 6),
        new ExpectedValues(Set.of("6"), 7),
        new ExpectedValues(Set.of("7"), 8),
        new ExpectedValues(Set.of("8"), 9),
        new ExpectedValues(Set.of("9"), 10),
        new ExpectedValues(Set.of("1"), 15),
        new ExpectedValues(Set.of("2"), 18),
        new ExpectedValues(Set.of("0"), 22),
        new ExpectedValues(Set.of("-1"), 45)
      };
  static final ExpectedValues[] ORDINAL_EXPECTED_SUBSET_MULTI_ORDER_DESC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("-1"), 45), new ExpectedValues(Set.of("0"), 22)
      };
  static final ExpectedValues[] ORDINAL_EXPECTED_SUBSET_MULTI_ORDER_ASC =
      new ExpectedValues[] {new ExpectedValues(Set.of("3"), 4), new ExpectedValues(Set.of("4"), 5)};
  static final ExpectedValues[] ORDINAL_EXPECTED_RANGE_MULTI_ORDER_DESC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("-1"), 44),
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
  static final ExpectedValues[] ORDINAL_EXPECTED_RANGE_MULTI_ORDER_ASC =
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
        new ExpectedValues(Set.of("-1"), 44)
      };
  static final ExpectedValues[] ORDINAL_EXPECTED_SUBSET_RANGE_MULTI_ORDER_DESC =
      new ExpectedValues[] {
        new ExpectedValues(Set.of("-1"), 44), new ExpectedValues(Set.of("0"), 15)
      };
  static final ExpectedValues[] ORDINAL_EXPECTED_SUBSET_RANGE_MULTI_ORDER_ASC =
      new ExpectedValues[] {new ExpectedValues(Set.of("3"), 2), new ExpectedValues(Set.of("4"), 3)};

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/collection/terms_ordinal.json");
  }

  @Override
  protected AddDocumentRequest getIndexRequest(String index, int id) {
    int valueOrder = id / 10 + id % 10;
    valueOrder = valueOrder < 10 ? valueOrder : -1;
    AddDocumentRequest.Builder builder =
        AddDocumentRequest.newBuilder()
            .setIndexName(index)
            .putFields(
                "doc_id",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(String.valueOf(id))
                    .build())
            .putFields(
                "int_field",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(String.valueOf(id))
                    .build())
            .putFields(
                "value",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(String.valueOf(id % 3))
                    .build())
            .putFields(
                "value_order",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(String.valueOf(valueOrder))
                    .build())
            .putFields(
                "value_multi",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(String.valueOf(id % 2))
                    .addValue(String.valueOf(id % 5))
                    .build())
            .putFields(
                "value_multi_order",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(String.valueOf(valueOrder))
                    .addValue(String.valueOf(valueOrder % 3))
                    .build());

    if ((id % 2) == 0) {
      builder.putFields(
          "sparse",
          AddDocumentRequest.MultiValuedField.newBuilder()
              .addValue(String.valueOf(id / 10))
              .build());
    }

    return builder.build();
  }

  @Test
  public void testTermsCollection() {
    TermsCollector terms = TermsCollector.newBuilder().setField(VALUE_FIELD).setSize(3).build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response,
        3,
        3,
        0,
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 34),
        new ExpectedValues(new HashSet<>(Arrays.asList("1", "2")), 33));
  }

  @Test
  public void testTermsCollection_order() {
    testTermsCollectionOrder();
  }

  @Test
  public void testTermsCollectionSubset() {
    TermsCollector terms = TermsCollector.newBuilder().setField(VALUE_FIELD).setSize(1).build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response, 3, 1, 66, new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 34));
  }

  @Test
  public void testTermsCollectionSubset_order() {
    testTermsCollectionSubsetOrder();
  }

  @Test
  public void testTermsCollectionGreaterSize() {
    TermsCollector terms = TermsCollector.newBuilder().setField(VALUE_FIELD).setSize(10).build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response,
        3,
        3,
        0,
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 34),
        new ExpectedValues(new HashSet<>(Arrays.asList("1", "2")), 33));
  }

  @Test
  public void testTermsCollectionGreaterSize_order() {
    testTermsCollectionGreaterSizeOrder();
  }

  @Test
  public void testTermsMultiCollection() {
    TermsCollector terms =
        TermsCollector.newBuilder().setField(VALUE_MULTI_FIELD).setSize(5).build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response,
        5,
        5,
        0,
        new ExpectedValues(new HashSet<>(Arrays.asList("0", "1")), 60),
        new ExpectedValues(new HashSet<>(Arrays.asList("2", "3", "4")), 20));
  }

  @Test
  public void testTermsMultiCollection_order() {
    testTermsMultiCollectionOrder(
        ORDINAL_EXPECTED_MULTI_ORDER_DESC, ORDINAL_EXPECTED_MULTI_ORDER_ASC);
  }

  @Test
  public void testTermsMultiCollectionSubset() {
    TermsCollector terms =
        TermsCollector.newBuilder().setField(VALUE_MULTI_FIELD).setSize(2).build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response, 5, 2, 60, new ExpectedValues(new HashSet<>(Arrays.asList("0", "1")), 60));
  }

  @Test
  public void testTermsMultiCollectionSubset_order() {
    testTermsMultiCollectionSubsetOrder(
        ORDINAL_EXPECTED_SUBSET_MULTI_ORDER_DESC, 82, ORDINAL_EXPECTED_SUBSET_MULTI_ORDER_ASC, 140);
  }

  @Test
  public void testTermsMultiCollectionGreaterSize() {
    TermsCollector terms =
        TermsCollector.newBuilder().setField(VALUE_MULTI_FIELD).setSize(10).build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response,
        5,
        5,
        0,
        new ExpectedValues(new HashSet<>(Arrays.asList("0", "1")), 60),
        new ExpectedValues(new HashSet<>(Arrays.asList("2", "3", "4")), 20));
  }

  @Test
  public void testTermsMultiCollectionGreaterSize_order() {
    testTermsMultiCollectionGreaterSizeOrder(
        ORDINAL_EXPECTED_MULTI_ORDER_DESC, ORDINAL_EXPECTED_MULTI_ORDER_ASC);
  }

  @Test
  public void testTermsRange() {
    TermsCollector terms = TermsCollector.newBuilder().setField(VALUE_FIELD).setSize(3).build();
    SearchResponse response = doRangeQuery(terms);
    assertResponse(
        response,
        3,
        3,
        0,
        new ExpectedValues(new HashSet<>(Collections.singletonList("2")), 4),
        new ExpectedValues(new HashSet<>(Arrays.asList("0", "1")), 3));
  }

  @Test
  public void testTermsRange_order() {
    testTermsRangeOrder();
  }

  @Test
  public void testTermsRangeSubset() {
    TermsCollector terms = TermsCollector.newBuilder().setField(VALUE_FIELD).setSize(1).build();
    SearchResponse response = doRangeQuery(terms);
    assertResponse(
        response, 3, 1, 6, new ExpectedValues(new HashSet<>(Collections.singletonList("2")), 4));
  }

  @Test
  public void testTermsRangeSubset_order() {
    testTermsRangeSubsetOrder();
  }

  @Test
  public void testTermsMultiRange() {
    TermsCollector terms =
        TermsCollector.newBuilder().setField(VALUE_MULTI_FIELD).setSize(5).build();
    SearchResponse response = doRangeQuery(terms);
    assertResponse(
        response,
        5,
        5,
        0,
        new ExpectedValues(new HashSet<>(Arrays.asList("0", "1")), 6),
        new ExpectedValues(new HashSet<>(Arrays.asList("2", "3", "4")), 2));
  }

  @Test
  public void testTermsMultiRange_order() {
    testTermsMultiRangeOrder(
        ORDINAL_EXPECTED_RANGE_MULTI_ORDER_DESC, ORDINAL_EXPECTED_RANGE_MULTI_ORDER_ASC);
  }

  @Test
  public void testTermsMultiRangeSubset() {
    TermsCollector terms =
        TermsCollector.newBuilder().setField(VALUE_MULTI_FIELD).setSize(2).build();
    SearchResponse response = doRangeQuery(terms);
    assertResponse(
        response, 5, 2, 6, new ExpectedValues(new HashSet<>(Arrays.asList("0", "1")), 6));
  }

  @Test
  public void testTermsMultiRangeSubset_order() {
    testTermsMultiRangeSubsetOrder(
        ORDINAL_EXPECTED_SUBSET_RANGE_MULTI_ORDER_DESC,
        55,
        ORDINAL_EXPECTED_SUBSET_RANGE_MULTI_ORDER_ASC,
        109);
  }

  @Test
  public void testEmptyField() {
    TermsCollector terms = TermsCollector.newBuilder().setField("empty").setSize(3).build();
    SearchResponse response = doQuery(terms);
    assertResponse(response, 0, 0, 0);
  }

  @Test
  public void testSparseTerms() {
    TermsCollector terms = TermsCollector.newBuilder().setField("sparse").setSize(3).build();
    SearchResponse response = doRangeQuery(terms);
    assertResponse(
        response,
        2,
        2,
        0,
        new ExpectedValues(new HashSet<>(Collections.singletonList("4")), 4),
        new ExpectedValues(new HashSet<>(Collections.singletonList("5")), 1));
  }

  @Test
  public void testSparseTerms_asc() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setField("sparse")
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.ASC).build())
            .setSize(3)
            .build();
    SearchResponse response = doRangeQuery(terms);
    assertResponse(
        response,
        2,
        2,
        0,
        new ExpectedValues(new HashSet<>(Collections.singletonList("5")), 1),
        new ExpectedValues(new HashSet<>(Collections.singletonList("4")), 4));
  }

  @Test
  public void testNestedCollector() {
    TermsCollector terms = TermsCollector.newBuilder().setField(VALUE_FIELD).setSize(3).build();
    SearchResponse response = doNestedQuery(terms);
    assertNestedResult(response);
  }

  @Test
  public void testNestedCollector_asc() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setField(VALUE_FIELD)
            .setOrder(BucketOrder.newBuilder().setKey(COUNT).setOrder(OrderType.ASC).build())
            .setSize(3)
            .build();
    SearchResponse response = doNestedQuery(terms);
    assertNestedResult(response);
  }

  @Test
  public void testOrderByNestedCollector() {
    super.testOrderByNestedCollector();
  }
}
