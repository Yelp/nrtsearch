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

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.junit.ClassRule;
import org.junit.Test;

public class DoubleTermsCollectorManagerTest extends TermsCollectorManagerTestsBase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/collection/terms_double.json");
  }

  @Override
  protected AddDocumentRequest getIndexRequest(String index, int id) {
    return AddDocumentRequest.newBuilder()
        .setIndexName(index)
        .putFields(
            "doc_id",
            AddDocumentRequest.MultiValuedField.newBuilder().addValue(String.valueOf(id)).build())
        .putFields(
            "int_field",
            AddDocumentRequest.MultiValuedField.newBuilder().addValue(String.valueOf(id)).build())
        .putFields(
            "value",
            AddDocumentRequest.MultiValuedField.newBuilder()
                .addValue(String.valueOf((id % 3) * 1.25))
                .build())
        .putFields(
            "value_multi",
            AddDocumentRequest.MultiValuedField.newBuilder()
                .addValue(String.valueOf((id % 2) * 1.5))
                .addValue(String.valueOf((id % 5) * 1.5))
                .build())
        .build();
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
        new ExpectedValues(new HashSet<>(Collections.singletonList("0.0")), 34),
        new ExpectedValues(new HashSet<>(Arrays.asList("1.25", "2.5")), 33));
  }

  @Test
  public void testTermsCollectionSubset() {
    TermsCollector terms = TermsCollector.newBuilder().setField(VALUE_FIELD).setSize(1).build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response,
        3,
        1,
        66,
        new ExpectedValues(new HashSet<>(Collections.singletonList("0.0")), 34));
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
        new ExpectedValues(new HashSet<>(Collections.singletonList("0.0")), 34),
        new ExpectedValues(new HashSet<>(Arrays.asList("1.25", "2.5")), 33));
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
        new ExpectedValues(new HashSet<>(Arrays.asList("0.0", "1.5")), 70),
        new ExpectedValues(new HashSet<>(Arrays.asList("3.0", "4.5", "6.0")), 20));
  }

  @Test
  public void testTermsMultiCollectionSubset() {
    TermsCollector terms =
        TermsCollector.newBuilder().setField(VALUE_MULTI_FIELD).setSize(2).build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response, 5, 2, 60, new ExpectedValues(new HashSet<>(Arrays.asList("0.0", "1.5")), 70));
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
        new ExpectedValues(new HashSet<>(Arrays.asList("0.0", "1.5")), 70),
        new ExpectedValues(new HashSet<>(Arrays.asList("3.0", "4.5", "6.0")), 20));
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
        new ExpectedValues(new HashSet<>(Collections.singletonList("2.5")), 4),
        new ExpectedValues(new HashSet<>(Arrays.asList("0.0", "1.25")), 3));
  }

  @Test
  public void testTermsRangeSubset() {
    TermsCollector terms = TermsCollector.newBuilder().setField(VALUE_FIELD).setSize(1).build();
    SearchResponse response = doRangeQuery(terms);
    assertResponse(
        response, 3, 1, 6, new ExpectedValues(new HashSet<>(Collections.singletonList("2.5")), 4));
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
        new ExpectedValues(new HashSet<>(Arrays.asList("0.0", "1.5")), 7),
        new ExpectedValues(new HashSet<>(Arrays.asList("3.0", "4.5", "6.0")), 2));
  }

  @Test
  public void testTermsMultiRangeSubset() {
    TermsCollector terms =
        TermsCollector.newBuilder().setField(VALUE_MULTI_FIELD).setSize(2).build();
    SearchResponse response = doRangeQuery(terms);
    assertResponse(
        response, 5, 2, 6, new ExpectedValues(new HashSet<>(Arrays.asList("0.0", "1.5")), 7));
  }
}
