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
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.grpc.VirtualField;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.junit.ClassRule;
import org.junit.Test;

public class VirtualTermsCollectorManagerTest extends TermsCollectorManagerTestsBase {
  private static final String VIRTUAL_FIELD = "index_virtual";
  private static final String VIRTUAL_SCORE_FIELD = "index_virtual_score";

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/collection/terms_virtual.json");
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
                .addValue(String.valueOf(id % 3))
                .build())
        .build();
  }

  @Test
  public void testTermsCollection() {
    TermsCollector terms = TermsCollector.newBuilder().setField(VIRTUAL_FIELD).setSize(3).build();
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
    TermsCollector terms = TermsCollector.newBuilder().setField(VIRTUAL_FIELD).setSize(1).build();
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
    TermsCollector terms = TermsCollector.newBuilder().setField(VIRTUAL_FIELD).setSize(10).build();
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
  public void testTermsRange() {
    TermsCollector terms = TermsCollector.newBuilder().setField(VIRTUAL_FIELD).setSize(3).build();
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
    TermsCollector terms = TermsCollector.newBuilder().setField(VIRTUAL_FIELD).setSize(1).build();
    SearchResponse response = doRangeQuery(terms);
    assertResponse(
        response, 3, 1, 6, new ExpectedValues(new HashSet<>(Collections.singletonList("2.5")), 4));
  }

  @Test
  public void testQueryVirtualField() {
    TermsCollector terms =
        TermsCollector.newBuilder().setField("query_virtual_field").setSize(10).build();
    SearchResponse response =
        getGrpcServer()
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
                    .addVirtualFields(
                        VirtualField.newBuilder()
                            .setName("query_virtual_field")
                            .setScript(
                                Script.newBuilder().setLang("js").setSource("value * 2.5").build())
                            .build())
                    .putCollectors("test_collector", Collector.newBuilder().setTerms(terms).build())
                    .build());
    assertResponse(
        response,
        3,
        3,
        0,
        new ExpectedValues(new HashSet<>(Collections.singletonList("5.0")), 4),
        new ExpectedValues(new HashSet<>(Arrays.asList("0.0", "2.5")), 3));
  }

  @Test
  public void testScriptScore() {
    TermsCollector terms =
        TermsCollector.newBuilder().setField(VIRTUAL_SCORE_FIELD).setSize(10).build();
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .setQuery(
                        Query.newBuilder()
                            .setFunctionScoreQuery(
                                FunctionScoreQuery.newBuilder()
                                    .setQuery(
                                        Query.newBuilder()
                                            .setRangeQuery(
                                                RangeQuery.newBuilder()
                                                    .setField("int_field")
                                                    .setLower("41")
                                                    .setUpper("45")
                                                    .build())
                                            .build())
                                    .setScript(
                                        Script.newBuilder()
                                            .setLang("js")
                                            .setSource("int_field")
                                            .build())
                                    .build())
                            .build())
                    .putCollectors("test_collector", Collector.newBuilder().setTerms(terms).build())
                    .build());
    assertResponse(
        response,
        5,
        5,
        0,
        new ExpectedValues(
            new HashSet<>(Arrays.asList("51.25", "52.5", "53.75", "55.0", "56.25")), 1));
  }

  @Test
  public void testNestedCollector() {
    TermsCollector terms = TermsCollector.newBuilder().setField(VALUE_FIELD).setSize(3).build();
    SearchResponse response = doNestedQuery(terms);
    assertNestedResult(response);
  }
}
