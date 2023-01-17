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
import com.yelp.nrtsearch.server.grpc.BucketResult;
import com.yelp.nrtsearch.server.grpc.BucketResult.Bucket;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.CollectorResult.CollectorResultsCase;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FilterCollector;
import com.yelp.nrtsearch.server.grpc.FilterResult;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.HitsResult;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery.DoubleTerms;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery.FloatTerms;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery.IntTerms;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery.LongTerms;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery.TextTerms;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.grpc.TopHitsCollector;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.script.js.JsScriptEngine;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.Test;

public class FilterCollectorManagerTest extends ServerTestCase {
  private static final List<String> VALUE_FIELDS =
      List.of("int_value", "long_value", "float_value", "double_value", "text_value");

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/collection/filter.json");
  }

  @Override
  protected String getExtraConfig() {
    return "stateConfig:\n  backendType: LOCAL";
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

    int docId = 0;
    List<String> values = List.of("0", "1", "2", "3", "4", "5", "6");
    for (String value : values) {
      Map<String, MultiValuedField> valueFieldData =
          VALUE_FIELDS.stream()
              .collect(
                  Collectors.toMap(
                      f -> f, f -> MultiValuedField.newBuilder().addValue(value).build()));
      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id", MultiValuedField.newBuilder().addValue(Integer.toString(docId)).build())
              .putFields("query_field", MultiValuedField.newBuilder().addValue("1").build())
              .putAllFields(valueFieldData)
              .build();
      docs.add(request);
      docId++;
    }

    values = List.of("1", "2");
    for (String value : values) {
      Map<String, MultiValuedField> valueFieldData =
          VALUE_FIELDS.stream()
              .collect(
                  Collectors.toMap(
                      f -> f, f -> MultiValuedField.newBuilder().addValue(value).build()));
      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id", MultiValuedField.newBuilder().addValue(Integer.toString(docId)).build())
              .putFields("query_field", MultiValuedField.newBuilder().addValue("2").build())
              .putAllFields(valueFieldData)
              .build();
      docs.add(request);
      docId++;
    }

    // ensure multiple segments
    writer.commit();

    List<List<String>> multiValues = List.of(List.of("0", "2"), List.of("3", "5"), List.of("4"));
    for (List<String> vals : multiValues) {
      MultiValuedField.Builder mvfBuilder = MultiValuedField.newBuilder();
      for (String value : vals) {
        mvfBuilder.addValue(value);
      }
      Map<String, MultiValuedField> valueFieldData =
          VALUE_FIELDS.stream().collect(Collectors.toMap(f -> f, f -> mvfBuilder.build()));

      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id", MultiValuedField.newBuilder().addValue(Integer.toString(docId)).build())
              .putFields("query_field", MultiValuedField.newBuilder().addValue("3").build())
              .putAllFields(valueFieldData)
              .build();
      docs.add(request);
      docId++;
    }

    values = List.of("4", "5");
    for (String value : values) {
      Map<String, MultiValuedField> valueFieldData =
          VALUE_FIELDS.stream()
              .collect(
                  Collectors.toMap(
                      f -> f, f -> MultiValuedField.newBuilder().addValue(value).build()));
      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id", MultiValuedField.newBuilder().addValue(Integer.toString(docId)).build())
              .putFields("query_field", MultiValuedField.newBuilder().addValue("4").build())
              .putAllFields(valueFieldData)
              .build();
      docs.add(request);
      docId++;
    }
    addDocuments(docs.stream());
  }

  @Test
  public void testIntFilterSet() {
    queryAndVerify(
        TermInSetQuery.newBuilder()
            .setField("int_value")
            .setIntTerms(IntTerms.newBuilder().addTerms(2).addTerms(3).build())
            .build(),
        true);
  }

  @Test
  public void testIntFilterQuery() {
    queryAndVerify(
        TermInSetQuery.newBuilder()
            .setField("int_value")
            .setIntTerms(IntTerms.newBuilder().addTerms(2).addTerms(3).build())
            .build(),
        false);
  }

  @Test
  public void testLongFilterSet() {
    queryAndVerify(
        TermInSetQuery.newBuilder()
            .setField("long_value")
            .setLongTerms(LongTerms.newBuilder().addTerms(2).addTerms(3).build())
            .build(),
        true);
  }

  @Test
  public void testLongFilterQuery() {
    queryAndVerify(
        TermInSetQuery.newBuilder()
            .setField("long_value")
            .setLongTerms(LongTerms.newBuilder().addTerms(2).addTerms(3).build())
            .build(),
        false);
  }

  @Test
  public void testFloatFilterSet() {
    queryAndVerify(
        TermInSetQuery.newBuilder()
            .setField("float_value")
            .setFloatTerms(FloatTerms.newBuilder().addTerms(2).addTerms(3).build())
            .build(),
        true);
  }

  @Test
  public void testFloatFilterQuery() {
    queryAndVerify(
        TermInSetQuery.newBuilder()
            .setField("float_value")
            .setFloatTerms(FloatTerms.newBuilder().addTerms(2).addTerms(3).build())
            .build(),
        false);
  }

  @Test
  public void testDoubleFilterSet() {
    queryAndVerify(
        TermInSetQuery.newBuilder()
            .setField("double_value")
            .setDoubleTerms(DoubleTerms.newBuilder().addTerms(2).addTerms(3).build())
            .build(),
        true);
  }

  @Test
  public void testDoubleFilterQuery() {
    queryAndVerify(
        TermInSetQuery.newBuilder()
            .setField("double_value")
            .setDoubleTerms(DoubleTerms.newBuilder().addTerms(2).addTerms(3).build())
            .build(),
        false);
  }

  @Test
  public void testTextFilterSet() {
    queryAndVerify(
        TermInSetQuery.newBuilder()
            .setField("text_value")
            .setTextTerms(TextTerms.newBuilder().addTerms("2").addTerms("3").build())
            .build(),
        true);
  }

  @Test
  public void testTextFilterQuery() {
    queryAndVerify(
        TermInSetQuery.newBuilder()
            .setField("text_value")
            .setTextTerms(TextTerms.newBuilder().addTerms("2").addTerms("3").build())
            .build(),
        false);
  }

  @Test
  public void testNoNestedCollector() {
    try {
      doQuery(
          Collector.newBuilder()
              .setFilter(FilterCollector.newBuilder().setQuery(Query.newBuilder().build()).build())
              .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(
          e.getMessage().contains("Filter collector \"filter\" must have nested collectors"));
    }
  }

  @Test
  public void testNoFilter() {
    try {
      doQuery(
          Collector.newBuilder()
              .setFilter(FilterCollector.newBuilder().build())
              .putNestedCollectors(
                  "nested",
                  Collector.newBuilder()
                      .setTerms(
                          TermsCollector.newBuilder().setField("int_value").setSize(10).build())
                      .build())
              .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Unknown filter type: FILTER_NOT_SET"));
    }
  }

  @Test
  public void testNestedNeedsScores() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setTopHits(10)
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setQuery(
                        Query.newBuilder()
                            .setFunctionScoreQuery(
                                FunctionScoreQuery.newBuilder()
                                    .setQuery(
                                        Query.newBuilder()
                                            .setRangeQuery(
                                                RangeQuery.newBuilder()
                                                    .setField("query_field")
                                                    .setLower("2")
                                                    .build())
                                            .build())
                                    .setScript(
                                        Script.newBuilder()
                                            .setLang(JsScriptEngine.LANG)
                                            .setSource("query_field * int_value")
                                            .build())
                                    .build())
                            .build())
                    .putCollectors(
                        "filter",
                        Collector.newBuilder()
                            .setFilter(
                                FilterCollector.newBuilder()
                                    .setSetQuery(
                                        TermInSetQuery.newBuilder()
                                            .setField("int_value")
                                            .setIntTerms(
                                                IntTerms.newBuilder()
                                                    .addTerms(2)
                                                    .addTerms(3)
                                                    .build())
                                            .build())
                                    .build())
                            .putNestedCollectors(
                                "top_docs",
                                Collector.newBuilder()
                                    .setTopHitsCollector(
                                        TopHitsCollector.newBuilder()
                                            .setTopHits(10)
                                            .addRetrieveFields("query_field")
                                            .addRetrieveFields("int_value")
                                            .build())
                                    .build())
                            .build())
                    .build());
    HitsResult hitsResult =
        response
            .getCollectorResultsOrThrow("filter")
            .getFilterResult()
            .getNestedCollectorResultsOrThrow("top_docs")
            .getHitsResult();
    verifyHitsResult(hitsResult);
  }

  @Test
  public void testMultipleNestedCollectors() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setTopHits(10)
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setQuery(
                        Query.newBuilder()
                            .setFunctionScoreQuery(
                                FunctionScoreQuery.newBuilder()
                                    .setQuery(
                                        Query.newBuilder()
                                            .setRangeQuery(
                                                RangeQuery.newBuilder()
                                                    .setField("query_field")
                                                    .setLower("2")
                                                    .build())
                                            .build())
                                    .setScript(
                                        Script.newBuilder()
                                            .setLang(JsScriptEngine.LANG)
                                            .setSource("query_field * int_value")
                                            .build())
                                    .build())
                            .build())
                    .putCollectors(
                        "filter",
                        Collector.newBuilder()
                            .setFilter(
                                FilterCollector.newBuilder()
                                    .setSetQuery(
                                        TermInSetQuery.newBuilder()
                                            .setField("int_value")
                                            .setIntTerms(
                                                IntTerms.newBuilder()
                                                    .addTerms(2)
                                                    .addTerms(3)
                                                    .build())
                                            .build())
                                    .build())
                            .putNestedCollectors(
                                "top_docs",
                                Collector.newBuilder()
                                    .setTopHitsCollector(
                                        TopHitsCollector.newBuilder()
                                            .setTopHits(10)
                                            .addRetrieveFields("query_field")
                                            .addRetrieveFields("int_value")
                                            .build())
                                    .build())
                            .putNestedCollectors(
                                "terms",
                                Collector.newBuilder()
                                    .setTerms(
                                        TermsCollector.newBuilder()
                                            .setField("query_field")
                                            .setSize(10)
                                            .build())
                                    .build())
                            .build())
                    .build());
    HitsResult hitsResult =
        response
            .getCollectorResultsOrThrow("filter")
            .getFilterResult()
            .getNestedCollectorResultsOrThrow("top_docs")
            .getHitsResult();
    verifyHitsResult(hitsResult);
    BucketResult bucketResult =
        response
            .getCollectorResultsOrThrow("filter")
            .getFilterResult()
            .getNestedCollectorResultsOrThrow("terms")
            .getBucketResult();
    verifyBucketResult(bucketResult);
  }

  @Test
  public void testSetQueryFieldNotExist() {
    try {
      doQuery(
          Collector.newBuilder()
              .setFilter(
                  FilterCollector.newBuilder()
                      .setSetQuery(
                          TermInSetQuery.newBuilder()
                              .setField("not_exist")
                              .setIntTerms(IntTerms.newBuilder().addTerms(1).build())
                              .build())
                      .build())
              .putNestedCollectors(
                  "terms",
                  Collector.newBuilder()
                      .setTerms(
                          TermsCollector.newBuilder().setField("query_field").setSize(10).build())
                      .build())
              .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Unknown filter field: not_exist"));
    }
  }

  @Test
  public void testSetQueryFieldNotIndexable() {
    try {
      doQuery(
          Collector.newBuilder()
              .setFilter(
                  FilterCollector.newBuilder()
                      .setSetQuery(
                          TermInSetQuery.newBuilder()
                              .setField("virtual_field")
                              .setIntTerms(IntTerms.newBuilder().addTerms(1).build())
                              .build())
                      .build())
              .putNestedCollectors(
                  "terms",
                  Collector.newBuilder()
                      .setTerms(
                          TermsCollector.newBuilder().setField("query_field").setSize(10).build())
                      .build())
              .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Filter field is not indexable: virtual_field"));
    }
  }

  @Test
  public void testSetQueryFieldNoDocValues() {
    try {
      doQuery(
          Collector.newBuilder()
              .setFilter(
                  FilterCollector.newBuilder()
                      .setSetQuery(
                          TermInSetQuery.newBuilder()
                              .setField("no_doc_values")
                              .setIntTerms(IntTerms.newBuilder().addTerms(1).build())
                              .build())
                      .build())
              .putNestedCollectors(
                  "terms",
                  Collector.newBuilder()
                      .setTerms(
                          TermsCollector.newBuilder().setField("query_field").setSize(10).build())
                      .build())
              .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(
          e.getMessage().contains("Filter field must have doc values enabled: no_doc_values"));
    }
  }

  @Test
  public void testSetQueryNoTermsSet() {
    try {
      doQuery(
          Collector.newBuilder()
              .setFilter(
                  FilterCollector.newBuilder()
                      .setSetQuery(TermInSetQuery.newBuilder().setField("int_value").build())
                      .build())
              .putNestedCollectors(
                  "terms",
                  Collector.newBuilder()
                      .setTerms(
                          TermsCollector.newBuilder().setField("query_field").setSize(10).build())
                      .build())
              .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Unknown filter term type: TERMTYPES_NOT_SET"));
    }
  }

  private SearchResponse doQuery(Collector filterCollector) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setTopHits(10)
                .setIndexName(DEFAULT_TEST_INDEX)
                .setQuery(
                    Query.newBuilder()
                        .setRangeQuery(
                            RangeQuery.newBuilder().setField("query_field").setLower("2").build())
                        .build())
                .putCollectors("filter", filterCollector)
                .build());
  }

  private void queryAndVerify(TermInSetQuery termInSetQuery, boolean setFilter) {
    FilterCollector.Builder filterBuilder = FilterCollector.newBuilder();
    if (setFilter) {
      filterBuilder.setSetQuery(termInSetQuery);
    } else {
      filterBuilder.setQuery(Query.newBuilder().setTermInSetQuery(termInSetQuery).build());
    }

    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setTopHits(10)
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setQuery(
                        Query.newBuilder()
                            .setRangeQuery(
                                RangeQuery.newBuilder()
                                    .setField("query_field")
                                    .setLower("2")
                                    .build())
                            .build())
                    .putCollectors(
                        "filter",
                        Collector.newBuilder()
                            .setFilter(filterBuilder)
                            .putNestedCollectors(
                                "terms",
                                Collector.newBuilder()
                                    .setTerms(
                                        TermsCollector.newBuilder()
                                            .setField("query_field")
                                            .setSize(10)
                                            .build())
                                    .build())
                            .build())
                    .build());

    assertEquals(7, response.getTotalHits().getValue());
    assertEquals(1, response.getCollectorResultsCount());
    CollectorResult collectorResult = response.getCollectorResultsOrThrow("filter");
    assertEquals(CollectorResultsCase.FILTERRESULT, collectorResult.getCollectorResultsCase());
    FilterResult filterResult = collectorResult.getFilterResult();
    assertEquals(3, filterResult.getDocCount());

    assertEquals(1, filterResult.getNestedCollectorResultsCount());
    collectorResult = filterResult.getNestedCollectorResultsOrThrow("terms");
    assertEquals(CollectorResultsCase.BUCKETRESULT, collectorResult.getCollectorResultsCase());
    BucketResult bucketResult = collectorResult.getBucketResult();
    verifyBucketResult(bucketResult);
  }

  private void verifyBucketResult(BucketResult bucketResult) {
    assertEquals(2, bucketResult.getTotalBuckets());
    assertEquals(2, bucketResult.getBucketsCount());

    Map<String, Integer> valuesMap = new HashMap<>();
    for (Bucket bucket : bucketResult.getBucketsList()) {
      valuesMap.put(bucket.getKey(), bucket.getCount());
    }
    assertEquals(Set.of("2", "3"), valuesMap.keySet());
    assertEquals(1, valuesMap.get("2").intValue());
    assertEquals(2, valuesMap.get("3").intValue());
  }

  private void verifyHitsResult(HitsResult hitsResult) {
    assertEquals(3, hitsResult.getTotalHits().getValue());
    assertEquals(3, hitsResult.getHitsCount());

    Hit hit = hitsResult.getHits(0);
    assertEquals(9.0, hit.getScore(), 0);
    CompositeFieldValue intFieldValue = hit.getFieldsOrThrow("int_value");
    assertEquals(
        List.of(3, 5),
        List.of(
            intFieldValue.getFieldValue(0).getIntValue(),
            intFieldValue.getFieldValue(1).getIntValue()));
    assertEquals(3, hit.getFieldsOrThrow("query_field").getFieldValue(0).getIntValue());

    hit = hitsResult.getHits(1);
    assertEquals(4.0, hit.getScore(), 0);
    assertEquals(2, hit.getFieldsOrThrow("int_value").getFieldValue(0).getIntValue());
    assertEquals(2, hit.getFieldsOrThrow("query_field").getFieldValue(0).getIntValue());

    hit = hitsResult.getHits(2);
    assertEquals(0.0, hit.getScore(), 0);
    intFieldValue = hit.getFieldsOrThrow("int_value");
    assertEquals(
        List.of(0, 2),
        List.of(
            intFieldValue.getFieldValue(0).getIntValue(),
            intFieldValue.getFieldValue(1).getIntValue()));
    assertEquals(3, hit.getFieldsOrThrow("query_field").getFieldValue(0).getIntValue());
  }
}
