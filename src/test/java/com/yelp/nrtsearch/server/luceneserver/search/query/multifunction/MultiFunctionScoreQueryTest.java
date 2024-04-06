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
package com.yelp.nrtsearch.server.luceneserver.search.query.multifunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.type.LatLng;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery.BoostMode;
import com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery.FunctionScoreMode;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.script.js.JsScriptEngine;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Test;

public class MultiFunctionScoreQueryTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private static final Script DOUBLE_SCRIPT =
      Script.newBuilder().setLang(JsScriptEngine.LANG).setSource("double_field").build();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/query/multifunction/registerFieldsMFSQ.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields("double_field", MultiValuedField.newBuilder().addValue("3.3").build())
            .putFields(
                "text_field",
                MultiValuedField.newBuilder()
                    .addValue("Document1 with none of filter terms")
                    .build())
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields("double_field", MultiValuedField.newBuilder().addValue("4.4").build())
            .putFields(
                "text_field",
                MultiValuedField.newBuilder().addValue("Document2 with term1 filter term").build())
            .putFields(
                "lat_lon_field",
                MultiValuedField.newBuilder().addValue("41.8781").addValue("-87.6298").build())
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("3").build())
            .putFields("double_field", MultiValuedField.newBuilder().addValue("5.5").build())
            .putFields(
                "text_field",
                MultiValuedField.newBuilder().addValue("Document1 with term2 filter term").build())
            .putFields(
                "lat_lon_field",
                MultiValuedField.newBuilder().addValue("51.5074").addValue("-0.1278").build())
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("4").build())
            .putFields("double_field", MultiValuedField.newBuilder().addValue("6.6").build())
            .putFields(
                "text_field",
                MultiValuedField.newBuilder()
                    .addValue("Document2 with both term1 and term2 filter terms")
                    .build())
            .putFields(
                "lat_lon_field",
                MultiValuedField.newBuilder().addValue("45.5051").addValue("-122.6750").build())
            .build();
    docs.add(request);
    addDocuments(docs.stream());
  }

  @Test
  public void testNoFunctionsMatchAll() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            Collections.emptyList(),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(1, 2, 3, 4), List.of(1.0, 1.0, 1.0, 1.0));
  }

  @Test
  public void testNoFunctionsInnerQuery() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("Document2").build())
                .build(),
            Collections.emptyList(),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(2, 4), List.of(0.33812057971954346, 0.27725890278816223));
  }

  @Test
  public void testSingle_boostModeDefault() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("text_field")
                    .setQuery(
                        Query.newBuilder()
                            .setMultiFunctionScoreQuery(
                                MultiFunctionScoreQuery.newBuilder()
                                    .setQuery(Query.newBuilder().build())
                                    .addAllFunctions(
                                        List.of(
                                            MultiFunctionScoreQuery.FilterFunction.newBuilder()
                                                .setFilter(
                                                    Query.newBuilder()
                                                        .setMatchQuery(
                                                            MatchQuery.newBuilder()
                                                                .setField("text_field")
                                                                .setQuery("Document2")
                                                                .build())
                                                        .build())
                                                .setWeight(1.5f)
                                                .build()))
                                    .setScoreMode(FunctionScoreMode.SCORE_MODE_MULTIPLY)
                                    .build())
                            .build())
                    .build());
    verifyResponseHits(response, List.of(1, 2, 3, 4), List.of(1.0, 1.5, 1.0, 1.5));
  }

  @Test
  public void testSingle_boostModeMultiply() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("Document2")
                                    .build())
                            .build())
                    .setWeight(1.5f)
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(1, 2, 3, 4), List.of(1.0, 1.5, 1.0, 1.5));
  }

  @Test
  public void testSingle_boostModeSum() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("Document2")
                                    .build())
                            .build())
                    .setWeight(1.5f)
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_SUM);
    verifyResponseHits(response, List.of(1, 2, 3, 4), List.of(2.0, 2.5, 2.0, 2.5));
  }

  @Test
  public void testSingle_boostModeReplace() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("Document2")
                                    .build())
                            .build())
                    .setWeight(1.5f)
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_REPLACE);
    verifyResponseHits(response, List.of(1, 2, 3, 4), List.of(1.0, 1.5, 1.0, 1.5));
  }

  @Test
  public void testSingleWeightMatchAll_noFilter_noWeight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(MultiFunctionScoreQuery.FilterFunction.newBuilder().build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(1, 2, 3, 4), List.of(1.0, 1.0, 1.0, 1.0));
  }

  @Test
  public void testSingleWeightMatchAll_filter_noWeight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("Document2")
                                    .build())
                            .build())
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(1, 2, 3, 4), List.of(1.0, 1.0, 1.0, 1.0));
  }

  @Test
  public void testSingleWeightMatchAll_noFilter_weight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(MultiFunctionScoreQuery.FilterFunction.newBuilder().setWeight(1.5f).build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(1, 2, 3, 4), List.of(1.5, 1.5, 1.5, 1.5));
  }

  @Test
  public void testSingleWeightMatchAll_filter_weight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("Document2")
                                    .build())
                            .build())
                    .setWeight(1.5f)
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(1, 2, 3, 4), List.of(1.0, 1.5, 1.0, 1.5));
  }

  @Test
  public void testSingleWeightInnerQuery_noFilter_noWeight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("Document2").build())
                .build(),
            List.of(MultiFunctionScoreQuery.FilterFunction.newBuilder().build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(2, 4), List.of(0.33812057971954346, 0.27725890278816223));
  }

  @Test
  public void testSingleWeightInnerQuery_filter_noWeight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("Document2").build())
                .build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("term2")
                                    .build())
                            .build())
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(2, 4), List.of(0.33812057971954346, 0.27725890278816223));
  }

  @Test
  public void testSingleWeightInnerQuery_noFilter_weight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("Document2").build())
                .build(),
            List.of(MultiFunctionScoreQuery.FilterFunction.newBuilder().setWeight(1.5f).build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(2, 4), List.of(0.5071808695793152, 0.41588836908340454));
  }

  @Test
  public void testSingleWeightInnerQuery_filter_weight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("Document2").build())
                .build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("term2")
                                    .build())
                            .build())
                    .setWeight(1.5f)
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(2, 4), List.of(0.33812057971954346, 0.41588836908340454));
  }

  @Test
  public void testSingleScriptMatchAll_noFilter_noWeight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setScript(DOUBLE_SCRIPT)
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(1, 2, 3, 4), List.of(3.3, 4.4, 5.5, 6.6));
  }

  @Test
  public void testSingleScriptMatchAll_filter_noWeight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("Document2")
                                    .build())
                            .build())
                    .setScript(DOUBLE_SCRIPT)
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(1, 2, 3, 4), List.of(1.0, 4.4, 1.0, 6.6));
  }

  @Test
  public void testSingleScriptMatchAll_noFilter_weight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setWeight(1.5f)
                    .setScript(DOUBLE_SCRIPT)
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(1, 2, 3, 4), List.of(4.95, 6.6, 8.25, 9.9));
  }

  @Test
  public void testSingleScriptMatchAll_filter_weight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("Document2")
                                    .build())
                            .build())
                    .setWeight(1.5f)
                    .setScript(DOUBLE_SCRIPT)
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(1, 2, 3, 4), List.of(1.0, 6.6, 1.0, 9.9));
  }

  @Test
  public void testSingleScriptInnerQuery_noFilter_noWeight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("Document2").build())
                .build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setScript(DOUBLE_SCRIPT)
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(2, 4), List.of(1.4877305030822754, 1.8299087285995483));
  }

  @Test
  public void testSingleScriptInnerQuery_filter_noWeight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("Document2").build())
                .build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("term2")
                                    .build())
                            .build())
                    .setScript(DOUBLE_SCRIPT)
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(2, 4), List.of(0.33812057971954346, 1.8299087285995483));
  }

  @Test
  public void testSingleScriptInnerQuery_noFilter_weight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("Document2").build())
                .build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setWeight(1.5f)
                    .setScript(DOUBLE_SCRIPT)
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(2, 4), List.of(2.231595754623413, 2.7448630332946777));
  }

  @Test
  public void testSingleScriptInnerQuery_filter_weight() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("Document2").build())
                .build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("term2")
                                    .build())
                            .build())
                    .setWeight(1.5f)
                    .setScript(DOUBLE_SCRIPT)
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(2, 4), List.of(0.33812057971954346, 2.7448630332946777));
  }

  @Test
  public void testScriptWithScore() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("Document2").build())
                .build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setScript(
                        Script.newBuilder()
                            .setLang(JsScriptEngine.LANG)
                            .setSource("double_field * _score"))
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHits(response, List.of(2, 4), List.of(0.5030323266983032, 0.5073584914207458));
  }

  @Test
  public void testExpDecayFunctionGeoPointWithWeight() {
    LatLng latLng = LatLng.newBuilder().setLatitude(40.7128).setLongitude(-74.0060).build();
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("Document2").build())
                .build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setDecayFunction(
                        MultiFunctionScoreQuery.DecayFunction.newBuilder()
                            .setDecay(0.99f)
                            .setDecayType(MultiFunctionScoreQuery.DecayType.DECAY_TYPE_EXPONENTIAL)
                            .setOffset("0 m")
                            .setScale("1 km")
                            .setGeoPoint(latLng)
                            .setFieldName("lat_lon_field")
                            .build())
                    .setWeight(0.7f)
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHitsWithDelta(
        response, List.of(2, 4), List.of(2.3963971216289792E-6, 2.034676950471705E-18), 0.00000001);
  }

  @Test
  public void testExpDecayFunctionGeoPoint() {
    LatLng latLng = LatLng.newBuilder().setLatitude(40.7128).setLongitude(-74.0060).build();
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("Document2").build())
                .build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setDecayFunction(
                        MultiFunctionScoreQuery.DecayFunction.newBuilder()
                            .setDecay(0.99f)
                            .setDecayType(MultiFunctionScoreQuery.DecayType.DECAY_TYPE_EXPONENTIAL)
                            .setOffset("0 m")
                            .setScale("1 km")
                            .setGeoPoint(latLng)
                            .setFieldName("lat_lon_field")
                            .build())
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHitsWithDelta(
        response, List.of(2, 4), List.of(3.4234246868436458E-6, 2.034676950471705E-18), 0.0);
  }

  @Test
  public void testExpDecayFunctionNoDocValue() {
    LatLng latLng = LatLng.newBuilder().setLatitude(40.7128).setLongitude(-74.0060).build();
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("none").build())
                .build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setDecayFunction(
                        MultiFunctionScoreQuery.DecayFunction.newBuilder()
                            .setDecay(0.99f)
                            .setDecayType(MultiFunctionScoreQuery.DecayType.DECAY_TYPE_EXPONENTIAL)
                            .setOffset("0 m")
                            .setScale("1 km")
                            .setGeoPoint(latLng)
                            .setFieldName("lat_lon_field")
                            .build())
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHitsWithDelta(response, List.of(1), List.of(0.0), 0.0);
  }

  @Test
  public void testLinearDecayFunctionGeoPoint() {
    LatLng latLng = LatLng.newBuilder().setLatitude(40.7128).setLongitude(-74.0060).build();
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("Document2").build())
                .build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setDecayFunction(
                        MultiFunctionScoreQuery.DecayFunction.newBuilder()
                            .setDecay(0.2f)
                            .setDecayType(MultiFunctionScoreQuery.DecayType.DECAY_TYPE_LINEAR)
                            .setOffset("100 km")
                            .setScale("6000 km")
                            .setGeoPoint(latLng)
                            .setFieldName("lat_lon_field")
                            .build())
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHitsWithDelta(response, List.of(2, 4), List.of(0.2910, 0.1358), 0.0001);
  }

  @Test
  public void testGuassDecayFunctionGeoPoint() {
    LatLng latLng = LatLng.newBuilder().setLatitude(40.7128).setLongitude(-74.0060).build();
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("text_field").setQuery("Document2").build())
                .build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setDecayFunction(
                        MultiFunctionScoreQuery.DecayFunction.newBuilder()
                            .setDecay(0.5f)
                            .setDecayType(MultiFunctionScoreQuery.DecayType.DECAY_TYPE_GUASSIAN)
                            .setOffset("10000 km")
                            .setScale("100 km")
                            .setGeoPoint(latLng)
                            .setFieldName("lat_lon_field")
                            .build())
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_MULTIPLY);
    verifyResponseHitsWithDelta(response, List.of(2, 4), List.of(0.3381, 0.2772), 0.0001);
  }

  @Test
  public void testInvalidGeoPointField() {
    LatLng latLng = LatLng.newBuilder().setLatitude(40.7128).setLongitude(-74.0060).build();
    assertThrows(
        Exception.class,
        () -> {
          SearchResponse response =
              doQuery(
                  Query.newBuilder()
                      .setMatchQuery(
                          MatchQuery.newBuilder()
                              .setField("text_field")
                              .setQuery("Document2")
                              .build())
                      .build(),
                  List.of(
                      MultiFunctionScoreQuery.FilterFunction.newBuilder()
                          .setDecayFunction(
                              MultiFunctionScoreQuery.DecayFunction.newBuilder()
                                  .setDecay(0.99f)
                                  .setDecayType(
                                      MultiFunctionScoreQuery.DecayType.DECAY_TYPE_EXPONENTIAL)
                                  .setOffset("0 m")
                                  .setScale("1 km")
                                  .setGeoPoint(latLng)
                                  .setFieldName("text_field")
                                  .build())
                          .build()),
                  FunctionScoreMode.SCORE_MODE_MULTIPLY,
                  BoostMode.BOOST_MODE_MULTIPLY);
        });
  }

  @Test
  public void testMultiMatchAll_multiply_multiply() {
    multiFunctionAndVerify(
        Query.newBuilder().build(),
        FunctionScoreMode.SCORE_MODE_MULTIPLY,
        BoostMode.BOOST_MODE_MULTIPLY,
        List.of(1, 2, 3, 4),
        List.of(105.0, 1155.0, 1365.0, 15015.0));
  }

  @Test
  public void testMultiMatchAll_multiply_sum() {
    multiFunctionAndVerify(
        Query.newBuilder().build(),
        FunctionScoreMode.SCORE_MODE_MULTIPLY,
        BoostMode.BOOST_MODE_SUM,
        List.of(1, 2, 3, 4),
        List.of(106.0, 1156.0, 1366.0, 15016.0));
  }

  @Test
  public void testMultiMatchAll_multiply_replace() {
    multiFunctionAndVerify(
        Query.newBuilder().build(),
        FunctionScoreMode.SCORE_MODE_MULTIPLY,
        BoostMode.BOOST_MODE_REPLACE,
        List.of(1, 2, 3, 4),
        List.of(105.0, 1155.0, 1365.0, 15015.0));
  }

  @Test
  public void testMultiMatchAll_sum_multiply() {
    multiFunctionAndVerify(
        Query.newBuilder().build(),
        FunctionScoreMode.SCORE_MODE_SUM,
        BoostMode.BOOST_MODE_MULTIPLY,
        List.of(1, 2, 3, 4),
        List.of(38.0, 49.0, 51.0, 62.0));
  }

  @Test
  public void testMultiMatchAll_sum_sum() {
    multiFunctionAndVerify(
        Query.newBuilder().build(),
        FunctionScoreMode.SCORE_MODE_SUM,
        BoostMode.BOOST_MODE_SUM,
        List.of(1, 2, 3, 4),
        List.of(39.0, 50.0, 52.0, 63.0));
  }

  @Test
  public void testMultiMatchAll_sum_replace() {
    multiFunctionAndVerify(
        Query.newBuilder().build(),
        FunctionScoreMode.SCORE_MODE_SUM,
        BoostMode.BOOST_MODE_REPLACE,
        List.of(1, 2, 3, 4),
        List.of(38.0, 49.0, 51.0, 62.0));
  }

  @Test
  public void testMultiInnerQuery_multiply_multiply() {
    multiFunctionAndVerify(
        Query.newBuilder()
            .setMatchQuery(
                MatchQuery.newBuilder().setField("text_field").setQuery("filter").build())
            .build(),
        FunctionScoreMode.SCORE_MODE_MULTIPLY,
        BoostMode.BOOST_MODE_MULTIPLY,
        List.of(1, 2, 3, 4),
        List.of(5.02856969833374, 59.36165237426758, 70.1546859741211, 632.7952880859375));
  }

  @Test
  public void testMultiInnerQuery_multiply_sum() {
    multiFunctionAndVerify(
        Query.newBuilder()
            .setMatchQuery(
                MatchQuery.newBuilder().setField("text_field").setQuery("filter").build())
            .build(),
        FunctionScoreMode.SCORE_MODE_MULTIPLY,
        BoostMode.BOOST_MODE_SUM,
        List.of(1, 2, 3, 4),
        List.of(105.04788970947266, 1155.0513916015625, 1365.0513916015625, 15015.0419921875));
  }

  @Test
  public void testMultiInnerQuery_multiply_replace() {
    multiFunctionAndVerify(
        Query.newBuilder()
            .setMatchQuery(
                MatchQuery.newBuilder().setField("text_field").setQuery("filter").build())
            .build(),
        FunctionScoreMode.SCORE_MODE_MULTIPLY,
        BoostMode.BOOST_MODE_REPLACE,
        List.of(1, 2, 3, 4),
        List.of(105d, 1155d, 1365d, 15015d));
  }

  @Test
  public void testMultiInnerQuery_sum_multiply() {
    multiFunctionAndVerify(
        Query.newBuilder()
            .setMatchQuery(
                MatchQuery.newBuilder().setField("text_field").setQuery("filter").build())
            .build(),
        FunctionScoreMode.SCORE_MODE_SUM,
        BoostMode.BOOST_MODE_MULTIPLY,
        List.of(1, 2, 3, 4),
        List.of(1.8198633193969727, 2.5183732509613037, 2.621163845062256, 2.612941026687622));
  }

  @Test
  public void testMultiInnerQuery_sum_sum() {
    multiFunctionAndVerify(
        Query.newBuilder()
            .setMatchQuery(
                MatchQuery.newBuilder().setField("text_field").setQuery("filter").build())
            .build(),
        FunctionScoreMode.SCORE_MODE_SUM,
        BoostMode.BOOST_MODE_SUM,
        List.of(1, 2, 3, 4),
        List.of(38.047889709472656, 49.051395416259766, 51.051395416259766, 62.042144775390625));
  }

  @Test
  public void testMultiInnerQuery_sum_replace() {
    multiFunctionAndVerify(
        Query.newBuilder()
            .setMatchQuery(
                MatchQuery.newBuilder().setField("text_field").setQuery("filter").build())
            .build(),
        FunctionScoreMode.SCORE_MODE_SUM,
        BoostMode.BOOST_MODE_REPLACE,
        List.of(1, 2, 3, 4),
        List.of(38d, 49d, 51d, 62d));
  }

  @Test
  public void testSingle_ScriptWithMinScore() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setScript(
                        Script.newBuilder().setLang(JsScriptEngine.LANG).setSource("double_field"))
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_REPLACE,
            5f,
            false);
    verifyResponseHits(response, List.of(3, 4), List.of(5.5, 6.6));
  }

  @Test
  public void testSingle_ScriptWithMinScoreIncluded() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setScript(
                        Script.newBuilder().setLang(JsScriptEngine.LANG).setSource("double_field"))
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_REPLACE,
            4.4f,
            false);
    verifyResponseHits(response, List.of(4, 3, 2), List.of(6.6, 5.5, 4.4));
  }

  @Test
  public void testSingle_ScriptWithMinScoreExcluded() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setScript(
                        Script.newBuilder().setLang(JsScriptEngine.LANG).setSource("double_field"))
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_REPLACE,
            4.4f,
            true);
    verifyResponseHits(response, List.of(4, 3), List.of(6.6, 5.5));
  }

  @Test
  public void testSingle_ScriptWith_0_Score_default() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setScript(Script.newBuilder().setLang(JsScriptEngine.LANG).setSource("0.0"))
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_REPLACE);
    verifyResponseHits(response, List.of(1, 2, 3, 4), List.of(0d, 0d, 0d, 0d));
  }

  @Test
  public void testSingle_ScriptWith_0_Score_excluded() {
    SearchResponse response =
        doQuery(
            Query.newBuilder().build(),
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setScript(Script.newBuilder().setLang(JsScriptEngine.LANG).setSource("0.0"))
                    .build()),
            FunctionScoreMode.SCORE_MODE_MULTIPLY,
            BoostMode.BOOST_MODE_REPLACE,
            0,
            true);
    verifyResponseHits(response, List.of(), List.of());
  }

  @Test
  public void test_noFunctions() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("text_field")
                    .setExplain(true)
                    .setQuery(
                        Query.newBuilder()
                            .setMultiFunctionScoreQuery(
                                MultiFunctionScoreQuery.newBuilder()
                                    .setQuery(
                                        Query.newBuilder()
                                            .setMatchQuery(
                                                MatchQuery.newBuilder()
                                                    .setField("text_field")
                                                    .setQuery("term1")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build());
    verifyResponseHits(response, List.of(2, 4), List.of(0.33812057971954346, 0.27725890278816223));
  }

  @Test
  public void test_noFunctions_withMinScore() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("text_field")
                    .setExplain(true)
                    .setQuery(
                        Query.newBuilder()
                            .setMultiFunctionScoreQuery(
                                MultiFunctionScoreQuery.newBuilder()
                                    .setQuery(
                                        Query.newBuilder()
                                            .setMatchQuery(
                                                MatchQuery.newBuilder()
                                                    .setField("text_field")
                                                    .setQuery("term1")
                                                    .build())
                                            .build())
                                    .setMinScore(0.3f)
                                    .build())
                            .build())
                    .build());
    verifyResponseHits(response, List.of(2), List.of(0.33812057971954346));
  }

  private void multiFunctionAndVerify(
      Query innerQuery,
      FunctionScoreMode scoreMode,
      BoostMode boostMode,
      List<Integer> expectedIds,
      List<Double> expectedScores) {
    SearchResponse response =
        doQuery(
            innerQuery,
            List.of(
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("not_exist")
                                    .build())
                            .build())
                    .setWeight(2.0f)
                    .build(),
                MultiFunctionScoreQuery.FilterFunction.newBuilder().setWeight(3.0f).build(),
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setScript(Script.newBuilder().setLang(JsScriptEngine.LANG).setSource("5.0"))
                    .setWeight(7.0f)
                    .build(),
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("Document2")
                                    .build())
                            .build())
                    .setWeight(11.0f)
                    .build(),
                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("term2")
                                    .build())
                            .build())
                    .setScript(Script.newBuilder().setLang(JsScriptEngine.LANG).setSource("13.0"))
                    .build()),
            scoreMode,
            boostMode);
    verifyResponseHits(response, expectedIds, expectedScores);
  }

  private void verifyResponseHits(
      SearchResponse searchResponse, List<Integer> ids, List<Double> scores) {
    verifyResponseHitsWithDelta(searchResponse, ids, scores, 0.00001);
  }

  private void verifyResponseHitsWithDelta(
      SearchResponse searchResponse, List<Integer> ids, List<Double> scores, Double delta) {
    assertEquals(ids.size(), scores.size());
    Map<String, Double> scoresMap = new HashMap<>();
    for (int i = 0; i < ids.size(); ++i) {
      scoresMap.put(Integer.toString(ids.get(i)), scores.get(i));
    }
    assertEquals(scoresMap.size(), searchResponse.getHitsCount());

    Map<String, Double> responseScoresMap = new HashMap<>();
    for (Hit hit : searchResponse.getHitsList()) {
      responseScoresMap.put(
          hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue(), hit.getScore());
    }
    for (Map.Entry<String, Double> entry : scoresMap.entrySet()) {
      assertEquals(entry.getValue(), responseScoresMap.get(entry.getKey()), delta);
    }
  }

  private SearchResponse doQuery(
      Query innerQuery,
      List<MultiFunctionScoreQuery.FilterFunction> functions,
      FunctionScoreMode functionScoreMode,
      BoostMode boostMode) {
    return doQuery(innerQuery, functions, functionScoreMode, boostMode, 0, false);
  }

  private SearchResponse doQuery(
      Query innerQuery,
      List<MultiFunctionScoreQuery.FilterFunction> functions,
      FunctionScoreMode functionScoreMode,
      BoostMode boostMode,
      float minScore,
      boolean minExcluded) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("text_field")
                .setQuery(
                    Query.newBuilder()
                        .setMultiFunctionScoreQuery(
                            MultiFunctionScoreQuery.newBuilder()
                                .setQuery(innerQuery)
                                .addAllFunctions(functions)
                                .setScoreMode(functionScoreMode)
                                .setBoostMode(boostMode)
                                .setMinScore(minScore)
                                .setMinExcluded(minExcluded)
                                .build())
                        .build())
                .build());
  }
}
