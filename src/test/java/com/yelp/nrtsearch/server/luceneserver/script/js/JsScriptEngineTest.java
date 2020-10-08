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
package com.yelp.nrtsearch.server.luceneserver.script.js;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.VirtualField;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;

public class JsScriptEngineTest extends ServerTestCase {
  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/script/registerFieldsJs.json");
  }

  protected void initIndex(String name) throws Exception {
    addDocsFromResourceFile(name, "/script/addDocsJs.csv");
  }

  @Test
  public void testBindsIndexFields() throws Exception {
    SearchResponse searchResponse =
        doFunctionScoreQuery("long_field*3.0+double_field*5.0", Collections.emptyMap());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(58.05, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(41.05, searchResponse.getHits(1).getScore(), 0.001);
  }

  @Test
  public void testBindsExtendedIndexFields() throws Exception {
    SearchResponse searchResponse =
        doFunctionScoreQuery(
            "doc['long_field'].value*3.0+doc['double_field'].value*5.0", Collections.emptyMap());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(58.05, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(41.05, searchResponse.getHits(1).getScore(), 0.001);
  }

  @Test
  public void testBindsMixedIndexFields() throws Exception {
    SearchResponse searchResponse =
        doFunctionScoreQuery(
            "doc['long_field'].value*3.0+double_field*5.0", Collections.emptyMap());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(58.05, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(41.05, searchResponse.getHits(1).getScore(), 0.001);
  }

  @Test
  public void testBindsScriptParam() throws Exception {
    Map<String, Script.ParamValue> params = new HashMap<>();
    params.put("param_1", Script.ParamValue.newBuilder().setLongValue(10).build());
    params.put("param_2", Script.ParamValue.newBuilder().setFloatValue(2.22F).build());
    params.put("param_3", Script.ParamValue.newBuilder().setDoubleValue(1.11).build());

    SearchResponse searchResponse = doFunctionScoreQuery("param_1*2.0+param_2*5.0", params);
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(31.1, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(31.1, searchResponse.getHits(1).getScore(), 0.001);
  }

  @Test
  public void testBindsAllScriptParamTypes() throws Exception {
    Map<String, Script.ParamValue> params = new HashMap<>();
    params.put("bool_p", Script.ParamValue.newBuilder().setBooleanValue(true).build());
    params.put("int_p", Script.ParamValue.newBuilder().setIntValue(3).build());
    params.put("long_p", Script.ParamValue.newBuilder().setLongValue(100).build());
    params.put("float_p", Script.ParamValue.newBuilder().setFloatValue(5.55F).build());
    params.put("double_p", Script.ParamValue.newBuilder().setDoubleValue(1.11).build());

    SearchResponse searchResponse =
        doFunctionScoreQuery("bool_p*2.0+int_p*3.0+long_p*5.0+float_p*7.0+double_p*11.0", params);
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(562.06, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(562.06, searchResponse.getHits(1).getScore(), 0.001);
  }

  @Test
  public void testBindsFieldsAndParams() throws Exception {
    Map<String, Script.ParamValue> params = new HashMap<>();
    params.put("param_1", Script.ParamValue.newBuilder().setLongValue(10).build());
    params.put("param_2", Script.ParamValue.newBuilder().setFloatValue(2.22F).build());
    params.put("param_3", Script.ParamValue.newBuilder().setDoubleValue(1.11).build());

    SearchResponse searchResponse =
        doFunctionScoreQuery("param_1*long_field+param_2*float_field+param_3", params);
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(605.1544, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(343.1322, searchResponse.getHits(1).getScore(), 0.001);
  }

  @Test
  public void testBindsExtendedFieldsAndParams() throws Exception {
    Map<String, Script.ParamValue> params = new HashMap<>();
    params.put("param_1", Script.ParamValue.newBuilder().setLongValue(10).build());
    params.put("param_2", Script.ParamValue.newBuilder().setFloatValue(2.22F).build());
    params.put("param_3", Script.ParamValue.newBuilder().setDoubleValue(1.11).build());

    SearchResponse searchResponse =
        doFunctionScoreQuery(
            "param_1*doc['long_field'].value+param_2*doc['float_field'].value+param_3", params);
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(605.1544, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(343.1322, searchResponse.getHits(1).getScore(), 0.001);
  }

  @Test
  public void testBindsParamsFirst() throws Exception {
    Map<String, Script.ParamValue> params = new HashMap<>();
    params.put("param_1", Script.ParamValue.newBuilder().setLongValue(10).build());
    params.put("param_2", Script.ParamValue.newBuilder().setFloatValue(2.22F).build());
    params.put("int_field", Script.ParamValue.newBuilder().setDoubleValue(1.11).build());

    SearchResponse searchResponse =
        doFunctionScoreQuery("param_1*long_field+int_field*double_field", params);
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(162.2311, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(121.1211, searchResponse.getHits(1).getScore(), 0.001);
  }

  @Test
  public void testExpressionsWithDifferentParams() throws Exception {
    Map<String, Script.ParamValue> params1 = new HashMap<>();
    params1.put("param_1", Script.ParamValue.newBuilder().setLongValue(10).build());
    params1.put("param_2", Script.ParamValue.newBuilder().setFloatValue(2.22F).build());
    params1.put("param_3", Script.ParamValue.newBuilder().setDoubleValue(1.11).build());

    Map<String, Script.ParamValue> params2 = new HashMap<>();
    params2.put("param_1", Script.ParamValue.newBuilder().setLongValue(7).build());
    params2.put("param_2", Script.ParamValue.newBuilder().setFloatValue(3.33F).build());
    params2.put("param_3", Script.ParamValue.newBuilder().setIntValue(25).build());

    VirtualField virtualField1 =
        VirtualField.newBuilder()
            .setName("expr_1")
            .setScript(
                Script.newBuilder()
                    .setLang("js")
                    .setSource("param_3*long_field+param_2*int_field+param_1*float_field")
                    .putAllParams(params1)
                    .build())
            .build();

    VirtualField virtualField2 =
        VirtualField.newBuilder()
            .setName("expr_2")
            .setScript(
                Script.newBuilder()
                    .setLang("js")
                    .setSource("param_3*long_field+param_2*int_field+param_1*float_field")
                    .putAllParams(params2)
                    .build())
            .build();

    List<VirtualField> fields = Arrays.asList(virtualField1, virtualField2);

    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllVirtualFields(fields)
                    .build());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(
        1020.08,
        searchResponse.getHits(0).getFieldsOrThrow("expr_1").getFieldValue(0).getDoubleValue(),
        0.001);
    assertEquals(
        2033.5,
        searchResponse.getHits(1).getFieldsOrThrow("expr_1").getFieldValue(0).getDoubleValue(),
        0.001);
    assertEquals(
        1010.06,
        searchResponse.getHits(0).getFieldsOrThrow("expr_2").getFieldValue(0).getDoubleValue(),
        0.001);
    assertEquals(
        1823.45,
        searchResponse.getHits(1).getFieldsOrThrow("expr_2").getFieldValue(0).getDoubleValue(),
        0.001);
  }

  @Test
  public void testVirtualField() {
    SearchResponse searchResponse =
        doFunctionScoreQuery("virtual_field + 10", Collections.emptyMap());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(458.04, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(246.02, searchResponse.getHits(1).getScore(), 0.001);
  }

  @Test
  public void testVirtualFieldExtended() {
    SearchResponse searchResponse =
        doFunctionScoreQuery("doc['virtual_field'].value + 10", Collections.emptyMap());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(458.04, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(246.02, searchResponse.getHits(1).getScore(), 0.001);
  }

  @Test
  public void testAllExtendedSingleValueBindings() {
    SearchResponse searchResponse =
        doFunctionScoreQuery(
            "doc['long_field'].value*3.0+doc['double_field'].value*5.0+doc['float_field'].value*2.0+doc['int_field'].value",
            Collections.emptyMap());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(465.09, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(244.07, searchResponse.getHits(1).getScore(), 0.001);
  }

  @Test
  public void testAllMultiValueBindings() {
    SearchResponse searchResponse =
        doFunctionScoreQuery(
            "doc['long_field_multi'].value*3.0+doc['double_field_multi'].value*5.0+doc['float_field_multi'].value*2.0+doc['int_field_multi'].value",
            Collections.emptyMap());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(831.4, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(508.7, searchResponse.getHits(1).getScore(), 0.001);
  }

  @Test
  public void testSingleValueLength() {
    verifyLength("int_field", 1);
    verifyLength("long_field", 1);
    verifyLength("float_field", 1);
    verifyLength("double_field", 1);

    verifyLength("empty_int", 0);
    verifyLength("empty_long", 0);
    verifyLength("empty_float", 0);
    verifyLength("empty_double", 0);
  }

  @Test
  public void testSingleValueEmpty() {
    verifyEmpty("int_field", false);
    verifyEmpty("long_field", false);
    verifyEmpty("float_field", false);
    verifyEmpty("double_field", false);

    verifyEmpty("empty_int", true);
    verifyEmpty("empty_long", true);
    verifyEmpty("empty_float", true);
    verifyEmpty("empty_double", true);
  }

  @Test
  public void testMultiValueLength() {
    verifyLength("int_field_multi", 2);
    verifyLength("long_field_multi", 2);
    verifyLength("float_field_multi", 2);
    verifyLength("double_field_multi", 2);

    verifyLength("empty_int_multi", 0);
    verifyLength("empty_long_multi", 0);
    verifyLength("empty_float_multi", 0);
    verifyLength("empty_double_multi", 0);
  }

  @Test
  public void testMultiValueEmpty() {
    verifyEmpty("int_field_multi", false);
    verifyEmpty("long_field_multi", false);
    verifyEmpty("float_field_multi", false);
    verifyEmpty("double_field_multi", false);

    verifyEmpty("empty_int_multi", true);
    verifyEmpty("empty_long_multi", true);
    verifyEmpty("empty_float_multi", true);
    verifyEmpty("empty_double_multi", true);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testNumericUnknownProperty() {
    doFunctionScoreQuery("doc['long_field_multi'].invalid", Collections.emptyMap());
  }

  @Test(expected = StatusRuntimeException.class)
  public void testVirtualFieldUnknownProperty() {
    doFunctionScoreQuery("doc['virtual_field'].length", Collections.emptyMap());
  }

  private void verifyLength(String field, double expectedLength) {
    SearchResponse searchResponse =
        doFunctionScoreQuery("doc['" + field + "'].length", Collections.emptyMap());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(expectedLength, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(expectedLength, searchResponse.getHits(1).getScore(), 0.001);
  }

  private void verifyEmpty(String field, boolean expectedEmpty) {
    SearchResponse searchResponse =
        doFunctionScoreQuery("doc['" + field + "'].empty ? 5 : 4", Collections.emptyMap());
    double expectedValue;
    if (expectedEmpty) {
      expectedValue = 5;
    } else {
      expectedValue = 4;
    }
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(expectedValue, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(expectedValue, searchResponse.getHits(1).getScore(), 0.001);
  }

  private SearchResponse doFunctionScoreQuery(
      String scriptSource, Map<String, Script.ParamValue> params) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .setQuery(
                    Query.newBuilder()
                        .setFunctionScoreQuery(
                            FunctionScoreQuery.newBuilder()
                                .setScript(
                                    Script.newBuilder()
                                        .setLang("js")
                                        .setSource(scriptSource)
                                        .putAllParams(params)
                                        .build())
                                .setQuery(
                                    Query.newBuilder()
                                        .setMatchQuery(
                                            MatchQuery.newBuilder()
                                                .setField("vendor_name")
                                                .setQuery("first vendor")
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build());
  }
}
