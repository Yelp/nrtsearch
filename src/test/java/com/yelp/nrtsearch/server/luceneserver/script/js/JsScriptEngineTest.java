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

import static com.yelp.nrtsearch.server.grpc.GrpcServer.rmDir;
import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.GrpcServer;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RefreshRequest;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.VirtualField;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import io.grpc.testing.GrpcCleanupRule;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JsScriptEngineTest {
  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  /**
   * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private GrpcServer grpcServer;
  private CollectorRegistry collectorRegistry;

  @After
  public void tearDown() throws IOException {
    tearDownGrpcServer();
  }

  private void tearDownGrpcServer() throws IOException {
    grpcServer.getGlobalState().close();
    grpcServer.shutdown();
    rmDir(Paths.get(grpcServer.getIndexDir()).getParent());
  }

  @Before
  public void setUp() throws IOException {
    collectorRegistry = new CollectorRegistry();
    grpcServer = setUpGrpcServer(collectorRegistry);
  }

  private GrpcServer setUpGrpcServer(CollectorRegistry collectorRegistry) throws IOException {
    String testIndex = "test_index";
    LuceneServerConfiguration luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE, folder.getRoot());
    GlobalState globalState = new GlobalState(luceneServerConfiguration);
    return new GrpcServer(
        collectorRegistry,
        grpcCleanup,
        luceneServerConfiguration,
        folder,
        false,
        globalState,
        luceneServerConfiguration.getIndexDir(),
        testIndex,
        globalState.getPort(),
        null,
        Collections.emptyList());
  }

  @Test
  public void testBindsIndexFields() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    SearchResponse searchResponse =
        doFunctionScoreQuery("long_field*3.0+double_field*5.0", Collections.emptyMap());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(58.05, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(41.05, searchResponse.getHits(1).getScore(), 0.001);
  }

  @Test
  public void testBindsScriptParam() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

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
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

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
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

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
  public void testBindsParamsFirst() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    Map<String, Script.ParamValue> params = new HashMap<>();
    params.put("param_1", Script.ParamValue.newBuilder().setLongValue(10).build());
    params.put("param_2", Script.ParamValue.newBuilder().setFloatValue(2.22F).build());
    params.put("count", Script.ParamValue.newBuilder().setDoubleValue(1.11).build());

    SearchResponse searchResponse =
        doFunctionScoreQuery("param_1*long_field+count*double_field", params);
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(162.2311, searchResponse.getHits(0).getScore(), 0.001);
    assertEquals(121.1211, searchResponse.getHits(1).getScore(), 0.001);
  }

  @Test
  public void testExpressionsWithDifferentParams() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

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
                    .setSource("param_3*long_field+param_2*count+param_1*float_field")
                    .putAllParams(params1)
                    .build())
            .build();

    VirtualField virtualField2 =
        VirtualField.newBuilder()
            .setName("expr_2")
            .setScript(
                Script.newBuilder()
                    .setLang("js")
                    .setSource("param_3*long_field+param_2*count+param_1*float_field")
                    .putAllParams(params2)
                    .build())
            .build();

    List<VirtualField> fields = Arrays.asList(virtualField1, virtualField2);

    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
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

  private SearchResponse doFunctionScoreQuery(
      String scriptSource, Map<String, Script.ParamValue> params) {
    return grpcServer
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(grpcServer.getTestIndex())
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
