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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class QueryTest {

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

  @After
  public void tearDown() throws IOException {
    tearDownGrpcServer();
  }

  private void tearDownGrpcServer() throws IOException {
    grpcServer.getGlobalState().close();
    grpcServer.shutdown();
    GrpcServer.rmDir(Paths.get(grpcServer.getIndexDir()).getParent());
  }

  @Before
  public void setUp() throws Exception {
    grpcServer = setUpGrpcServer();
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
  }

  private GrpcServer setUpGrpcServer() throws IOException {
    String testIndex = "test_index";
    LuceneServerConfiguration luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE, folder.getRoot());
    return new GrpcServer(
        grpcCleanup,
        luceneServerConfiguration,
        folder,
        null,
        luceneServerConfiguration.getIndexDir(),
        testIndex,
        luceneServerConfiguration.getPort());
  }

  @Test
  public void testSearchQueryText() {
    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(LuceneServerTest.RETRIEVED_VALUES)
                    .setQueryText("SECOND")
                    .build());

    assertEquals(1, searchResponse.getTotalHits().getValue());
    assertEquals(1, searchResponse.getHitsList().size());
    SearchResponse.Hit hit = searchResponse.getHits(0);
    String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
    assertEquals("2", docId);
    LuceneServerTest.checkHits(hit);
  }

  @Test
  public void testSearchV2QueryText() throws InvalidProtocolBufferException {
    Any anyResponse =
        grpcServer
            .getBlockingStub()
            .searchV2(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(LuceneServerTest.RETRIEVED_VALUES)
                    .setQueryText("SECOND")
                    .build());
    assertTrue(anyResponse.is(SearchResponse.class));
    SearchResponse searchResponse = anyResponse.unpack(SearchResponse.class);
    assertEquals(1, searchResponse.getTotalHits().getValue());
    assertEquals(1, searchResponse.getHitsList().size());
    SearchResponse.Hit hit = searchResponse.getHits(0);
    String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
    assertEquals("2", docId);
    LuceneServerTest.checkHits(hit);
  }

  @Test
  public void testSearchQueryResponseCompression() {
    List<String> compressionTypes = Arrays.asList("", "identity", "gzip", "lz4", "invalid");

    for (String compressionType : compressionTypes) {
      SearchResponse searchResponse =
          grpcServer
              .getBlockingStub()
              .search(
                  SearchRequest.newBuilder()
                      .setIndexName(grpcServer.getTestIndex())
                      .setStartHit(0)
                      .setTopHits(10)
                      .addAllRetrieveFields(LuceneServerTest.RETRIEVED_VALUES)
                      .setQueryText("SECOND")
                      .setResponseCompression(compressionType)
                      .build());

      assertEquals(1, searchResponse.getTotalHits().getValue());
      assertEquals(1, searchResponse.getHitsList().size());
      SearchResponse.Hit hit = searchResponse.getHits(0);
      String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
      assertEquals("2", docId);
      LuceneServerTest.checkHits(hit);
    }
  }

  @Test
  public void testSearchV2ResponseCompression() throws InvalidProtocolBufferException {
    List<String> compressionTypes = Arrays.asList("", "identity", "gzip", "lz4", "invalid");

    for (String compressionType : compressionTypes) {
      Any anyResponse =
          grpcServer
              .getBlockingStub()
              .searchV2(
                  SearchRequest.newBuilder()
                      .setIndexName(grpcServer.getTestIndex())
                      .setStartHit(0)
                      .setTopHits(10)
                      .addAllRetrieveFields(LuceneServerTest.RETRIEVED_VALUES)
                      .setQueryText("SECOND")
                      .setResponseCompression(compressionType)
                      .build());
      assertTrue(anyResponse.is(SearchResponse.class));
      SearchResponse searchResponse = anyResponse.unpack(SearchResponse.class);
      assertEquals(1, searchResponse.getTotalHits().getValue());
      assertEquals(1, searchResponse.getHitsList().size());
      SearchResponse.Hit hit = searchResponse.getHits(0);
      String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
      assertEquals("2", docId);
      LuceneServerTest.checkHits(hit);
    }
  }

  @Test
  public void testSearchBooleanQuery() {
    Query query =
        Query.newBuilder()
            .setBooleanQuery(
                BooleanQuery.newBuilder()
                    .addClauses(
                        BooleanClause.newBuilder()
                            .setQuery(
                                Query.newBuilder()
                                    .setPhraseQuery(
                                        PhraseQuery.newBuilder()
                                            .setSlop(0)
                                            .setField("vendor_name")
                                            .addTerms("first")
                                            .addTerms("again")
                                            .build())
                                    .build())
                            .setOccur(BooleanClause.Occur.MUST)
                            .build())
                    .build())
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("1", docId);
          LuceneServerTest.checkHits(hit);
        };

    testQuery(query, responseTester);
  }

  @Test
  public void testSearchBooleanQueryMustNotBeOne() {
    Query query =
        Query.newBuilder()
            .setBooleanQuery(
                BooleanQuery.newBuilder()
                    .addClauses(
                        BooleanClause.newBuilder()
                            .setQuery(
                                Query.newBuilder()
                                    .setPhraseQuery(
                                        PhraseQuery.newBuilder()
                                            .setSlop(0)
                                            .setField("vendor_name")
                                            .addTerms("first")
                                            .addTerms("again")
                                            .build())
                                    .build())
                            .setOccur(BooleanClause.Occur.MUST_NOT)
                            .build())
                    .build())
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", docId);
          LuceneServerTest.checkHits(hit);
        };

    testQuery(query, responseTester);
  }

  @Test
  public void testSearchPhraseQuery() {
    Query query =
        Query.newBuilder()
            .setPhraseQuery(
                PhraseQuery.newBuilder()
                    .setSlop(0)
                    .setField("vendor_name")
                    .addTerms("second")
                    .addTerms("again")
                    .build())
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", docId);
          LuceneServerTest.checkHits(hit);
        };

    testQuery(query, responseTester);
  }

  @Test
  public void testSearchFunctionScoreQuery() {
    Query query =
        Query.newBuilder()
            .setFunctionScoreQuery(
                FunctionScoreQuery.newBuilder()
                    .setScript(
                        Script.newBuilder().setLang("js").setSource("sqrt(4) * count").build())
                    .setQuery(
                        Query.newBuilder()
                            .setPhraseQuery(
                                PhraseQuery.newBuilder()
                                    .setSlop(0)
                                    .setField("vendor_name")
                                    .addTerms("second")
                                    .addTerms("again")
                                    .build()))
                    .build())
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", docId);
          assertEquals(14.0, hit.getScore(), 0.0);
          LuceneServerTest.checkHits(hit);
        };

    testQuery(query, responseTester);
  }

  @Test
  public void testSearchFunctionScoreQueryNoInnerQuery() {
    Query query =
        Query.newBuilder()
            .setFunctionScoreQuery(
                FunctionScoreQuery.newBuilder()
                    .setScript(
                        Script.newBuilder().setLang("js").setSource("sqrt(4) * count").build())
                    .build())
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(2, searchResponse.getTotalHits().getValue());
          assertEquals(2, searchResponse.getHitsList().size());

          SearchResponse.Hit firstHit = searchResponse.getHits(0);
          String firstDocId = firstHit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", firstDocId);
          assertEquals(14.0, firstHit.getScore(), 0.0);
          LuceneServerTest.checkHits(firstHit);

          SearchResponse.Hit secondHit = searchResponse.getHits(1);
          String secondDocId =
              secondHit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("1", secondDocId);
          assertEquals(6.0, secondHit.getScore(), 0.0);
          LuceneServerTest.checkHits(secondHit);
        };

    testQuery(query, responseTester);
  }

  @Test
  public void testSearchFunctionFilterQuery() {
    Query query =
        Query.newBuilder()
            .setFunctionFilterQuery(
                FunctionFilterQuery.newBuilder()
                    .setScript(Script.newBuilder().setLang("js").setSource("count - 4").build()))
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", docId);
          LuceneServerTest.checkHits(hit);
        };

    testQuery(query, responseTester);
  }

  @Test
  public void testSearchQueryRescorer() {
    Query firstPassQuery =
        Query.newBuilder()
            .setFunctionScoreQuery(
                FunctionScoreQuery.newBuilder()
                    .setScript(Script.newBuilder().setLang("js").setSource("5").build())
                    .setQuery(
                        Query.newBuilder()
                            .setPhraseQuery(
                                PhraseQuery.newBuilder()
                                    .setSlop(0)
                                    .setField("vendor_name")
                                    .addTerms("second")
                                    .addTerms("again")
                                    .build()))
                    .build())
            .build();

    Query rescoreQuery =
        Query.newBuilder()
            .setFunctionScoreQuery(
                FunctionScoreQuery.newBuilder()
                    .setScript(Script.newBuilder().setLang("js").setSource("10").build())
                    .setQuery(
                        Query.newBuilder()
                            .setPhraseQuery(
                                PhraseQuery.newBuilder()
                                    .setSlop(0)
                                    .setField("vendor_name")
                                    .addTerms("second")
                                    .addTerms("again")
                                    .build()))
                    .build())
            .build();

    Rescorer queryRescorer =
        Rescorer.newBuilder()
            .setWindowSize(2)
            .setName("query_rescorer")
            .setQueryRescorer(
                QueryRescorer.newBuilder()
                    .setQueryWeight(1.0)
                    .setRescoreQueryWeight(4.0)
                    .setRescoreQuery(rescoreQuery))
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(
              Set.of("query_rescorer"),
              searchResponse.getDiagnostics().getRescorersTimeMsMap().keySet());
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", docId);
          // score should be equal 1.0 * firstPassScore + 4.0 * secondPassScore = 45
          // the scores are hardcoded in query. firstPass: 5, secondsPass: 10
          assertEquals(45, hit.getScore(), 0);
          LuceneServerTest.checkHits(hit);
        };

    testQueryWithRescorers(firstPassQuery, List.of(queryRescorer), responseTester);

    // Rescorers are not required to have a name
    Rescorer queryRescorerNoName =
        Rescorer.newBuilder()
            .setWindowSize(2)
            .setQueryRescorer(
                QueryRescorer.newBuilder()
                    .setQueryWeight(1.0)
                    .setRescoreQueryWeight(4.0)
                    .setRescoreQuery(rescoreQuery))
            .build();

    Consumer<SearchResponse> responseTesterNoName =
        searchResponse -> {
          assertEquals(
              Set.of("rescorer_0"),
              searchResponse.getDiagnostics().getRescorersTimeMsMap().keySet());
        };

    testQueryWithRescorers(firstPassQuery, List.of(queryRescorerNoName), responseTesterNoName);
  }

  @Test
  public void testSearchTermQuery() {
    TermQuery textQuery =
        TermQuery.newBuilder().setField("vendor_name").setTextValue("second").build();
    TermQuery intQuery = TermQuery.newBuilder().setField("count").setIntValue(7).build();
    TermQuery longQuery = TermQuery.newBuilder().setField("long_field").setLongValue(16).build();
    TermQuery floatQuery =
        TermQuery.newBuilder().setField("float_field").setFloatValue(200.02f).build();
    TermQuery doubleQuery =
        TermQuery.newBuilder().setField("double_field").setDoubleValue(2.01).build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", docId);
          LuceneServerTest.checkHits(hit);
        };

    for (TermQuery termQuery :
        new TermQuery[] {textQuery, intQuery, longQuery, floatQuery, doubleQuery}) {
      Query query = Query.newBuilder().setTermQuery(termQuery).build();
      testQuery(query, responseTester);
    }
  }

  @Test
  public void testSearchTermInSetQuery() {
    TermInSetQuery textQuery =
        TermInSetQuery.newBuilder()
            .setField("vendor_name")
            .setTextTerms(
                TermInSetQuery.TextTerms.newBuilder().addAllTerms(List.of("first", "second")))
            .build();
    TermInSetQuery intQuery =
        TermInSetQuery.newBuilder()
            .setField("count")
            .setIntTerms(TermInSetQuery.IntTerms.newBuilder().addAllTerms(List.of(3, 7)))
            .build();
    TermInSetQuery longQuery =
        TermInSetQuery.newBuilder()
            .setField("long_field")
            .setLongTerms(TermInSetQuery.LongTerms.newBuilder().addAllTerms(List.of(12L, 16L)))
            .build();
    TermInSetQuery floatQuery =
        TermInSetQuery.newBuilder()
            .setField("float_field")
            .setFloatTerms(
                TermInSetQuery.FloatTerms.newBuilder().addAllTerms(List.of(100.01f, 200.02f)))
            .build();
    TermInSetQuery doubleQuery =
        TermInSetQuery.newBuilder()
            .setField("double_field")
            .setDoubleTerms(
                TermInSetQuery.DoubleTerms.newBuilder().addAllTerms(List.of(1.01, 2.01)))
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(2, searchResponse.getTotalHits().getValue());
          assertEquals(2, searchResponse.getHitsList().size());
          SearchResponse.Hit firstHit = searchResponse.getHits(0);
          LuceneServerTest.checkHits(firstHit);
          SearchResponse.Hit secondHit = searchResponse.getHits(1);
          LuceneServerTest.checkHits(secondHit);
        };

    for (TermInSetQuery termInSetQuery :
        new TermInSetQuery[] {textQuery, intQuery, longQuery, floatQuery, doubleQuery}) {
      Query query = Query.newBuilder().setTermInSetQuery(termInSetQuery).build();
      testQuery(query, responseTester);
    }
  }

  @Test
  public void testSearchDisjunctionMaxQuery() {
    Query query =
        Query.newBuilder()
            .setDisjunctionMaxQuery(
                DisjunctionMaxQuery.newBuilder()
                    .addDisjuncts(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("vendor_name")
                                    .setTextValue("second")))
                    .addDisjuncts(
                        Query.newBuilder()
                            .setFunctionScoreQuery(
                                FunctionScoreQuery.newBuilder()
                                    .setScript(
                                        Script.newBuilder()
                                            .setLang("js")
                                            .setSource("sqrt(4) * count")
                                            .build())
                                    .setQuery(
                                        Query.newBuilder()
                                            .setTermQuery(
                                                TermQuery.newBuilder()
                                                    .setField("vendor_name")
                                                    .setTextValue("second")))))
                    .setTieBreakerMultiplier(0))
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", docId);
          assertEquals(14.0, hit.getScore(), 0.0);
          LuceneServerTest.checkHits(hit);
        };

    testQuery(query, responseTester);
  }

  @Test
  public void testSearchMatchQuery() {
    Query query =
        Query.newBuilder()
            .setMatchQuery(
                MatchQuery.newBuilder()
                    .setField("vendor_name")
                    .setQuery("SECOND again")
                    .setOperator(MatchOperator.MUST))
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", docId);
          LuceneServerTest.checkHits(hit);
        };

    testQuery(query, responseTester);
  }

  @Test
  public void testSearchMatchQueryWithFuzzyParamsAndMinShouldMatch1() {
    Query query =
        Query.newBuilder()
            .setMatchQuery(
                MatchQuery.newBuilder()
                    .setField("vendor_name")
                    .setQuery("SECOND")
                    .setMinimumNumberShouldMatch(1)
                    .setFuzzyParams(FuzzyParams.newBuilder().setMaxEdits(2).setMaxExpansions(100))
                    .setOperator(MatchOperator.SHOULD))
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", docId);
          LuceneServerTest.checkHits(hit);
        };

    testQuery(query, responseTester);
  }

  @Test
  public void testSearchMatchQueryEmptyAfterAnalysis() {
    Query query =
        Query.newBuilder()
            .setMatchQuery(
                MatchQuery.newBuilder()
                    .setField("vendor_name")
                    .setQuery("////????")
                    .setOperator(MatchOperator.MUST))
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(0, searchResponse.getTotalHits().getValue());
          assertEquals(0, searchResponse.getHitsList().size());
        };

    testQuery(query, responseTester);
  }

  @Test
  public void testSearchMatchQueryFuzzyCustomAnalyzer() {
    Query query =
        Query.newBuilder()
            .setMatchQuery(
                MatchQuery.newBuilder()
                    .setField("vendor_name")
                    .setQuery("<br> SEND agn </br>")
                    .setFuzzyParams(
                        FuzzyParams.newBuilder()
                            .setMaxEdits(2)
                            .setPrefixLength(2)
                            .setMaxExpansions(1))
                    .setAnalyzer(getTestAnalyzer())
                    .setOperator(MatchOperator.MUST))
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", docId);
          LuceneServerTest.checkHits(hit);
        };

    testQuery(query, responseTester);
  }

  @Test
  public void testSearchMatchPhraseQuery() {
    Query query =
        Query.newBuilder()
            .setMatchPhraseQuery(
                MatchPhraseQuery.newBuilder()
                    .setField("vendor_name")
                    .setQuery("SECOND second")
                    .setSlop(1))
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", docId);
          LuceneServerTest.checkHits(hit);
        };

    testQuery(query, responseTester);
  }

  @Test
  public void testSearchMatchPhraseQueryEmptyAfterAnalysis() {
    Query query =
        Query.newBuilder()
            .setMatchPhraseQuery(
                MatchPhraseQuery.newBuilder()
                    .setField("vendor_name")
                    .setQuery("/?/ ?//?")
                    .setSlop(1))
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(0, searchResponse.getTotalHits().getValue());
          assertEquals(0, searchResponse.getHitsList().size());
        };

    testQuery(query, responseTester);
  }

  @Test
  public void testSearchMatchPhraseQueryCustomAnalyzer() {
    Query query =
        Query.newBuilder()
            .setMatchPhraseQuery(
                MatchPhraseQuery.newBuilder()
                    .setField("vendor_name")
                    .setQuery("<br> SECOND again </br>")
                    .setAnalyzer(getTestAnalyzer())
                    .setSlop(1))
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", docId);
          LuceneServerTest.checkHits(hit);
        };

    testQuery(query, responseTester);
  }

  @Test
  public void testSearchMultiMatchQuery() {
    Query query =
        Query.newBuilder()
            .setMultiMatchQuery(
                MultiMatchQuery.newBuilder()
                    .addFields("vendor_name")
                    .addFields("description")
                    .setQuery("SEnD")
                    .setOperator(MatchOperator.MUST)
                    .setFuzzyParams(
                        FuzzyParams.newBuilder()
                            .setPrefixLength(2)
                            .setMaxEdits(2)
                            .setMaxExpansions(3)))
            .build();

    Query queryWithAnalyzer =
        Query.newBuilder(query)
            .setMultiMatchQuery(
                MultiMatchQuery.newBuilder(query.getMultiMatchQuery())
                    .setQuery("<body> SEnD </body>")
                    .setAnalyzer(getTestAnalyzer()))
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", docId);
          LuceneServerTest.checkHits(hit);
        };

    testQuery(query, responseTester);
    testQuery(queryWithAnalyzer, responseTester);
  }

  @Test
  public void testSearchRangeQuery() {
    RangeQuery intQuery =
        RangeQuery.newBuilder().setField("count").setLower("5").setUpper("10").build();

    RangeQuery longQuery =
        RangeQuery.newBuilder().setField("long_field").setLower("15").setUpper("19").build();

    RangeQuery floatQuery =
        RangeQuery.newBuilder()
            .setField("float_field")
            .setLower("200.01")
            .setUpper("200.03")
            .build();

    RangeQuery doubleQuery =
        RangeQuery.newBuilder()
            .setField("double_field")
            .setLower("2.001")
            .setUpper("2.012")
            .build();

    RangeQuery dateQuery =
        RangeQuery.newBuilder()
            .setField("date")
            .setLower("2019-12-11 05:40:31")
            .setUpper("2020-05-05 10:31:56")
            .build();

    Consumer<SearchResponse> responseTester =
        searchResponse -> {
          assertEquals(1, searchResponse.getTotalHits().getValue());
          assertEquals(1, searchResponse.getHitsList().size());
          SearchResponse.Hit hit = searchResponse.getHits(0);
          String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
          assertEquals("2", docId);
          LuceneServerTest.checkHits(hit);
        };

    for (RangeQuery rangeQuery : List.of(intQuery, longQuery, floatQuery, doubleQuery, dateQuery)) {
      Query query = Query.newBuilder().setRangeQuery(rangeQuery).build();

      testQuery(query, responseTester);
    }
  }

  /**
   * Search with the query and then test the response. Additional test with boost will also be
   * performed on the query.
   *
   * @param query Query to test with
   * @param responseTester {@link Consumer} that tests a {@link SearchResponse}
   */
  private void testQuery(Query query, Consumer<SearchResponse> responseTester) {
    SearchResponse searchResponse = grpcServer.getBlockingStub().search(buildSearchRequest(query));
    responseTester.accept(searchResponse);
    testWithBoost(query, searchResponse);
  }

  private void testQueryWithRescorers(
      Query query, List<Rescorer> rescorers, Consumer<SearchResponse> responseTester) {
    SearchResponse searchResponse =
        grpcServer.getBlockingStub().search(buildSearchRequestWithRescorers(query, rescorers));
    responseTester.accept(searchResponse);
  }

  private Analyzer getTestAnalyzer() {
    return Analyzer.newBuilder()
        .setCustom(
            CustomAnalyzer.newBuilder()
                .setDefaultMatchVersion("LATEST")
                .addCharFilters(NameAndParams.newBuilder().setName("htmlstrip"))
                .setTokenizer(NameAndParams.newBuilder().setName("standard"))
                .addTokenFilters(NameAndParams.newBuilder().setName("lowercase")))
        .build();
  }

  private SearchRequest buildSearchRequest(Query query) {
    return SearchRequest.newBuilder()
        .setIndexName(grpcServer.getTestIndex())
        .setStartHit(0)
        .setTopHits(10)
        .addAllRetrieveFields(LuceneServerTest.RETRIEVED_VALUES)
        .setQuery(query)
        .build();
  }

  private SearchRequest buildSearchRequestWithRescorers(Query query, List<Rescorer> rescorers) {
    return SearchRequest.newBuilder()
        .setIndexName(grpcServer.getTestIndex())
        .setStartHit(0)
        .setTopHits(10)
        .addAllRetrieveFields(LuceneServerTest.RETRIEVED_VALUES)
        .setQuery(query)
        .addAllRescorers(rescorers)
        .build();
  }

  private void testWithBoost(Query originalQuery, SearchResponse searchResponse) {
    int boost = 2;
    Query boostedQuery = Query.newBuilder(originalQuery).setBoost(boost).build();
    SearchResponse searchResponseBoosted =
        grpcServer.getBlockingStub().search(buildSearchRequest(boostedQuery));

    assertEquals(
        searchResponse.getTotalHits().getValue(), searchResponseBoosted.getTotalHits().getValue());
    assertEquals(searchResponse.getHitsList().size(), searchResponseBoosted.getHitsList().size());

    verifyDiagnostics(searchResponse.getDiagnostics());

    for (int i = 0; i < searchResponse.getHitsCount(); i++) {
      SearchResponse.Hit hit = searchResponse.getHits(i);
      SearchResponse.Hit boostedHit = searchResponseBoosted.getHits(i);

      assertEquals(boost * hit.getScore(), boostedHit.getScore(), 0.0);

      SearchResponse.Hit hitWithoutScore = SearchResponse.Hit.newBuilder(hit).setScore(0).build();
      SearchResponse.Hit boostedHitWithoutScore =
          SearchResponse.Hit.newBuilder(boostedHit).setScore(0).build();
      assertEquals(hitWithoutScore, boostedHitWithoutScore);
    }
  }

  private void verifyDiagnostics(SearchResponse.Diagnostics diagnostics) {
    assertTrue(diagnostics.getFirstPassSearchTimeMs() > 0);
    assertTrue(diagnostics.getGetFieldsTimeMs() > 0);
  }
}
