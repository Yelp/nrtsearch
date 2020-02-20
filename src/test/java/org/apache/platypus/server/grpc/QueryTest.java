/*
 * Copyright 2020 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.platypus.server.grpc;

import io.grpc.testing.GrpcCleanupRule;
import org.apache.platypus.server.LuceneServerTestConfigurationFactory;
import org.apache.platypus.server.config.LuceneServerConfiguration;
import org.apache.platypus.server.luceneserver.GlobalState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.platypus.server.grpc.GrpcServer.rmDir;
import static org.apache.platypus.server.grpc.LuceneServerTest.checkHits;
import static org.junit.Assert.assertEquals;

public class QueryTest {

    public static final List<String> RETRIEVED_VALUES = Arrays.asList("doc_id", "license_no", "vendor_name", "vendor_name_atom", "count");
    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the
     * end of test.
     */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    /**
     * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private GrpcServer grpcServer;

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
    public void setUp() throws Exception {
        grpcServer = setUpGrpcServer();
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
        // 2 docs addDocuments
        testAddDocs.addDocuments();
        //manual refresh
        grpcServer.getBlockingStub().refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    }

    private GrpcServer setUpGrpcServer() throws IOException {
        String testIndex = "test_index";
        LuceneServerConfiguration luceneServerConfiguration = LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE);
        GlobalState globalState = new GlobalState(luceneServerConfiguration);
        return new GrpcServer(grpcCleanup, folder, false, globalState, luceneServerConfiguration.getIndexDir(), testIndex, globalState.getPort());
    }

    @Test
    public void testSearchQueryText() {
        SearchResponse searchResponse = grpcServer.getBlockingStub().search(SearchRequest.newBuilder()
                .setIndexName(grpcServer.getTestIndex())
                .setStartHit(0)
                .setTopHits(10)
                .addAllRetrieveFields(RETRIEVED_VALUES)
                .setQueryText("SECOND")
                .build());

        assertEquals(1, searchResponse.getTotalHits());
        assertEquals(1, searchResponse.getHitsList().size());
        SearchResponse.Hit hit = searchResponse.getHits(0);
        String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
        assertEquals("2", docId);
        checkHits(hit);
    }

    @Test
    public void testSearchBooleanQuery() {
        Supplier<Query> querySupplier = () -> Query.newBuilder()
                .setQueryType(QueryType.BOOLEAN_QUERY)
                .setBooleanQuery(BooleanQuery.newBuilder()
                        .addClauses(BooleanClause.newBuilder()
                                .setQuery(Query.newBuilder()
                                        .setQueryType(QueryType.PHRASE_QUERY)
                                        .setPhraseQuery(PhraseQuery.newBuilder()
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

        Consumer<SearchResponse> responseTester = searchResponse -> {
            assertEquals(1, searchResponse.getTotalHits());
            assertEquals(1, searchResponse.getHitsList().size());
            SearchResponse.Hit hit = searchResponse.getHits(0);
            String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
            assertEquals("1", docId);
            checkHits(hit);
        };

        testQuery(querySupplier, responseTester);
    }

    @Test
    public void testSearchPhraseQuery() {
        Supplier<Query> querySupplier = () -> Query.newBuilder()
                .setQueryType(QueryType.PHRASE_QUERY)
                .setPhraseQuery(PhraseQuery.newBuilder()
                        .setSlop(0)
                        .setField("vendor_name")
                        .addTerms("second")
                        .addTerms("again")
                        .build())
                .build();

        Consumer<SearchResponse> responseTester = searchResponse -> {
            assertEquals(1, searchResponse.getTotalHits());
            assertEquals(1, searchResponse.getHitsList().size());
            SearchResponse.Hit hit = searchResponse.getHits(0);
            String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
            assertEquals("2", docId);
            checkHits(hit);
        };

        testQuery(querySupplier, responseTester);
    }

    @Test
    public void testSearchFunctionScoreQuery() {
        Supplier<Query> querySupplier = () -> Query.newBuilder()
                .setQueryType(QueryType.FUNCTION_SCORE_QUERY)
                .setFunctionScoreQuery(FunctionScoreQuery.newBuilder()
                        .setFunction("sqrt(4) * count")
                        .setQuery(Query.newBuilder()
                                .setQueryType(QueryType.PHRASE_QUERY)
                                .setPhraseQuery(PhraseQuery.newBuilder()
                                        .setSlop(0)
                                        .setField("vendor_name")
                                        .addTerms("second")
                                        .addTerms("again")
                                        .build()))
                        .build())
                .build();

        Consumer<SearchResponse> responseTester = searchResponse -> {
            assertEquals(1, searchResponse.getTotalHits());
            assertEquals(1, searchResponse.getHitsList().size());
            SearchResponse.Hit hit = searchResponse.getHits(0);
            String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
            assertEquals("2", docId);
            assertEquals(14.0, hit.getScore(), 0.0);
            checkHits(hit);
        };

        testQuery(querySupplier, responseTester);
    }

    @Test
    public void testSearchTermQuery() {
        Supplier<Query> querySupplier = () -> Query.newBuilder()
                .setQueryType(QueryType.TERM_QUERY)
                .setTermQuery(TermQuery.newBuilder()
                        .setField("vendor_name")
                        .setTerm("second"))
                .build();

        Consumer<SearchResponse> responseTester = searchResponse -> {
            assertEquals(1, searchResponse.getTotalHits());
            assertEquals(1, searchResponse.getHitsList().size());
            SearchResponse.Hit hit = searchResponse.getHits(0);
            String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
            assertEquals("2", docId);
            checkHits(hit);
        };

        testQuery(querySupplier, responseTester);
    }

    @Test
    public void testSearchTermInSetQuery() {
        Supplier<Query> querySupplier = () -> Query.newBuilder()
                .setQueryType(QueryType.TERM_IN_SET_QUERY)
                .setTermInSetQuery(TermInSetQuery.newBuilder()
                        .setField("vendor_name")
                        .addTerms("second")
                        .addTerms("first"))
                .build();

        Consumer<SearchResponse> responseTester = searchResponse -> {
            assertEquals(2, searchResponse.getTotalHits());
            assertEquals(2, searchResponse.getHitsList().size());
            SearchResponse.Hit firstHit = searchResponse.getHits(0);
            checkHits(firstHit);
            SearchResponse.Hit secondHit = searchResponse.getHits(1);
            checkHits(secondHit);
        };

        testQuery(querySupplier, responseTester);
    }

    @Test
    public void testSearchDisjunctionMaxQuery() {
        Supplier<Query> querySupplier = () -> Query.newBuilder()
                .setQueryType(QueryType.DISJUNCTION_MAX)
                .setDisjunctionMaxQuery(DisjunctionMaxQuery.newBuilder()
                        .addDisjuncts(Query.newBuilder()
                                .setQueryType(QueryType.TERM_QUERY)
                                .setTermQuery(TermQuery.newBuilder()
                                        .setField("vendor_name")
                                        .setTerm("second")))
                        .addDisjuncts(Query.newBuilder()
                                .setQueryType(QueryType.FUNCTION_SCORE_QUERY)
                                .setFunctionScoreQuery(FunctionScoreQuery.newBuilder()
                                        .setFunction("sqrt(4) * count")
                                        .setQuery(Query.newBuilder()
                                                .setQueryType(QueryType.TERM_QUERY)
                                                .setTermQuery(TermQuery.newBuilder()
                                                        .setField("vendor_name")
                                                        .setTerm("second")))))
                        .setTieBreakerMultiplier(0))
                .build();

        Consumer<SearchResponse> responseTester = searchResponse -> {
            assertEquals(1, searchResponse.getTotalHits());
            assertEquals(1, searchResponse.getHitsList().size());
            SearchResponse.Hit hit = searchResponse.getHits(0);
            String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
            assertEquals("2", docId);
            assertEquals(14.0, hit.getScore(), 0.0);
            checkHits(hit);
        };

        testQuery(querySupplier, responseTester);
    }

    @Test
    public void testSearchMatchQuery() {
        Supplier<Query> querySupplier = () -> Query.newBuilder()
                .setQueryType(QueryType.MATCH)
                .setMatchQuery(MatchQuery.newBuilder()
                        .setField("vendor_name")
                        .setQuery("SECOND again")
                        .setOperator(MatchOperator.MUST))
                .build();

        Consumer<SearchResponse> responseTester = searchResponse -> {
            assertEquals(1, searchResponse.getTotalHits());
            assertEquals(1, searchResponse.getHitsList().size());
            SearchResponse.Hit hit = searchResponse.getHits(0);
            String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
            assertEquals("2", docId);
            checkHits(hit);
        };

        testQuery(querySupplier, responseTester);
    }

    @Test
    public void testSearchMatchQueryFuzzyCustomAnalyzer() {
        Supplier<Query> querySupplier = () -> Query.newBuilder()
                .setQueryType(QueryType.MATCH)
                .setMatchQuery(MatchQuery.newBuilder()
                        .setField("vendor_name")
                        .setQuery("<br> SEND agn </br>")
                        .setFuzzyParams(FuzzyParams.newBuilder()
                                .setMaxEdits(2)
                                .setPrefixLength(2)
                                .setMaxExpansions(1))
                        .setAnalyzer(getTestAnalyzer())
                        .setOperator(MatchOperator.MUST))
                .build();

        Consumer<SearchResponse> responseTester = searchResponse -> {
            assertEquals(1, searchResponse.getTotalHits());
            assertEquals(1, searchResponse.getHitsList().size());
            SearchResponse.Hit hit = searchResponse.getHits(0);
            String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
            assertEquals("2", docId);
            checkHits(hit);
        };

        testQuery(querySupplier, responseTester);
    }

    @Test
    public void testSearchMatchPhraseQuery() {
        Supplier<Query> querySupplier = () -> Query.newBuilder()
                .setQueryType(QueryType.MATCH_PHRASE)
                .setMatchPhraseQuery(MatchPhraseQuery.newBuilder()
                        .setField("vendor_name")
                        .setQuery("SECOND second")
                        .setSlop(1)).build();

        Consumer<SearchResponse> responseTester = searchResponse -> {
            assertEquals(1, searchResponse.getTotalHits());
            assertEquals(1, searchResponse.getHitsList().size());
            SearchResponse.Hit hit = searchResponse.getHits(0);
            String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
            assertEquals("2", docId);
            checkHits(hit);
        };

        testQuery(querySupplier, responseTester);
    }

    @Test
    public void testSearchMatchPhraseQueryCustomAnalyzer() {
        Supplier<Query> querySupplier = () -> Query.newBuilder()
                .setQueryType(QueryType.MATCH_PHRASE)
                .setMatchPhraseQuery(MatchPhraseQuery.newBuilder()
                        .setField("vendor_name")
                        .setQuery("<br> SECOND again </br>")
                        .setAnalyzer(getTestAnalyzer())
                        .setSlop(1)).build();

        Consumer<SearchResponse> responseTester = searchResponse -> {
            assertEquals(1, searchResponse.getTotalHits());
            assertEquals(1, searchResponse.getHitsList().size());
            SearchResponse.Hit hit = searchResponse.getHits(0);
            String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
            assertEquals("2", docId);
            checkHits(hit);
        };

        testQuery(querySupplier, responseTester);
    }

    /**
     * Search with the query and then test the response. Additional test with boost will also be performed on the query.
     *
     * @param querySupplier {@link Supplier} that provides the query to test with
     * @param responseTester {@link Consumer} that tests a {@link SearchResponse}
     */
    private void testQuery(Supplier<Query> querySupplier, Consumer<SearchResponse> responseTester) {
        SearchResponse searchResponse = grpcServer.getBlockingStub()
                .search(buildSearchRequest(querySupplier.get()));
        responseTester.accept(searchResponse);
        testWithBoost(querySupplier, searchResponse);
    }

    private Analyzer getTestAnalyzer() {
        return Analyzer.newBuilder()
                .setCustom(CustomAnalyzer.newBuilder()
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
                .addAllRetrieveFields(RETRIEVED_VALUES)
                .setQuery(query)
                .build();
    }

    private void testWithBoost(Supplier<Query> querySupplier, SearchResponse searchResponse) {
        int boost = 2;
        Query boostedQuery = Query.newBuilder(querySupplier.get())
                .setBoost(boost)
                .build();
        SearchResponse searchResponseBoosted = grpcServer.getBlockingStub()
                .search(buildSearchRequest(boostedQuery));
        for (int i = 0; i < searchResponse.getHitsCount(); i++) {
            SearchResponse.Hit hit = searchResponse.getHits(i);
            SearchResponse.Hit boostedHit = searchResponseBoosted.getHits(i);

            assertEquals(boost * hit.getScore(), boostedHit.getScore(), 0.0);
        }
    }

}
