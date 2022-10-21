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

import static com.yelp.nrtsearch.server.grpc.GrpcServer.rmDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.protobuf.ByteString;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.locationtech.spatial4j.io.GeohashUtils;

@RunWith(JUnit4.class)
@Ignore("Do we still want to support this?")
public class SuggestTest {

  enum Suggester {
    INFIX,
    ANALYZING,
    FUZZY,
    COMPLETION_INFIX,
    FUZZY_INFIX
  }

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
  static Path tempFile;

  @After
  public void tearDown() throws IOException {
    grpcServer.getGlobalState().close();
    rmDir(Paths.get(grpcServer.getIndexDir()).getParent());
    tempFile = null;
  }

  @Before
  public void setUp() throws IOException {
    LuceneServerConfiguration luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE, folder.getRoot());
    grpcServer =
        new GrpcServer(
            grpcCleanup,
            luceneServerConfiguration,
            folder,
            null,
            luceneServerConfiguration.getIndexDir(),
            "test_index",
            luceneServerConfiguration.getPort());
    Path tempDir = folder.newFolder("TestSuggest").toPath();
    tempFile = tempDir.resolve("suggest.in");
  }

  @Test
  public void testInfixSuggest() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("15\u001flove lost\u001ffoobar\n");
    out.close();

    BuildSuggestResponse response =
        sendBuildSuggest("suggest2", false, false, false, false, Suggester.INFIX);
    assertEquals(1, response.getCount());

    for (int i = 0; i < 2; i++) {
      assertOneHighlightOnIndexLoveLost("lost", "suggest2", 15, "foobar");
      assertMultipleHighlightsOnIndexLoveLost("lo", "suggest2", 15, "foobar");

      // commit state and indexes
      grpcServer
          .getBlockingStub()
          .commit(CommitRequest.newBuilder().setIndexName("test_index").build());
      // mimic bounce server to make sure suggestions survive restart
      grpcServer
          .getBlockingStub()
          .stopIndex(StopIndexRequest.newBuilder().setIndexName("test_index").build());
      grpcServer
          .getBlockingStub()
          .startIndex(StartIndexRequest.newBuilder().setIndexName("test_index").build());
    }
  }

  @Test
  public void testCompletionInfixSuggest() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);

    try (FileOutputStream fos = new FileOutputStream(tempFile.toFile())) {
      NrtsearchIndex.newBuilder()
          .setUniqueId(1)
          .addAllSearchTexts(List.of("home depot", "depot"))
          .setScore(1L)
          .setPayload(ByteString.copyFrom("payload".getBytes()))
          .addAllContexts(List.of("c1", "c2"))
          .build()
          .writeDelimitedTo(fos);
    }

    BuildSuggestResponse response =
        sendBuildSuggest("suggest2", true, true, true, false, Suggester.COMPLETION_INFIX);
    assertEquals(1, response.getCount());

    for (int i = 0; i < 2; i++) {
      // 1 transposition and this matches the infix of "depot":
      SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
      suggestLookupBuilder.setText("de");
      suggestLookupBuilder.setSuggestName("suggest2");
      suggestLookupBuilder.setIndexName("test_index");
      SuggestLookupResponse suggestResponse =
          grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
      List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();

      assertNotNull(results);
      assertEquals(1, results.size());
      assertEquals(1, results.get(0).getWeight());
      assertEquals("1", results.get(0).getKey());
      assertEquals("payload", results.get(0).getPayload());

      // commit state and indexes
      grpcServer
          .getBlockingStub()
          .commit(CommitRequest.newBuilder().setIndexName("test_index").build());
      // mimic bounce server to make sure suggestions survive restart
      grpcServer
          .getBlockingStub()
          .stopIndex(StopIndexRequest.newBuilder().setIndexName("test_index").build());
      grpcServer
          .getBlockingStub()
          .startIndex(StartIndexRequest.newBuilder().setIndexName("test_index").build());
    }
  }

  @Test
  public void testFuzzyInfixSuggest() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);

    try (FileOutputStream fos = new FileOutputStream(tempFile.toFile())) {
      NrtsearchIndex.newBuilder()
          .setUniqueId(1)
          .addAllSearchTexts(List.of("home depot", "depot"))
          .setScore(1L)
          .setPayload(ByteString.copyFrom("payload".getBytes()))
          .addAllContexts(List.of("c1", "c2"))
          .build()
          .writeDelimitedTo(fos);
    }

    BuildSuggestResponse response =
        sendBuildSuggest("suggest2", true, true, true, false, Suggester.FUZZY_INFIX);
    assertEquals(1, response.getCount());

    for (int i = 0; i < 2; i++) {
      // 1 transposition and this matches the infix of "depot":
      SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
      suggestLookupBuilder.setText("dip");
      suggestLookupBuilder.setSuggestName("suggest2");
      suggestLookupBuilder.setIndexName("test_index");
      SuggestLookupResponse suggestResponse =
          grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
      List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();

      assertNotNull(results);
      assertEquals(1, results.size());
      assertEquals(1, results.get(0).getWeight());
      assertEquals("1", results.get(0).getKey());
      assertEquals("payload", results.get(0).getPayload());

      // commit state and indexes
      grpcServer
          .getBlockingStub()
          .commit(CommitRequest.newBuilder().setIndexName("test_index").build());
      // mimic bounce server to make sure suggestions survive restart
      grpcServer
          .getBlockingStub()
          .stopIndex(StopIndexRequest.newBuilder().setIndexName("test_index").build());
      grpcServer
          .getBlockingStub()
          .startIndex(StartIndexRequest.newBuilder().setIndexName("test_index").build());
    }
  }

  private BuildSuggestResponse sendBuildSuggest(
      String suggestName,
      boolean hasContexts,
      boolean hasPayload,
      boolean hasMultiSearchTexts,
      boolean isUpdate,
      Suggester suggester) {
    BuildSuggestRequest.Builder buildSuggestRequestBuilder = BuildSuggestRequest.newBuilder();
    buildSuggestRequestBuilder.setSuggestName(suggestName);
    buildSuggestRequestBuilder.setIndexName("test_index");
    if (suggester.equals(Suggester.INFIX)) {
      buildSuggestRequestBuilder.setInfixSuggester(
          InfixSuggester.newBuilder().setAnalyzer("default").build());
    } else if (suggester.equals(Suggester.ANALYZING)) {
      buildSuggestRequestBuilder.setAnalyzingSuggester(
          AnalyzingSuggester.newBuilder().setAnalyzer("default").build());
    } else if (suggester.equals(Suggester.FUZZY)) {
      buildSuggestRequestBuilder.setFuzzySuggester(
          FuzzySuggester.newBuilder().setAnalyzer("default").build());
    } else if (suggester.equals(Suggester.COMPLETION_INFIX)) {
      buildSuggestRequestBuilder.setCompletionInfixSuggester(
          CompletionInfixSuggester.newBuilder().setAnalyzer("default").build());
    } else if (suggester.equals(Suggester.FUZZY_INFIX)) {
      buildSuggestRequestBuilder.setFuzzyInfixSuggester(
          FuzzyInfixSuggester.newBuilder().setAnalyzer("default").setTranspositions(true).build());
    }
    buildSuggestRequestBuilder.setLocalSource(
        SuggestLocalSource.newBuilder()
            .setLocalFile(tempFile.toAbsolutePath().toString())
            .setHasContexts(hasContexts)
            .setHasPayload(hasPayload)
            .setHasMultiSearchText(hasMultiSearchTexts)
            .build());
    BuildSuggestResponse response;
    if (isUpdate) {
      response = grpcServer.getBlockingStub().updateSuggest(buildSuggestRequestBuilder.build());
    } else {
      response = grpcServer.getBlockingStub().buildSuggest(buildSuggestRequestBuilder.build());
    }
    return response;
  }

  private void assertMultipleHighlightsOnIndexLoveLost(
      String text, String suggestName, long weight, String payload) {
    SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
    suggestLookupBuilder.setText(text);
    suggestLookupBuilder.setSuggestName(suggestName);
    suggestLookupBuilder.setIndexName("test_index");
    suggestLookupBuilder.setHighlight(true);
    SuggestLookupResponse suggestResponse =
        grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
    List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();
    assertEquals(weight, results.get(0).getWeight());
    SuggestLookupHighlight suggestLookupHighLight = results.get(0).getSuggestLookupHighlight();
    assertEquals(5, suggestLookupHighLight.getOneHighlightList().size());
    assertEquals("lo", suggestLookupHighLight.getOneHighlight(0).getText());
    assertEquals(true, suggestLookupHighLight.getOneHighlight(0).getIsHit());
    assertEquals("ve", suggestLookupHighLight.getOneHighlight(1).getText());
    assertEquals(false, suggestLookupHighLight.getOneHighlight(1).getIsHit());
    assertEquals(" ", suggestLookupHighLight.getOneHighlight(2).getText());
    assertEquals(false, suggestLookupHighLight.getOneHighlight(2).getIsHit());
    assertEquals("lo", suggestLookupHighLight.getOneHighlight(3).getText());
    assertEquals(true, suggestLookupHighLight.getOneHighlight(3).getIsHit());
    assertEquals("st", suggestLookupHighLight.getOneHighlight(4).getText());
    assertEquals(false, suggestLookupHighLight.getOneHighlight(4).getIsHit());
    assertEquals(payload, results.get(0).getPayload());
  }

  private void assertOneHighlightOnIndexLoveLost(
      String text, String suggestName, long weight, String payload) {
    SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
    suggestLookupBuilder.setText(text);
    suggestLookupBuilder.setSuggestName(suggestName);
    suggestLookupBuilder.setIndexName("test_index");
    suggestLookupBuilder.setHighlight(true);
    SuggestLookupResponse suggestResponse =
        grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
    List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();
    assertEquals(weight, results.get(0).getWeight());
    SuggestLookupHighlight suggestLookupHighLight = results.get(0).getSuggestLookupHighlight();
    assertEquals(2, suggestLookupHighLight.getOneHighlightList().size());
    assertEquals("love ", suggestLookupHighLight.getOneHighlight(0).getText());
    assertEquals(false, suggestLookupHighLight.getOneHighlight(0).getIsHit());
    assertEquals("lost", suggestLookupHighLight.getOneHighlight(1).getText());
    assertEquals(true, suggestLookupHighLight.getOneHighlight(1).getIsHit());
    assertEquals(payload, results.get(0).getPayload());
  }

  @Test
  public void testInfixSuggestNRT() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("15\u001flove lost\u001ffoobar\n");
    out.close();

    BuildSuggestResponse response =
        sendBuildSuggest("suggestnrt", false, false, false, false, Suggester.INFIX);
    assertEquals(1, response.getCount());

    assertOneHighlightOnIndexLoveLost("lost", "suggestnrt", 15, "foobar");
    assertMultipleHighlightsOnIndexLoveLost("lo", "suggestnrt", 15, "foobar");

    // Now update the suggestions:
    fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
    out = new BufferedWriter(fstream);
    out.write("10\u001flove lost\u001ffoobaz\n");
    out.write("20\u001flove found\u001ffooboo\n");
    out.close();

    response = sendBuildSuggest("suggestnrt", false, false, false, true, Suggester.INFIX);
    assertEquals(2, response.getCount());

    for (int i = 0; i < 2; i++) {
      assertOneHighlightOnIndexLoveLost("lost", "suggestnrt", 10, "foobaz");

      SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
      suggestLookupBuilder.setText("lo");
      suggestLookupBuilder.setSuggestName("suggestnrt");
      suggestLookupBuilder.setIndexName("test_index");
      suggestLookupBuilder.setHighlight(true);
      SuggestLookupResponse suggestResponse =
          grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
      List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();
      assertEquals(20, results.get(0).getWeight());
      assertEquals("fooboo", results.get(0).getPayload());

      SuggestLookupHighlight suggestLookupHighLight = results.get(0).getSuggestLookupHighlight();
      SuggestLookupHighlight expected =
          SuggestLookupHighlight.newBuilder()
              .addOneHighlight(OneHighlight.newBuilder().setText("lo").setIsHit(true).build())
              .addOneHighlight(OneHighlight.newBuilder().setText("ve").setIsHit(false).build())
              .addOneHighlight(OneHighlight.newBuilder().setText(" ").setIsHit(false).build())
              .addOneHighlight(OneHighlight.newBuilder().setText("found").setIsHit(false).build())
              .build();
      assertEquals(expected, suggestLookupHighLight);

      assertEquals(10, results.get(1).getWeight());
      assertEquals("foobaz", results.get(1).getPayload());
      suggestLookupHighLight = results.get(1).getSuggestLookupHighlight();
      expected =
          SuggestLookupHighlight.newBuilder()
              .addOneHighlight(OneHighlight.newBuilder().setText("lo").setIsHit(true).build())
              .addOneHighlight(OneHighlight.newBuilder().setText("ve").setIsHit(false).build())
              .addOneHighlight(OneHighlight.newBuilder().setText(" ").setIsHit(false).build())
              .addOneHighlight(OneHighlight.newBuilder().setText("lo").setIsHit(true).build())
              .addOneHighlight(OneHighlight.newBuilder().setText("st").setIsHit(false).build())
              .build();
      assertEquals(expected, suggestLookupHighLight);

      // commit state and indexes
      grpcServer
          .getBlockingStub()
          .commit(CommitRequest.newBuilder().setIndexName("test_index").build());
      // mimic bounce server to make sure suggestions survive restart
      grpcServer
          .getBlockingStub()
          .stopIndex(StopIndexRequest.newBuilder().setIndexName("test_index").build());
      grpcServer
          .getBlockingStub()
          .startIndex(StartIndexRequest.newBuilder().setIndexName("test_index").build());
    }
  }

  @Test
  public void testAnalyzingSuggest() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("5\u001flucene\u001ffoobar\n");
    out.write("10\u001flucifer\u001ffoobar\n");
    out.write("15\u001flove\u001ffoobar\n");
    out.write("5\u001ftheories take time\u001ffoobar\n");
    out.write("5\u001fthe time is now\u001ffoobar\n");
    out.close();
    BuildSuggestResponse response =
        sendBuildSuggest("suggest", false, false, false, false, Suggester.ANALYZING);
    assertEquals(5, response.getCount());

    for (int i = 0; i < 2; i++) {
      SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
      suggestLookupBuilder.setText("l");
      suggestLookupBuilder.setSuggestName("suggest");
      suggestLookupBuilder.setIndexName("test_index");
      SuggestLookupResponse suggestResponse =
          grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
      List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();

      assertEquals(3, results.size());

      assertEquals("love", results.get(0).getKey());
      assertEquals(15, results.get(0).getWeight());
      assertEquals("foobar", results.get(0).getPayload());

      assertEquals("lucifer", results.get(1).getKey());
      assertEquals(10, results.get(1).getWeight());
      assertEquals("foobar", results.get(1).getPayload());

      assertEquals("lucene", results.get(2).getKey());
      assertEquals(5, results.get(2).getWeight());
      assertEquals("foobar", results.get(2).getPayload());

      suggestLookupBuilder = SuggestLookupRequest.newBuilder();
      suggestLookupBuilder.setText("the");
      suggestLookupBuilder.setSuggestName("suggest");
      suggestLookupBuilder.setIndexName("test_index");
      suggestResponse = grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
      results = suggestResponse.getResultsList();

      assertEquals(2, results.size());

      assertEquals("theories take time", results.get(0).getKey());
      assertEquals(5, results.get(0).getWeight());
      assertEquals("foobar", results.get(0).getPayload());

      // commit state and indexes
      grpcServer
          .getBlockingStub()
          .commit(CommitRequest.newBuilder().setIndexName("test_index").build());
      // mimic bounce server to make sure suggestions survive restart
      grpcServer
          .getBlockingStub()
          .stopIndex(StopIndexRequest.newBuilder().setIndexName("test_index").build());
      grpcServer
          .getBlockingStub()
          .startIndex(StartIndexRequest.newBuilder().setIndexName("test_index").build());
    }
  }

  @Test
  public void testFuzzySuggest() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("15\u001flove lost\u001ffoobar\n");
    out.close();

    BuildSuggestResponse result =
        sendBuildSuggest("suggest3", false, false, false, false, Suggester.FUZZY);
    assertEquals(1, result.getCount());

    for (int i = 0; i < 2; i++) {
      // 1 transposition and this is prefix of "love":
      SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
      suggestLookupBuilder.setText("lvo");
      suggestLookupBuilder.setSuggestName("suggest3");
      suggestLookupBuilder.setIndexName("test_index");
      SuggestLookupResponse suggestResponse =
          grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
      List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();

      assertEquals(15, results.get(0).getWeight());
      assertEquals("love lost", results.get(0).getKey());
      assertEquals("foobar", results.get(0).getPayload());

      // commit state and indexes
      grpcServer
          .getBlockingStub()
          .commit(CommitRequest.newBuilder().setIndexName("test_index").build());
      // mimic bounce server to make sure suggestions survive restart
      grpcServer
          .getBlockingStub()
          .stopIndex(StopIndexRequest.newBuilder().setIndexName("test_index").build());
      grpcServer
          .getBlockingStub()
          .startIndex(StartIndexRequest.newBuilder().setIndexName("test_index").build());
    }
  }

  /** Build a suggest, pulling suggestions/weights/payloads from stored fields. */
  @Test
  public void testFromStoredFields() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(Mode.STANDALONE, 0, false, "registerFieldsSuggest.json");
    AddDocumentResponse addDocumentResponse = testAddDocs.addDocuments("addSuggestDocs.csv");
    BuildSuggestRequest.Builder buildSuggestRequestBuilder = BuildSuggestRequest.newBuilder();
    buildSuggestRequestBuilder.setSuggestName("suggest");
    buildSuggestRequestBuilder.setIndexName("test_index");
    buildSuggestRequestBuilder.setAnalyzingSuggester(
        AnalyzingSuggester.newBuilder().setAnalyzer("default").build());
    buildSuggestRequestBuilder.setNonLocalSource(
        SuggestNonLocalSource.newBuilder()
            .setIndexGen(Long.valueOf(addDocumentResponse.getGenId()))
            .setSuggestField("text")
            .setWeightField("weight")
            .setPayloadField("payload")
            .build());
    BuildSuggestResponse response =
        grpcServer.getBlockingStub().buildSuggest(buildSuggestRequestBuilder.build());
    // nocommit count isn't returned for stored fields source:
    assertEquals(2, response.getCount());

    for (int i = 0; i < 2; i++) {
      SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
      suggestLookupBuilder.setText("the");
      suggestLookupBuilder.setSuggestName("suggest");
      suggestLookupBuilder.setIndexName("test_index");
      SuggestLookupResponse suggestResponse =
          grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
      List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();

      assertEquals(2, results.get(0).getWeight());
      assertEquals("the dog barks", results.get(0).getKey());
      assertEquals("payload2", results.get(0).getPayload());
      assertEquals(1, results.get(1).getWeight());
      assertEquals("the cat meows", results.get(1).getKey());
      assertEquals("payload1", results.get(1).getPayload());

      // commit state and indexes
      grpcServer
          .getBlockingStub()
          .commit(CommitRequest.newBuilder().setIndexName("test_index").build());
      // mimic bounce server to make sure suggestions survive restart
      grpcServer
          .getBlockingStub()
          .stopIndex(StopIndexRequest.newBuilder().setIndexName("test_index").build());
      grpcServer
          .getBlockingStub()
          .startIndex(StartIndexRequest.newBuilder().setIndexName("test_index").build());
    }
  }

  /**
   * Build a suggest, pulling suggestions/payloads from stored fields, and weight from an expression
   */
  @Test
  public void testFromStoredFieldsWithWeightExpression() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(
            Mode.STANDALONE, 0, false, "registerFieldsSuggestExpr.json");
    AddDocumentResponse addDocumentResponse = testAddDocs.addDocuments("addSuggestDocsExpr.csv");
    BuildSuggestRequest.Builder buildSuggestRequestBuilder = BuildSuggestRequest.newBuilder();
    buildSuggestRequestBuilder.setSuggestName("suggest");
    buildSuggestRequestBuilder.setIndexName("test_index");
    buildSuggestRequestBuilder.setAnalyzingSuggester(
        AnalyzingSuggester.newBuilder().setAnalyzer("default").build());
    buildSuggestRequestBuilder.setNonLocalSource(
        SuggestNonLocalSource.newBuilder()
            .setIndexGen(Long.valueOf(addDocumentResponse.getGenId()))
            .setSuggestField("text")
            .setWeightExpression("-negWeight")
            .setPayloadField("payload")
            .build());
    BuildSuggestResponse response =
        grpcServer.getBlockingStub().buildSuggest(buildSuggestRequestBuilder.build());
    assertEquals(2, response.getCount());

    for (int i = 0; i < 2; i++) {
      // nocommit count isn't returned for stored fields source:
      SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
      suggestLookupBuilder.setText("the");
      suggestLookupBuilder.setSuggestName("suggest");
      suggestLookupBuilder.setIndexName("test_index");
      SuggestLookupResponse suggestResponse =
          grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
      List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();

      assertEquals(2, results.get(0).getWeight());
      assertEquals("the dog barks", results.get(0).getKey());
      assertEquals("payload2", results.get(0).getPayload());
      assertEquals(1, results.get(1).getWeight());
      assertEquals("the cat meows", results.get(1).getKey());
      assertEquals("payload1", results.get(1).getPayload());

      // commit state and indexes
      grpcServer
          .getBlockingStub()
          .commit(CommitRequest.newBuilder().setIndexName("test_index").build());
      // mimic bounce server to make sure suggestions survive restart
      grpcServer
          .getBlockingStub()
          .stopIndex(StopIndexRequest.newBuilder().setIndexName("test_index").build());
      grpcServer
          .getBlockingStub()
          .startIndex(StartIndexRequest.newBuilder().setIndexName("test_index").build());
    }
  }

  @Test
  public void testInfixSuggesterWithContexts() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);

    Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("15\u001flove lost\u001ffoobar\u001flucene\n");
    out.close();

    BuildSuggestResponse response =
        sendBuildSuggest("suggest", true, false, false, false, Suggester.INFIX);
    assertEquals(1, response.getCount());

    for (int i = 0; i < 2; i++) {
      SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
      suggestLookupBuilder.setText("lov");
      suggestLookupBuilder.setSuggestName("suggest");
      suggestLookupBuilder.setIndexName("test_index");
      suggestLookupBuilder.setHighlight(true);
      suggestLookupBuilder.addContexts("lucene");
      SuggestLookupResponse suggestResponse =
          grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
      List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();
      assertEquals(1, results.size());
      assertEquals(15, results.get(0).getWeight());
      assertEquals("foobar", results.get(0).getPayload());
      // commit state and indexes
      grpcServer
          .getBlockingStub()
          .commit(CommitRequest.newBuilder().setIndexName("test_index").build());
      // mimic bounce server to make sure suggestions survive restart
      grpcServer
          .getBlockingStub()
          .stopIndex(StopIndexRequest.newBuilder().setIndexName("test_index").build());
      grpcServer
          .getBlockingStub()
          .startIndex(StartIndexRequest.newBuilder().setIndexName("test_index").build());
    }
  }

  @Test
  public void testInfixSuggesterWithContextsFromField() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(
            Mode.STANDALONE, 0, false, "registerFieldsSuggestWithContext.json");
    AddDocumentResponse addDocumentResponse =
        testAddDocs.addDocuments("addSuggestDocsWithContext.csv");
    BuildSuggestRequest.Builder buildSuggestRequestBuilder = BuildSuggestRequest.newBuilder();
    buildSuggestRequestBuilder.setSuggestName("suggest");
    buildSuggestRequestBuilder.setIndexName("test_index");
    buildSuggestRequestBuilder.setInfixSuggester(
        InfixSuggester.newBuilder().setAnalyzer("default").build());
    buildSuggestRequestBuilder.setNonLocalSource(
        SuggestNonLocalSource.newBuilder()
            .setIndexGen(Long.valueOf(addDocumentResponse.getGenId()))
            .setSuggestField("text")
            .setWeightField("weight")
            .setPayloadField("payload")
            .setContextField("context")
            .build());
    BuildSuggestResponse response =
        grpcServer.getBlockingStub().buildSuggest(buildSuggestRequestBuilder.build());
    // nocommit count isn't returned for stored fields source:
    assertEquals(2, response.getCount());

    for (int i = 0; i < 2; i++) {
      SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
      suggestLookupBuilder.setText("the");
      suggestLookupBuilder.setSuggestName("suggest");
      suggestLookupBuilder.setIndexName("test_index");
      suggestLookupBuilder.setHighlight(true);
      suggestLookupBuilder.addContexts("lucene");
      SuggestLookupResponse suggestResponse =
          grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
      List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();
      assertEquals(1, results.size());
      assertEquals(1, results.get(0).getWeight());
      assertEquals("payload1", results.get(0).getPayload());
      // commit state and indexes
      grpcServer
          .getBlockingStub()
          .commit(CommitRequest.newBuilder().setIndexName("test_index").build());
      // mimic bounce server to make sure suggestions survive restart
      grpcServer
          .getBlockingStub()
          .stopIndex(StopIndexRequest.newBuilder().setIndexName("test_index").build());
      grpcServer
          .getBlockingStub()
          .startIndex(StartIndexRequest.newBuilder().setIndexName("test_index").build());
    }
  }

  @Test
  public void testInfixSuggesterWithPayloadContextsAndSearchTexts() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(
            Mode.STANDALONE, 0, false, "registerFieldsSuggestWithContextAndSearchTexts.json");
    AddDocumentResponse addDocumentResponse =
        testAddDocs.addDocuments("addSuggestDocsWithContextsAndSearchTexts.csv");
    BuildSuggestRequest.Builder buildSuggestRequestBuilder = BuildSuggestRequest.newBuilder();
    buildSuggestRequestBuilder.setSuggestName("suggest");
    buildSuggestRequestBuilder.setIndexName("test_index");
    buildSuggestRequestBuilder.setCompletionInfixSuggester(
        CompletionInfixSuggester.newBuilder().setAnalyzer("default").build());
    buildSuggestRequestBuilder.setNonLocalSource(
        SuggestNonLocalSource.newBuilder()
            .setIndexGen(Long.valueOf(addDocumentResponse.getGenId()))
            .setSuggestField("text")
            .setWeightField("weight")
            .setPayloadField("payload")
            .setContextField("context")
            .setSearchTextField("search_text")
            .build());
    BuildSuggestResponse response =
        grpcServer.getBlockingStub().buildSuggest(buildSuggestRequestBuilder.build());
    // nocommit count isn't returned for stored fields source:
    assertEquals(2, response.getCount());

    for (int i = 0; i < 2; i++) {
      // Only one result is expected to return, given only one suggest item contains c2 context
      SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
      suggestLookupBuilder.setText("dog");
      suggestLookupBuilder.setSuggestName("suggest");
      suggestLookupBuilder.setIndexName("test_index");
      suggestLookupBuilder.addContexts("c2");
      SuggestLookupResponse suggestResponse =
          grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
      List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();
      assertEquals(1, results.size());
      assertEquals(2, results.get(0).getWeight());
      assertEquals("payload2", results.get(0).getPayload());
      assertEquals("dog barks", results.get(0).getKey());
      // commit state and indexes
      grpcServer
          .getBlockingStub()
          .commit(CommitRequest.newBuilder().setIndexName("test_index").build());
      // mimic bounce server to make sure suggestions survive restart
      grpcServer
          .getBlockingStub()
          .stopIndex(StopIndexRequest.newBuilder().setIndexName("test_index").build());
      grpcServer
          .getBlockingStub()
          .startIndex(StartIndexRequest.newBuilder().setIndexName("test_index").build());
    }

    // All results are expected to return, given only one suggest item contains c2 context
    SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
    suggestLookupBuilder.setText("dog");
    suggestLookupBuilder.setSuggestName("suggest");
    suggestLookupBuilder.setIndexName("test_index");
    suggestLookupBuilder.addContexts("c1");
    SuggestLookupResponse suggestResponse =
        grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
    List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();
    assertEquals(2, results.size());

    // Only one results with highest weight is expected to return
    // In this case, "dog barks" suggestion with weight of 2 is returned.
    suggestLookupBuilder = SuggestLookupRequest.newBuilder();
    suggestLookupBuilder.setText("dog");
    suggestLookupBuilder.setSuggestName("suggest");
    suggestLookupBuilder.setIndexName("test_index");
    suggestLookupBuilder.setCount(1);
    suggestResponse = grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
    results = suggestResponse.getResultsList();
    assertEquals(1, results.size());
    assertEquals(2, results.get(0).getWeight());
    assertEquals("dog barks", results.get(0).getKey());
  }

  @Test
  public void testSuggesterWithGeoHashContexts() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(
            Mode.STANDALONE, 0, false, "registerFieldsSuggestGeohash.json");

    AddDocumentResponse addDocumentResponse = new GeohashDocumentProducer().indexDocs(grpcServer);
    BuildSuggestRequest.Builder buildSuggestRequestBuilder = BuildSuggestRequest.newBuilder();
    buildSuggestRequestBuilder.setSuggestName("suggest");
    buildSuggestRequestBuilder.setIndexName("test_index");
    buildSuggestRequestBuilder.setInfixSuggester(
        InfixSuggester.newBuilder().setAnalyzer("default").build());
    buildSuggestRequestBuilder.setNonLocalSource(
        SuggestNonLocalSource.newBuilder()
            .setIndexGen(Long.valueOf(addDocumentResponse.getGenId()))
            .setSuggestField("text")
            .setWeightField("weight")
            .setPayloadField("payload")
            .setContextField("context")
            .build());
    BuildSuggestResponse response =
        grpcServer.getBlockingStub().buildSuggest(buildSuggestRequestBuilder.build());
    // nocommit count isn't returned for stored fields source:
    assertEquals(2, response.getCount());

    SuggestLookupRequest.Builder suggestLookupBuilder = SuggestLookupRequest.newBuilder();
    suggestLookupBuilder.setText("the");
    suggestLookupBuilder.setSuggestName("suggest");
    suggestLookupBuilder.setIndexName("test_index");
    suggestLookupBuilder.setHighlight(true);
    // only need san fran Geohashes
    List<String> geohashes = GeohashDocumentProducer.getGeoHashes(37.7749, -122.4194, 5, 7);
    for (String geohash : geohashes) {
      suggestLookupBuilder.addContexts(geohash);
    }
    SuggestLookupResponse suggestResponse =
        grpcServer.getBlockingStub().suggestLookup(suggestLookupBuilder.build());
    List<OneSuggestLookupResponse> results = suggestResponse.getResultsList();
    assertEquals(1, results.size());
    assertEquals(1, results.get(0).getWeight());
    assertEquals("payload1", results.get(0).getPayload());
  }

  private static class GeohashDocumentProducer {
    AddDocumentResponse addDocumentResponse;

    private static List<String> getGeoHashes(
        double latitude, double longitude, int minPrecision, int maxPrecision) {
      List<String> geohashes = new ArrayList<>();
      for (int i = minPrecision; i <= maxPrecision; i++) {
        geohashes.add(GeohashUtils.encodeLatLon(latitude, longitude, i));
      }
      return geohashes;
    }

    private AddDocumentResponse indexDocs(GrpcServer grpcServer) throws InterruptedException {
      List<String> sanFranGeohashes = getGeoHashes(37.7749, -122.4194, 5, 7);
      List<String> fremontGeohashes = getGeoHashes(37.5485, -121.9886, 5, 7);

      ArrayList<AddDocumentRequest> addDocumentRequests =
          new ArrayList<AddDocumentRequest>(
              Arrays.asList(
                  AddDocumentRequest.newBuilder()
                      .setIndexName("test_index")
                      .putFields(
                          "text",
                          AddDocumentRequest.MultiValuedField.newBuilder()
                              .addValue("the cat meows")
                              .build())
                      .putFields(
                          "weight",
                          AddDocumentRequest.MultiValuedField.newBuilder()
                              .addValue(String.valueOf(1))
                              .build())
                      .putFields(
                          "payload",
                          AddDocumentRequest.MultiValuedField.newBuilder()
                              .addValue("payload1")
                              .build())
                      .putFields(
                          "context",
                          AddDocumentRequest.MultiValuedField.newBuilder()
                              .addAllValue(sanFranGeohashes)
                              .build())
                      .build(),
                  AddDocumentRequest.newBuilder()
                      .setIndexName("test_index")
                      .putFields(
                          "text",
                          AddDocumentRequest.MultiValuedField.newBuilder()
                              .addValue("the dog barks")
                              .build())
                      .putFields(
                          "weight",
                          AddDocumentRequest.MultiValuedField.newBuilder()
                              .addValue(String.valueOf(2))
                              .build())
                      .putFields(
                          "payload",
                          AddDocumentRequest.MultiValuedField.newBuilder()
                              .addValue("payload2")
                              .build())
                      .putFields(
                          "context",
                          AddDocumentRequest.MultiValuedField.newBuilder()
                              .addAllValue(fremontGeohashes)
                              .build())
                      .build()));

      CountDownLatch finishLatch = new CountDownLatch(1);
      AddDocumentResponse addDocResponse;
      // observers responses from Server(should get one onNext and oneCompleted)
      StreamObserver<AddDocumentResponse> responseStreamObserver =
          new StreamObserver<AddDocumentResponse>() {
            @Override
            public void onNext(AddDocumentResponse value) {
              addDocumentResponse = value;
            }

            @Override
            public void onError(Throwable t) {
              finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
              finishLatch.countDown();
            }
          };

      // requestObserver sends requests to Server (one onNext per AddDocumentRequest and one
      // onCompleted)
      StreamObserver<AddDocumentRequest> requestObserver =
          grpcServer.getStub().addDocuments(responseStreamObserver);
      for (AddDocumentRequest addDocumentRequest : addDocumentRequests) {
        requestObserver.onNext(addDocumentRequest);
      }
      // Mark the end of requests
      requestObserver.onCompleted();
      // Receiving happens asynchronously, so block here 5 seconds
      if (!finishLatch.await(5, TimeUnit.SECONDS)) {
        throw new RuntimeException("addDocuments can not finish within 5 seconds");
      }

      return addDocumentResponse;
    }
  }
}
