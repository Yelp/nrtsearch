/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.LoggingHits;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.plugins.HitsLoggerPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class HitsLoggerTest extends ServerTestCase {
  private static String logMessage;

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  protected List<Plugin> getPlugins(LuceneServerConfiguration configuration) {
    return Collections.singletonList(new HitsLoggerTest.TestHitsLoggerPlugin());
  }

  @Override
  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/registerFieldsBasic.json");
  }

  @Override
  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();

    for (int docNum = 1; docNum < 11; docNum++) {
      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(docNum))
                      .build())
              .putFields(
                  "vendor_name",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue("vendor " + docNum)
                      .build())
              .putFields(
                  "long_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(2 + docNum))
                      .build())
              .build();
      docs.add(request);
    }
    addDocuments(docs.stream());
  }

  @Before
  public void cleanUpLogMessage() {
    logMessage = "";
  }

  static class TestHitsLoggerPlugin extends Plugin implements HitsLoggerPlugin {
    static class CustomHitsLogger implements HitsLogger {
      private final Map<String, Object> params;

      public CustomHitsLogger(Map<String, Object> params) {
        this.params = params;
      }

      @Override
      public void log(SearchContext context, List<SearchResponse.Hit.Builder> hits) {
        HitsLoggerTest.logMessage = "LOGGED ";

        for (SearchResponse.Hit.Builder hit : hits) {
          HitsLoggerTest.logMessage +=
              "doc_id: "
                  + hit.getFieldsMap().get("doc_id").getFieldValueList().get(0).getTextValue()
                  + ", ";
        }

        if (!params.isEmpty()) {
          HitsLoggerTest.logMessage += " " + params;
        }
      }
    }

    static class CustomHitsLogger2 implements HitsLogger {
      private final Map<String, Object> params;

      public CustomHitsLogger2(Map<String, Object> params) {
        this.params = params;
      }

      @Override
      public void log(SearchContext context, List<SearchResponse.Hit.Builder> hits) {
        HitsLoggerTest.logMessage = "LOGGED_2 ";

        for (SearchResponse.Hit.Builder hit : hits) {
          HitsLoggerTest.logMessage +=
              "doc_id: "
                  + hit.getFieldsMap().get("doc_id").getFieldValueList().get(0).getTextValue()
                  + ", ";
        }

        if (!params.isEmpty()) {
          HitsLoggerTest.logMessage += " " + params;
        }
      }
    }

    @Override
    public Map<String, HitsLoggerProvider<? extends HitsLogger>> getHitsLoggers() {
      return Map.of(
          "custom_logger", CustomHitsLogger::new,
          "custom_logger_2", CustomHitsLogger2::new);
    }
  }

  @Test
  public void testCustomHitsLoggerWithParam() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(2)
            .setStartHit(0)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("doc_id")
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(
                        TermQuery.newBuilder()
                            .setField("vendor_name")
                            .setTextValue("vendor")
                            .build())
                    .build())
            .setLoggingHits(
                LoggingHits.newBuilder()
                    .setName("custom_logger")
                    .setHitsToLog(2)
                    .setParams(
                        Struct.newBuilder()
                            .putFields(
                                "external_value", Value.newBuilder().setStringValue("abc").build()))
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    String expectedLogMessage = "LOGGED doc_id: 1, doc_id: 2,  {external_value=abc}";

    assertEquals(expectedLogMessage, HitsLoggerTest.logMessage);
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(2, response.getHitsCount());
  }

  @Test
  public void testCustomHitsLoggerWithoutParam() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(2)
            .setStartHit(0)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("doc_id")
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(
                        TermQuery.newBuilder()
                            .setField("vendor_name")
                            .setTextValue("vendor")
                            .build())
                    .build())
            .setLoggingHits(
                LoggingHits.newBuilder().setName("custom_logger_2").setHitsToLog(2).build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    String expectedLogMessage = "LOGGED_2 doc_id: 1, doc_id: 2, ";

    assertEquals(expectedLogMessage, HitsLoggerTest.logMessage);
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(2, response.getHitsCount());
  }

  @Test
  public void testResponseSizeReductionWithHitsToLogSameAsHitsCount() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(5)
            .setStartHit(0)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("doc_id")
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(
                        TermQuery.newBuilder()
                            .setField("vendor_name")
                            .setTextValue("vendor")
                            .build())
                    .build())
            .setLoggingHits(
                LoggingHits.newBuilder().setName("custom_logger").setHitsToLog(5).build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    String expectedLogMessage = "LOGGED doc_id: 1, doc_id: 2, doc_id: 3, doc_id: 4, doc_id: 5, ";

    assertEquals(expectedLogMessage, HitsLoggerTest.logMessage);
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(5, response.getHitsCount());
  }

  @Test
  public void testResponseSizeReductionWithHitsToLogGreaterThanHitsCount() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(1)
            .setStartHit(0)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("doc_id")
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(
                        TermQuery.newBuilder()
                            .setField("vendor_name")
                            .setTextValue("vendor")
                            .build())
                    .build())
            .setLoggingHits(
                LoggingHits.newBuilder()
                    .setName("custom_logger")
                    .setHitsToLog(2)
                    .setParams(
                        Struct.newBuilder()
                            .putFields(
                                "external_value", Value.newBuilder().setStringValue("abc").build()))
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    String expectedLogMessage = "LOGGED doc_id: 1, doc_id: 2,  {external_value=abc}";

    assertEquals(expectedLogMessage, HitsLoggerTest.logMessage);
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(1, response.getHitsCount());
  }

  @Test
  public void testResponseSizeReductionWithHitsToLogGreaterThanHitsCountAndTotalDocs() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(10)
            .setStartHit(0)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("doc_id")
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(
                        TermQuery.newBuilder()
                            .setField("vendor_name")
                            .setTextValue("vendor")
                            .build())
                    .build())
            .setLoggingHits(
                LoggingHits.newBuilder().setName("custom_logger").setHitsToLog(15).build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    String expectedLogMessage =
        "LOGGED doc_id: 1, doc_id: 2, doc_id: 3, doc_id: 4, doc_id: 5, doc_id: 6, doc_id: 7, doc_id: 8, doc_id: 9, doc_id: 10, ";

    assertEquals(expectedLogMessage, HitsLoggerTest.logMessage);
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(10, response.getHitsCount());
  }

  @Test
  public void testResponseSizeReductionWithHitsToLogLessThanHitsCount() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(5)
            .setStartHit(0)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("doc_id")
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(
                        TermQuery.newBuilder()
                            .setField("vendor_name")
                            .setTextValue("vendor")
                            .build())
                    .build())
            .setLoggingHits(
                LoggingHits.newBuilder().setName("custom_logger").setHitsToLog(3).build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    String expectedLogMessage = "LOGGED doc_id: 1, doc_id: 2, doc_id: 3, ";

    assertEquals(expectedLogMessage, HitsLoggerTest.logMessage);
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(5, response.getHitsCount());
  }

  @Test
  public void testResponseSizeReductionWithStartHitAndHitsToLogSameAsHitsCount() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(10)
            .setStartHit(5)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("doc_id")
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(
                        TermQuery.newBuilder()
                            .setField("vendor_name")
                            .setTextValue("vendor")
                            .build())
                    .build())
            .setLoggingHits(
                LoggingHits.newBuilder().setName("custom_logger").setHitsToLog(5).build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    String expectedLogMessage = "LOGGED doc_id: 6, doc_id: 7, doc_id: 8, doc_id: 9, doc_id: 10, ";

    assertEquals(expectedLogMessage, HitsLoggerTest.logMessage);
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(5, response.getHitsCount());
  }

  @Test
  public void testResponseSizeReductionWithStartHitAndHitsToLogGreaterThanHitsCountAndTotalDocs() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(10)
            .setStartHit(5)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("doc_id")
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(
                        TermQuery.newBuilder()
                            .setField("vendor_name")
                            .setTextValue("vendor")
                            .build())
                    .build())
            .setLoggingHits(
                LoggingHits.newBuilder().setName("custom_logger").setHitsToLog(6).build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    String expectedLogMessage = "LOGGED doc_id: 6, doc_id: 7, doc_id: 8, doc_id: 9, doc_id: 10, ";

    assertEquals(expectedLogMessage, HitsLoggerTest.logMessage);
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(5, response.getHitsCount());
  }

  @Test
  public void testResponseSizeReductionWithStartHitAndHitsToLogGreaterThanHitsCount() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(9)
            .setStartHit(4)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("doc_id")
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(
                        TermQuery.newBuilder()
                            .setField("vendor_name")
                            .setTextValue("vendor")
                            .build())
                    .build())
            .setLoggingHits(
                LoggingHits.newBuilder().setName("custom_logger").setHitsToLog(6).build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    String expectedLogMessage =
        "LOGGED doc_id: 5, doc_id: 6, doc_id: 7, doc_id: 8, doc_id: 9, doc_id: 10, ";

    assertEquals(expectedLogMessage, HitsLoggerTest.logMessage);
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(5, response.getHitsCount());
  }

  @Test
  public void testResponseSizeReductionWithStartHitAndHitsToLogLessThanHitsCount() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(10)
            .setStartHit(5)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("doc_id")
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(
                        TermQuery.newBuilder()
                            .setField("vendor_name")
                            .setTextValue("vendor")
                            .build())
                    .build())
            .setLoggingHits(
                LoggingHits.newBuilder().setName("custom_logger").setHitsToLog(3).build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    String expectedLogMessage = "LOGGED doc_id: 6, doc_id: 7, doc_id: 8, ";

    assertEquals(expectedLogMessage, HitsLoggerTest.logMessage);
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(5, response.getHitsCount());
  }

  @Test
  public void testLoggingWithZeroHitsToLog() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(1)
            .setStartHit(0)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("doc_id")
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(
                        TermQuery.newBuilder()
                            .setField("vendor_name")
                            .setTextValue("vendor")
                            .build())
                    .build())
            .setLoggingHits(
                LoggingHits.newBuilder()
                    .setName("custom_logger")
                    .setHitsToLog(0)
                    .setParams(
                        Struct.newBuilder()
                            .putFields(
                                "external_value", Value.newBuilder().setStringValue("abc").build()))
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);

    assertEquals("", HitsLoggerTest.logMessage);
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(1, response.getHitsCount());
  }

  @Test
  public void testLoggingTimeTaken() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(1)
            .setStartHit(0)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("doc_id")
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(
                        TermQuery.newBuilder()
                            .setField("vendor_name")
                            .setTextValue("vendor")
                            .build())
                    .build())
            .setLoggingHits(
                LoggingHits.newBuilder()
                    .setName("custom_logger")
                    .setHitsToLog(1)
                    .setParams(
                        Struct.newBuilder()
                            .putFields(
                                "external_value", Value.newBuilder().setStringValue("abc").build()))
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);

    assertTrue(response.getDiagnostics().getLoggingHitsTimeMs() > 0);
  }
}
