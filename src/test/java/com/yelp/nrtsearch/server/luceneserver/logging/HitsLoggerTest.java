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

import static org.junit.Assert.*;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.plugins.HitsLoggerPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.*;
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
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("1").build())
            .putFields(
                "vendor_name",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("first vendor").build())
            .putFields(
                "long_field",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("5").build())
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("2").build())
            .putFields(
                "vendor_name",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue("second vendor review")
                    .build())
            .putFields(
                "long_field",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("10").build())
            .build();
    docs.add(request);
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
        HitsLoggerTest.logMessage = "LOGGED " + hits.toString();

        if (!params.isEmpty()) {
          HitsLoggerTest.logMessage += " " + params;
        }
      }
    }

    @Override
    public HitsLoggerProvider<? extends HitsLogger> getHitsLogger() {
      return CustomHitsLogger::new;
    }
  }

  @Test
  public void testCustomHitsLoggerWithParam() {
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
                    .setParams(
                        Struct.newBuilder()
                            .putFields(
                                "external_value", Value.newBuilder().setStringValue("abc").build()))
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    String expectedLogMessage = "LOGGED " + List.of(response.getHits(0)) + " {external_value=abc}";

    assertEquals(expectedLogMessage, HitsLoggerTest.logMessage);
  }

  @Test
  public void testCustomHitsLoggerWithoutParam() {
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
            .setLoggingHits(LoggingHits.newBuilder().build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    String expectedLogMessage = "LOGGED " + List.of(response.getHits(0));

    assertEquals(expectedLogMessage, HitsLoggerTest.logMessage);
  }
}
