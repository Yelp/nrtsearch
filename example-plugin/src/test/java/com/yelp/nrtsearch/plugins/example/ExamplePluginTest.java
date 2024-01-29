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
package com.yelp.nrtsearch.plugins.example;

import static org.assertj.core.api.Assertions.assertThat;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.CustomRequest;
import com.yelp.nrtsearch.server.grpc.CustomResponse;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.plugins.Plugin;
import io.grpc.testing.GrpcCleanupRule;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.ClassRule;
import org.junit.Test;

public class ExamplePluginTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final ExamplePlugin examplePlugin = new ExamplePlugin(getConfig());

  @Override
  protected List<Plugin> getPlugins(LuceneServerConfiguration configuration) {
    return List.of(examplePlugin);
  }

  @Override
  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/register_fields.json");
  }

  @Override
  protected void initIndex(String name) throws Exception {
    AddDocumentRequest addDocumentRequest =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "field1",
                MultiValuedField.newBuilder().addValue("How to use Nrtsearch<br>").build())
            .build();
    AddDocumentRequest addDocumentRequest2 =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "field1",
                MultiValuedField.newBuilder().addValue("<head>How to create plugin</head>").build())
            .build();
    addDocuments(Stream.of(addDocumentRequest, addDocumentRequest2));
  }

  @Test
  public void testCustomRoute() {
    CustomResponse response =
        getGrpcServer()
            .getBlockingStub()
            .custom(
                CustomRequest.newBuilder()
                    .setId("custom_analyzers")
                    .setPath("get_available_analyzers")
                    .build());
    assertThat(response.getResponseOrThrow("available_analyzers")).isEqualTo("plugin_analyzer");
  }

  @Test
  public void testCustomAnalysis() throws IOException {
    String inputString = "Test plugiN<br>";
    String[] expectedTokens = new String[] {"test", "plugin"};
    Analyzer searchAnalyzer = getGlobalState().getIndex(DEFAULT_TEST_INDEX).searchAnalyzer;
    try (TokenStream ts = searchAnalyzer.tokenStream("field1", inputString)) {
      int index = 0;
      CharTermAttribute charTermAtt = ts.addAttribute(CharTermAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        assertThat(charTermAtt.toString()).isEqualTo(expectedTokens[index]);
        index++;
      }
    }
  }

  @Test
  public void testAnalysisForSearch() {
    SearchRequest searchRequest =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setStartHit(0)
            .setTopHits(5)
            .setQuery(
                Query.newBuilder()
                    .setMatchQuery(
                        MatchQuery.newBuilder().setField("field1").setQuery("nrtsearch").build())
                    .build())
            .build();

    SearchResponse response = getGrpcServer().getBlockingStub().search(searchRequest);
    assertThat(response.getHitsCount()).isEqualTo(1);
  }

  private static LuceneServerConfiguration getConfig() {
    String config = "nodeName: \"lucene_server_foo\"";
    return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
  }
}
