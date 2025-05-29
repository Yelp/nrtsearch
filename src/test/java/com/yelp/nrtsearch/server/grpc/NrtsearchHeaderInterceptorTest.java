/*
 * Copyright 2025 Yelp Inc.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.plugins.FetchTaskPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.search.FetchTaskProvider;
import com.yelp.nrtsearch.server.search.FetchTasks;
import com.yelp.nrtsearch.server.search.SearchContext;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Test;

public class NrtsearchHeaderInterceptorTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  static class HeaderInterceptorTestPlugin extends Plugin implements FetchTaskPlugin {
    static volatile Map<String, String> headers;

    static class HeaderTestTask implements FetchTasks.FetchTask {
      @Override
      public void processAllHits(
          SearchContext searchContext, List<SearchResponse.Hit.Builder> hits) {
        headers = ContextKeys.NRTSEARCH_HEADER_MAP.get();
      }
    }

    @Override
    public Map<String, FetchTaskProvider<? extends FetchTasks.FetchTask>> getFetchTasks() {
      return Map.of("header_test_task", params -> new HeaderTestTask());
    }
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/registerFieldsHeaders.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    AddDocumentRequest.MultiValuedField firstValue =
        AddDocumentRequest.MultiValuedField.newBuilder()
            .addValue("first vendor")
            .addValue("first again")
            .build();
    AddDocumentRequest.MultiValuedField secondValue =
        AddDocumentRequest.MultiValuedField.newBuilder()
            .addValue("second vendor")
            .addValue("second again")
            .build();
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("1").build())
            .putFields("vendor_name", firstValue)
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("2").build())
            .putFields("vendor_name", secondValue)
            .build();
    docs.add(request);
    addDocuments(docs.stream());
  }

  @Override
  protected List<Plugin> getPlugins(NrtsearchConfig configuration) {
    return Collections.singletonList(new HeaderInterceptorTestPlugin());
  }

  @Test
  public void testNoHeaders() {
    HeaderInterceptorTestPlugin.headers = null;
    SearchRequest searchRequest =
        SearchRequest.newBuilder()
            .setIndexName("test_index")
            .setTopHits(5)
            .setQuery(
                Query.newBuilder().setMatchAllQuery(MatchAllQuery.getDefaultInstance()).build())
            .addFetchTasks(FetchTask.newBuilder().setName("header_test_task").build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(searchRequest);
    assertNotNull(response);
    assertNotNull(HeaderInterceptorTestPlugin.headers);
    assertTrue(HeaderInterceptorTestPlugin.headers.isEmpty());
  }

  @Test
  public void testSingleHeader() {
    HeaderInterceptorTestPlugin.headers = null;
    SearchRequest searchRequest =
        SearchRequest.newBuilder()
            .setIndexName("test_index")
            .setTopHits(5)
            .setQuery(
                Query.newBuilder().setMatchAllQuery(MatchAllQuery.getDefaultInstance()).build())
            .addFetchTasks(FetchTask.newBuilder().setName("header_test_task").build())
            .build();

    Metadata metadata = new Metadata();
    Metadata.Key<String> headerKey1 =
        Metadata.Key.of("nrtsearch-prop1", Metadata.ASCII_STRING_MARSHALLER);
    metadata.put(headerKey1, "value1");

    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
            .search(searchRequest);
    assertNotNull(response);
    assertNotNull(HeaderInterceptorTestPlugin.headers);
    assertEquals(Map.of("nrtsearch-prop1", "value1"), HeaderInterceptorTestPlugin.headers);
  }

  @Test
  public void testMultipleHeaders() {
    HeaderInterceptorTestPlugin.headers = null;
    SearchRequest searchRequest =
        SearchRequest.newBuilder()
            .setIndexName("test_index")
            .setTopHits(5)
            .setQuery(
                Query.newBuilder().setMatchAllQuery(MatchAllQuery.getDefaultInstance()).build())
            .addFetchTasks(FetchTask.newBuilder().setName("header_test_task").build())
            .build();

    Metadata metadata = new Metadata();
    Metadata.Key<String> headerKey1 =
        Metadata.Key.of("nrtsearch-prop1", Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> headerKey2 =
        Metadata.Key.of("nrtsearch-prop2", Metadata.ASCII_STRING_MARSHALLER);
    metadata.put(headerKey1, "value1");
    metadata.put(headerKey2, "value2");

    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
            .search(searchRequest);
    assertNotNull(response);
    assertNotNull(HeaderInterceptorTestPlugin.headers);
    assertEquals(
        Map.of("nrtsearch-prop1", "value1", "nrtsearch-prop2", "value2"),
        HeaderInterceptorTestPlugin.headers);
  }

  @Test
  public void testHeadersWithoutPrefix() {
    HeaderInterceptorTestPlugin.headers = null;
    SearchRequest searchRequest =
        SearchRequest.newBuilder()
            .setIndexName("test_index")
            .setTopHits(5)
            .setQuery(
                Query.newBuilder().setMatchAllQuery(MatchAllQuery.getDefaultInstance()).build())
            .addFetchTasks(FetchTask.newBuilder().setName("header_test_task").build())
            .build();

    Metadata metadata = new Metadata();
    Metadata.Key<String> headerKey1 =
        Metadata.Key.of("nrtsearch-prop1", Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> headerKey2 =
        Metadata.Key.of("nrtsearch-prop2", Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> headerKey3 =
        Metadata.Key.of("other-prop3", Metadata.ASCII_STRING_MARSHALLER);
    metadata.put(headerKey1, "value1");
    metadata.put(headerKey2, "value2");
    metadata.put(headerKey3, "value3");

    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
            .search(searchRequest);
    assertNotNull(response);
    assertNotNull(HeaderInterceptorTestPlugin.headers);
    assertEquals(
        Map.of("nrtsearch-prop1", "value1", "nrtsearch-prop2", "value2"),
        HeaderInterceptorTestPlugin.headers);
  }

  @Test
  public void testIgnoreBinary() {
    HeaderInterceptorTestPlugin.headers = null;
    SearchRequest searchRequest =
        SearchRequest.newBuilder()
            .setIndexName("test_index")
            .setTopHits(5)
            .setQuery(
                Query.newBuilder().setMatchAllQuery(MatchAllQuery.getDefaultInstance()).build())
            .addFetchTasks(FetchTask.newBuilder().setName("header_test_task").build())
            .build();

    Metadata metadata = new Metadata();
    Metadata.Key<String> headerKey1 =
        Metadata.Key.of("nrtsearch-prop1", Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<byte[]> headerKey2 =
        Metadata.Key.of(
            "nrtsearch-prop2" + Metadata.BINARY_HEADER_SUFFIX, Metadata.BINARY_BYTE_MARSHALLER);
    metadata.put(headerKey1, "value1");
    metadata.put(headerKey2, new byte[] {1, 2, 3});

    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
            .search(searchRequest);
    assertNotNull(response);
    assertNotNull(HeaderInterceptorTestPlugin.headers);
    assertEquals(Map.of("nrtsearch-prop1", "value1"), HeaderInterceptorTestPlugin.headers);
  }
}
