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
package com.yelp.nrtsearch.server;

import com.google.gson.Gson;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentResponse;
import com.yelp.nrtsearch.server.grpc.CreateIndexRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.LiveSettingsRequest;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import com.yelp.nrtsearch.server.grpc.NrtsearchClientBuilder;
import com.yelp.nrtsearch.server.grpc.RefreshRequest;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SettingsRequest;
import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.state.GlobalState;
import com.yelp.nrtsearch.test_utils.TestDocumentHelper;
import com.yelp.nrtsearch.test_utils.TestResourceHelper;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/**
 * Base class for tests that want an nrtsearch server that is setup once and used for all class
 * tests. Protected methods may be overridden to specify arbitrary indices and add documents.
 */
public class ServerTestCase {
  public static final String DEFAULT_TEST_INDEX = "test_index";

  /**
   * This rule ensures the temporary folder which maintains indexes is cleaned up after each test
   */
  @ClassRule public static final TemporaryFolder folder = new TemporaryFolder();

  private static TestServer testServer;
  private static boolean initialized = false;

  /** Adapter returned by {@link #getGrpcServer()} for backward compatibility with subclasses. */
  public static class GrpcServerAdapter {
    public LuceneServerGrpc.LuceneServerBlockingStub getBlockingStub() {
      return testServer.getClient().getBlockingStub();
    }

    public GlobalState getGlobalState() {
      return testServer.getGlobalState();
    }
  }

  private static final GrpcServerAdapter GRPC_SERVER_ADAPTER = new GrpcServerAdapter();

  /**
   * @deprecated Use {@link #getBlockingStub()} or {@link #getGlobalState()} directly.
   */
  @Deprecated
  public static GrpcServerAdapter getGrpcServer() {
    return GRPC_SERVER_ADAPTER;
  }

  public static LuceneServerGrpc.LuceneServerBlockingStub getBlockingStub() {
    return testServer.getClient().getBlockingStub();
  }

  public static PrometheusRegistry getPrometheusRegistry() {
    return testServer.getPrometheusRegistry();
  }

  public static GlobalState getGlobalState() {
    return testServer.getGlobalState();
  }

  public static FieldDefRequest getFieldsFromResourceFile(String resourceFileName)
      throws IOException {
    return TestResourceHelper.getFieldsFromResourceFile(resourceFileName);
  }

  public static FieldDefRequest getFieldsFromJson(String jsonStr) {
    return TestResourceHelper.getFieldsFromJson(jsonStr);
  }

  public static SearchRequest getSearchRequestFromResourceFile(String resourceFileName)
      throws IOException {
    return TestResourceHelper.getSearchRequestFromResourceFile(resourceFileName);
  }

  public static AddDocumentResponse addDocuments(Stream<AddDocumentRequest> requestStream)
      throws Exception {
    return TestDocumentHelper.addDocuments(testServer.getClient().getAsyncStub(), requestStream);
  }

  public static void addDocsFromResourceFile(String index, String resourceFile) throws Exception {
    Path filePath = Paths.get(ServerTestCase.class.getResource(resourceFile).toURI());
    Reader reader = Files.newBufferedReader(filePath);
    CSVParser csvParser =
        new CSVParser(
            reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build());
    Stream<AddDocumentRequest> requestStream =
        new NrtsearchClientBuilder.AddDocumentsClientBuilder(index, csvParser)
            .buildRequest(filePath);
    addDocuments(requestStream);
  }

  public static void addDocsFromJsonResourceFile(String index, String resourceFile)
      throws Exception {
    Path filePath = Paths.get(ServerTestCase.class.getResource(resourceFile).toURI());
    int maxBufferLen = 10;
    Stream<AddDocumentRequest> requestStream =
        new NrtsearchClientBuilder.AddJsonDocumentsClientBuilder(
                index, new Gson(), filePath, maxBufferLen)
            .buildRequest();
    addDocuments(requestStream);
  }

  @AfterClass
  public static void tearDownClass() {
    if (initialized) {
      TestServer.cleanupAll();
      initialized = false;
    }
  }

  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      setUpClass();
      initialized = true;
    }
  }

  public void setUpClass() throws Exception {
    testServer =
        TestServer.builder(folder)
            .withPlugins(getPlugins(null))
            .withAdditionalConfig(getExtraConfig())
            .build();
    initIndices();
  }

  protected void initIndices() throws Exception {
    for (String indexName : getIndices()) {
      LuceneServerGrpc.LuceneServerBlockingStub blockingStub =
          testServer.getClient().getBlockingStub();

      // create the index
      blockingStub.createIndex(CreateIndexRequest.newBuilder().setIndexName(indexName).build());

      // register fields
      blockingStub.registerFields(getIndexDef(indexName));

      // apply settings
      SettingsRequest settingsRequest = getSettings(indexName);
      if (settingsRequest != null) {
        blockingStub.settings(settingsRequest);
      }

      // apply live settings
      blockingStub.liveSettings(getLiveSettings(indexName));

      // start the index
      StartIndexRequest.Builder startIndexBuilder =
          StartIndexRequest.newBuilder().setIndexName(indexName);
      blockingStub.startIndex(startIndexBuilder.build());

      // add Docs
      initIndex(indexName);

      // refresh
      blockingStub.refresh(RefreshRequest.newBuilder().setIndexName(indexName).build());
    }
  }

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/registerFieldsBasic.json");
  }

  protected LiveSettingsRequest getLiveSettings(String name) {
    return LiveSettingsRequest.newBuilder().setIndexName(name).build();
  }

  protected SettingsRequest getSettings(String name) {
    return null;
  }

  protected void initIndex(String name) throws Exception {}

  protected List<Plugin> getPlugins(
      com.yelp.nrtsearch.server.config.NrtsearchConfig configuration) {
    return Collections.emptyList();
  }

  protected String getExtraConfig() {
    return "";
  }
}
