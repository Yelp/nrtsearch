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
package com.yelp.nrtsearch.server.luceneserver;

import static com.yelp.nrtsearch.server.grpc.GrpcServer.rmDir;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentResponse;
import com.yelp.nrtsearch.server.grpc.CreateIndexRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.GrpcServer;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.RefreshRequest;
import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import com.yelp.nrtsearch.server.plugins.Plugin;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.prometheus.client.CollectorRegistry;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/**
 * Base class for tests that want a lucene server that is setup once and used for all class tests.
 * Protected methods may be overridden to specify arbitrary indices and add documents.
 */
public class ServerTestCase {
  public static final String DEFAULT_TEST_INDEX = "test_index";
  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  /**
   * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
   */
  @ClassRule public static final TemporaryFolder folder = new TemporaryFolder();

  private static GrpcServer grpcServer;
  private static CollectorRegistry collectorRegistry;
  private static GlobalState globalState;
  private static boolean initialized = false;

  public static GrpcServer getGrpcServer() {
    return grpcServer;
  }

  public static CollectorRegistry getCollectorRegistry() {
    return collectorRegistry;
  }

  public static GlobalState getGlobalState() {
    return globalState;
  }

  public static FieldDefRequest getFieldsFromResourceFile(String resourceFileName)
      throws IOException {
    InputStream fileStream = ServerTestCase.class.getResourceAsStream(resourceFileName);
    String jsonText =
        new BufferedReader(new InputStreamReader(fileStream, StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining(System.lineSeparator()));
    return getFieldsFromJson(jsonText);
  }

  public static FieldDefRequest getFieldsFromJson(String jsonStr) {
    FieldDefRequest.Builder fieldDefRequestBuilder = FieldDefRequest.newBuilder();
    try {
      JsonFormat.parser().merge(jsonStr, fieldDefRequestBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    return fieldDefRequestBuilder.build();
  }

  public static AddDocumentResponse addDocuments(Stream<AddDocumentRequest> requestStream)
      throws InterruptedException {
    CountDownLatch finishLatch = new CountDownLatch(1);
    // observers responses from Server(should get one onNext and oneCompleted)
    final AtomicReference<AddDocumentResponse> response = new AtomicReference<>();
    StreamObserver<AddDocumentResponse> responseStreamObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(AddDocumentResponse value) {
            response.set(value);
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
    // parse CSV into a stream of AddDocumentRequest
    try {
      requestStream.forEach(requestObserver::onNext);
    } catch (RuntimeException e) {
      // Cancel RPC
      requestObserver.onError(e);
      throw e;
    }
    // Mark the end of requests
    requestObserver.onCompleted();
    // Receiving happens asynchronously, so block here 20 seconds
    if (!finishLatch.await(20, TimeUnit.SECONDS)) {
      throw new RuntimeException("addDocuments can not finish within 20 seconds");
    }
    return response.get();
  }

  @AfterClass
  public static void tearDownClass() throws IOException {
    tearDownGrpcServer();
  }

  private static void tearDownGrpcServer() throws IOException {
    if (initialized) {
      grpcServer.getGlobalState().close();
      grpcServer.shutdown();
      rmDir(Paths.get(grpcServer.getIndexDir()).getParent());
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
    collectorRegistry = new CollectorRegistry();
    grpcServer = setUpGrpcServer(collectorRegistry);
    initIndices();
  }

  private GrpcServer setUpGrpcServer(CollectorRegistry collectorRegistry) throws IOException {
    String testIndex = "test_index";
    LuceneServerConfiguration luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE, folder.getRoot());
    globalState = new GlobalState(luceneServerConfiguration);
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
        getPlugins());
  }

  private void initIndices() throws Exception {
    for (String indexName : getIndices()) {
      String rootDirName = grpcServer.getIndexDir();
      LuceneServerGrpc.LuceneServerBlockingStub blockingStub = grpcServer.getBlockingStub();

      // create the index
      blockingStub.createIndex(
          CreateIndexRequest.newBuilder().setIndexName(indexName).setRootDir(rootDirName).build());

      // register fields
      blockingStub.registerFields(getIndexDef(indexName));

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

  protected void initIndex(String name) throws Exception {}

  protected List<Plugin> getPlugins() {
    return Collections.emptyList();
  }
}
