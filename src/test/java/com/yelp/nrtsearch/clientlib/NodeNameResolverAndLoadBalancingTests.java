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
package com.yelp.nrtsearch.clientlib;

import static com.yelp.nrtsearch.server.grpc.GrpcServer.TEST_INDEX;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.CommitRequest;
import com.yelp.nrtsearch.server.grpc.CreateIndexRequest;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.grpc.GrpcServer;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import com.yelp.nrtsearch.server.grpc.LuceneServerStubBuilder;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.RefreshRequest;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class NodeNameResolverAndLoadBalancingTests {
  private static final String FIELD_NAME = "test_field";
  private static final String NODE_ADDRESSES_FILE_NAME = "nrtsearch-addresses.json";

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static final int SERVER_1_ID = 1;
  private static final int SERVER_2_ID = 2;
  private static final int SERVER_3_ID = 3;

  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  /**
   * This rule ensures the temporary folder which maintains indexes are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private GrpcServer server1;
  private GrpcServer server2;
  private GrpcServer server3;
  private int port1;
  private int port2;
  private int port3;
  private File addressesFile;
  private LuceneServerStubBuilder luceneServerStubBuilder;

  @Before
  public void setup() throws IOException, InterruptedException {
    addressesFile = folder.newFile(NODE_ADDRESSES_FILE_NAME);

    server1 = createGrpcServer();
    server2 = createGrpcServer();
    server3 = createGrpcServer();

    port1 = server1.getGlobalState().getPort();
    port2 = server2.getGlobalState().getPort();
    port3 = server3.getGlobalState().getPort();

    startIndexAndAddDocuments(server1, SERVER_1_ID);
    startIndexAndAddDocuments(server2, SERVER_2_ID);
    startIndexAndAddDocuments(server3, SERVER_3_ID);

    writeNodeAddressFile(port1, port2, port3);
    luceneServerStubBuilder = new LuceneServerStubBuilder(addressesFile.toString(), OBJECT_MAPPER);
  }

  private GrpcServer createGrpcServer() throws IOException {
    LuceneServerConfiguration luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE, folder.getRoot());
    return new GrpcServer(
        grpcCleanup,
        luceneServerConfiguration,
        folder,
        null,
        luceneServerConfiguration.getIndexDir(),
        TEST_INDEX,
        luceneServerConfiguration.getPort());
  }

  private void startIndexAndAddDocuments(GrpcServer server, int id)
      throws InterruptedException, IOException {
    LuceneServerGrpc.LuceneServerBlockingStub stub = server.getBlockingStub();

    stub.createIndex(CreateIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());

    FieldDefRequest fieldDefRequest =
        FieldDefRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .addField(
                Field.newBuilder()
                    .setName(FIELD_NAME)
                    .setType(FieldType.INT)
                    .setSearch(true)
                    .setStoreDocValues(true)
                    .build())
            .build();
    stub.registerFields(fieldDefRequest);
    stub.startIndex(StartIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());

    AddDocumentRequest addDocumentRequest =
        AddDocumentRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .putFields(
                FIELD_NAME,
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(String.valueOf(id))
                    .build())
            .build();
    new GrpcServer.TestServer(server, false, Mode.STANDALONE)
        .addDocumentsFromStream(Stream.of(addDocumentRequest));
    stub.commit(CommitRequest.newBuilder().setIndexName(TEST_INDEX).build());
    stub.refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    luceneServerStubBuilder.close();
    luceneServerStubBuilder.waitUntilClosed(100, TimeUnit.MILLISECONDS);
    luceneServerStubBuilder = null;
    teardownGrpcServer(server1);
    teardownGrpcServer(server2);
    teardownGrpcServer(server3);
  }

  private void teardownGrpcServer(GrpcServer server) throws IOException {
    server.getGlobalState().close();
    server.shutdown();
  }

  @Test(timeout = 10000)
  public void testSimpleLoadBalancing() throws IOException {
    LuceneServerGrpc.LuceneServerBlockingStub stub = luceneServerStubBuilder.createBlockingStub();

    warmConnections(stub);

    int requestsToEachServer = 20;
    int numServers = 3;

    Map<Integer, Integer> resultCounts =
        performSearchAndGetResultCounts(stub, requestsToEachServer, numServers);

    // All servers should get the same number of requests
    assertEquals(requestsToEachServer, resultCounts.get(SERVER_1_ID).intValue());
    assertEquals(requestsToEachServer, resultCounts.get(SERVER_2_ID).intValue());
    assertEquals(requestsToEachServer, resultCounts.get(SERVER_3_ID).intValue());
  }

  @Test(timeout = 10000)
  public void testSimpleLoadBalancingAsync() throws IOException, InterruptedException {
    LuceneServerGrpc.LuceneServerStub stub = luceneServerStubBuilder.createAsyncStub();

    warmConnections(stub);

    Map<Integer, Integer> resultCounts = new ConcurrentHashMap<>();
    int requestsToEachServer = 20;
    int numServers = 3;
    int totalRequests = numServers * requestsToEachServer;
    LongAdder errorCounter = new LongAdder();
    LongAdder completionCounter = new LongAdder();

    for (int i = 0; i < totalRequests; i++) {
      Consumer<Integer> resultConsumer =
          result ->
              resultCounts.compute(
                  result, (key, currentValue) -> currentValue == null ? 1 : currentValue + 1);
      performSearchAsync(stub, resultConsumer, completionCounter, errorCounter);
    }

    while (errorCounter.longValue() + completionCounter.longValue() < totalRequests) {
      Thread.sleep(10);
    }

    assertEquals(completionCounter.longValue(), totalRequests);

    // All servers should get the same number of requests
    assertEquals(requestsToEachServer, resultCounts.get(SERVER_1_ID).intValue());
    assertEquals(requestsToEachServer, resultCounts.get(SERVER_2_ID).intValue());
    assertEquals(requestsToEachServer, resultCounts.get(SERVER_3_ID).intValue());
  }

  @Test(timeout = 10000)
  public void testServerShutDown() throws IOException, InterruptedException {
    LuceneServerGrpc.LuceneServerBlockingStub stub = luceneServerStubBuilder.createBlockingStub();
    warmConnections(stub);

    int requestsToEachServer = 20;

    Map<Integer, Integer> resultCounts =
        performSearchAndGetResultCounts(stub, requestsToEachServer, 3);

    // Equal number of requests sent to all 3 servers
    assertEquals(resultCounts.get(SERVER_1_ID).intValue(), requestsToEachServer);
    assertEquals(resultCounts.get(SERVER_2_ID).intValue(), requestsToEachServer);
    assertEquals(resultCounts.get(SERVER_3_ID).intValue(), requestsToEachServer);

    // Shutdown server 1
    server1.forceShutdown();
    Thread.sleep(50);

    resultCounts = performSearchAndGetResultCounts(stub, requestsToEachServer, 2);

    // Now the requests are sent only to servers 2 and 3
    assertEquals(resultCounts.get(SERVER_2_ID).intValue(), requestsToEachServer);
    assertEquals(resultCounts.get(SERVER_3_ID).intValue(), requestsToEachServer);
  }

  @Test(timeout = 10000)
  public void testNodeRemovedFromAddressFile() throws IOException, InterruptedException {
    // Use a lower update interval for this test
    int updateInterval = 10;
    luceneServerStubBuilder =
        new LuceneServerStubBuilder(addressesFile.toString(), OBJECT_MAPPER, updateInterval);

    LuceneServerGrpc.LuceneServerBlockingStub stub = luceneServerStubBuilder.createBlockingStub();
    warmConnections(stub);

    int requestsToEachServer = 20;

    Map<Integer, Integer> resultCounts =
        performSearchAndGetResultCounts(stub, requestsToEachServer, 3);

    // Equal number of requests sent to all 3 servers
    assertEquals(resultCounts.get(SERVER_1_ID).intValue(), requestsToEachServer);
    assertEquals(resultCounts.get(SERVER_2_ID).intValue(), requestsToEachServer);
    assertEquals(resultCounts.get(SERVER_3_ID).intValue(), requestsToEachServer);

    // Remove server1 from the file
    writeNodeAddressFile(port2, port3);
    Thread.sleep(50);

    resultCounts = performSearchAndGetResultCounts(stub, requestsToEachServer, 2);

    // Now the requests are sent only to servers 2 and 3
    assertEquals(resultCounts.get(SERVER_2_ID).intValue(), requestsToEachServer);
    assertEquals(resultCounts.get(SERVER_3_ID).intValue(), requestsToEachServer);
  }

  @Test(timeout = 10000)
  public void testNodeAddedToAddressFile() throws IOException, InterruptedException {
    // Add only servers 2 and 3 to the file
    writeNodeAddressFile(port2, port3);

    // Use a lower update interval for this test
    int updateInterval = 10;
    luceneServerStubBuilder =
        new LuceneServerStubBuilder(addressesFile.toString(), OBJECT_MAPPER, updateInterval);

    LuceneServerGrpc.LuceneServerBlockingStub stub = luceneServerStubBuilder.createBlockingStub();
    warmConnections(stub, SERVER_2_ID, SERVER_3_ID);

    int requestsToEachServer = 20;

    Map<Integer, Integer> resultCounts =
        performSearchAndGetResultCounts(stub, requestsToEachServer, 2);

    // Requests sent to servers 2 and 3
    assertEquals(resultCounts.get(SERVER_2_ID).intValue(), requestsToEachServer);
    assertEquals(resultCounts.get(SERVER_3_ID).intValue(), requestsToEachServer);

    // Now add server 1 to file
    writeNodeAddressFile(port1, port2, port3);
    Thread.sleep(50);

    warmConnections(stub, SERVER_1_ID, SERVER_2_ID, SERVER_3_ID);

    resultCounts = performSearchAndGetResultCounts(stub, requestsToEachServer, 3);

    // Equal number of requests now sent to all 3 servers
    assertEquals(resultCounts.get(SERVER_1_ID).intValue(), requestsToEachServer);
    assertEquals(resultCounts.get(SERVER_2_ID).intValue(), requestsToEachServer);
    assertEquals(resultCounts.get(SERVER_3_ID).intValue(), requestsToEachServer);
  }

  /**
   * While the connections are initially being established perfect load balancing won't happen. Use
   * this method to call the stub and warm the connections.
   */
  private void warmConnections(LuceneServerGrpc.LuceneServerBlockingStub stub, int... expectedIds) {
    if (expectedIds.length == 0) {
      expectedIds = new int[] {SERVER_1_ID, SERVER_2_ID, SERVER_3_ID};
    }
    Set<Integer> receivedIds = new HashSet<>();
    while (Arrays.stream(expectedIds).filter(receivedIds::contains).count() != expectedIds.length) {
      receivedIds.add(performSearch(stub));
    }
  }

  /** Same as above but for async stub. */
  private void warmConnections(LuceneServerGrpc.LuceneServerStub stub, int... expectedIds)
      throws InterruptedException {
    if (expectedIds.length == 0) {
      expectedIds = new int[] {SERVER_1_ID, SERVER_2_ID, SERVER_3_ID};
    }
    LongAdder completionCounter = new LongAdder();
    LongAdder errorCounter = new LongAdder();

    Set<Integer> receivedIds = ConcurrentHashMap.newKeySet();
    int numRequests = 0;
    while (Arrays.stream(expectedIds).filter(receivedIds::contains).count() != expectedIds.length) {
      performSearchAsync(stub, receivedIds::add, completionCounter, errorCounter);
      numRequests += 1;
    }
    while (completionCounter.longValue() + errorCounter.longValue() < numRequests) {
      Thread.sleep(10);
    }
  }

  private void writeNodeAddressFile(int... ports) throws IOException {
    try (BufferedWriter writer = Files.newBufferedWriter(addressesFile.toPath())) {
      writer.write("[");
      if (ports.length != 0) {
        for (int currentPort = 0; currentPort < ports.length; currentPort += 1) {
          // name field isn't needed but added just to verify that with the right ObjectMapper
          // setting it is okay to add additional fields to the addresses file
          String node =
              String.format(
                  "{\"name\":\"server%d\",\"host\":\"127.0.0.1\",\"port\":%d}",
                  currentPort + 1, ports[currentPort]);
          writer.write(node);
          if (currentPort != ports.length - 1) {
            writer.write(",");
          }
        }
      }
      writer.write("]");
    }
  }

  private Map<Integer, Integer> performSearchAndGetResultCounts(
      LuceneServerGrpc.LuceneServerBlockingStub stub, int requestsToEachServer, int numServers) {
    Map<Integer, Integer> resultCounts = new HashMap<>();
    for (int i = 0; i < numServers * requestsToEachServer; i++) {
      int result = performSearch(stub);
      resultCounts.compute(result, (k, v) -> v == null ? 1 : v + 1);
    }
    return resultCounts;
  }

  private int performSearch(LuceneServerGrpc.LuceneServerBlockingStub stub) {
    SearchRequest searchRequest = buildSearchRequest();
    SearchResponse searchResponse =
        stub.withDeadlineAfter(4000, TimeUnit.MILLISECONDS).search(searchRequest);
    return searchResponse.getHits(0).getFieldsOrThrow(FIELD_NAME).getFieldValue(0).getIntValue();
  }

  private SearchRequest buildSearchRequest() {
    return SearchRequest.newBuilder()
        .setIndexName(TEST_INDEX)
        .setStartHit(0)
        .setTopHits(1)
        .addRetrieveFields(FIELD_NAME)
        .build();
  }

  private void performSearchAsync(
      LuceneServerGrpc.LuceneServerStub stub,
      Consumer<Integer> resultConsumer,
      LongAdder completionCounter,
      LongAdder errorCounter) {
    SearchRequest searchRequest = buildSearchRequest();
    StreamObserver<SearchResponse> responseObserver =
        new StreamObserver<>() {

          @Override
          public void onNext(SearchResponse searchResponse) {
            int result =
                searchResponse
                    .getHits(0)
                    .getFieldsOrThrow(FIELD_NAME)
                    .getFieldValue(0)
                    .getIntValue();
            if (resultConsumer != null) {
              resultConsumer.accept(result);
            }
          }

          @Override
          public void onError(Throwable t) {
            errorCounter.increment();
          }

          @Override
          public void onCompleted() {
            completionCounter.increment();
          }
        };
    stub.withDeadlineAfter(4000, TimeUnit.MILLISECONDS).search(searchRequest, responseObserver);
  }
}
