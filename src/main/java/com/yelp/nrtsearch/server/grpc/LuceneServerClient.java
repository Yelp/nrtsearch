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

import static com.yelp.nrtsearch.server.grpc.ReplicationServerClient.MAX_MESSAGE_BYTES_SIZE;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A simple client that requests a greeting from the {@link LuceneServer}. */
public class LuceneServerClient {
  private static final Logger logger = LoggerFactory.getLogger(LuceneServerClient.class.getName());

  private final ManagedChannel channel;

  public LuceneServerGrpc.LuceneServerBlockingStub getBlockingStub() {
    return blockingStub;
  }

  public LuceneServerGrpc.LuceneServerStub getAsyncStub() {
    return asyncStub;
  }

  private final LuceneServerGrpc.LuceneServerBlockingStub blockingStub;
  private final LuceneServerGrpc.LuceneServerStub asyncStub;

  /** Construct client connecting to LuceneServer server at {@code host:port}. */
  public LuceneServerClient(String host, int port) {
    this(
        ManagedChannelBuilder.forAddress(host, port)
            // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
            // needing certificates.
            .usePlaintext()
            .maxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
            .build());
  }

  /** Construct client for accessing LuceneServer server using the existing channel. */
  LuceneServerClient(ManagedChannel channel) {
    this.channel = channel;
    blockingStub =
        LuceneServerGrpc.newBlockingStub(channel)
            .withMaxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
            .withMaxOutboundMessageSize(MAX_MESSAGE_BYTES_SIZE);
    asyncStub =
        LuceneServerGrpc.newStub(channel)
            .withMaxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
            .withMaxOutboundMessageSize(MAX_MESSAGE_BYTES_SIZE);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  public void createIndex(String indexName) {
    logger.info("Will try to create index: " + indexName);
    CreateIndexRequest request = CreateIndexRequest.newBuilder().setIndexName(indexName).build();
    CreateIndexResponse response;
    try {
      response = blockingStub.createIndex(request);
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {}", e.getStatus());
      return;
    }
    logger.info("Server returned : " + response.getResponse());
  }

  public void liveSettings(
      String indexName,
      double maxRefreshSec,
      double minRefreshSec,
      double maxSearcherAgeSec,
      double indexRamBufferSizeMB,
      int addDocumentsMaxBufferLen) {
    logger.info(
        String.format(
            "will try to update liveSettings for indexName: %s, "
                + "maxRefreshSec: %s, minRefreshSec: %s, maxSearcherAgeSec: %s, "
                + "indexRamBufferSizeMB: %s, addDocumentsMaxBufferLen: %s ",
            indexName,
            maxRefreshSec,
            minRefreshSec,
            maxSearcherAgeSec,
            indexRamBufferSizeMB,
            addDocumentsMaxBufferLen));
    LiveSettingsRequest request =
        LiveSettingsRequest.newBuilder()
            .setIndexName(indexName)
            .setMaxRefreshSec(maxRefreshSec)
            .setMinRefreshSec(minRefreshSec)
            .setMaxSearcherAgeSec(maxSearcherAgeSec)
            .setIndexRamBufferSizeMB(indexRamBufferSizeMB)
            .setAddDocumentsMaxBufferLen(addDocumentsMaxBufferLen)
            .build();
    LiveSettingsResponse response;
    try {
      response = blockingStub.liveSettings(request);
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {}", e.getStatus());
      return;
    }
    logger.info("Server returned : " + response.getResponse());
  }

  public void registerFields(String jsonStr) {
    FieldDefRequest fieldDefRequest = getFieldDefRequest(jsonStr);
    FieldDefResponse response;
    try {
      response = blockingStub.registerFields(fieldDefRequest);
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {}", e.getStatus());
      return;
    }
    logger.info("Server returned : " + response.getResponse());
  }

  public void settings(Path filePath) throws IOException {
    SettingsRequest settingsRequest =
        new LuceneServerClientBuilder.SettingsClientBuilder().buildRequest(filePath);
    SettingsResponse response;
    try {
      response = blockingStub.settings(settingsRequest);
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {}", e.getStatus());
      return;
    }
    logger.info("Server returned : " + response.getResponse());
  }

  public void startIndex(Path filePath) throws IOException {
    StartIndexRequest startIndexRequest =
        new LuceneServerClientBuilder.StartIndexClientBuilder().buildRequest(filePath);
    StartIndexResponse response;
    try {
      response = blockingStub.startIndex(startIndexRequest);
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {}", e.getStatus());
      return;
    }
    logger.info("Server returned : " + response.toString());
  }

  public void addDocuments(Stream<AddDocumentRequest> addDocumentRequestStream)
      throws InterruptedException {
    final CountDownLatch finishLatch = new CountDownLatch(1);

    StreamObserver<AddDocumentResponse> responseObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(AddDocumentResponse value) {
            // Note that Server sends back only 1 message (Unary mode i.e. Server calls its onNext
            // only once
            // which is when it is done with indexing the entire stream), which means this method
            // should be
            // called only once.
            logger.info(String.format("Received response for genId: %s", value));
          }

          @Override
          public void onError(Throwable t) {
            logger.error(t.getMessage(), t);
            finishLatch.countDown();
          }

          @Override
          public void onCompleted() {
            logger.info("Received final response from server");
            finishLatch.countDown();
          }
        };

    // The responseObserver handles responses from the server (i.e. 1 onNext and 1 completed)
    // The requestObserver handles the sending of stream of client requests to server (i.e. multiple
    // onNext and 1 completed)
    StreamObserver<AddDocumentRequest> requestObserver = asyncStub.addDocuments(responseObserver);
    try {
      addDocumentRequestStream.forEach(requestObserver::onNext);
    } catch (RuntimeException e) {
      // Cancel RPC
      requestObserver.onError(e);
      throw e;
    }
    // Mark the end of requests
    requestObserver.onCompleted();

    logger.info("sent async addDocumentsRequest to server...");

    // Receiving happens asynchronously, so block here for 5 minutes
    if (!finishLatch.await(5, TimeUnit.MINUTES)) {
      logger.warn("addDocuments can not finish within 5 minutes");
    }
  }

  public void refresh(String indexName) {
    logger.info("Will try to refresh index: " + indexName);
    RefreshRequest request = RefreshRequest.newBuilder().setIndexName(indexName).build();
    RefreshResponse response;
    try {
      response = blockingStub.refresh(request);
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {}", e.getStatus());
      return;
    }
    logger.info("Server returned refreshTimeMS : " + response.getRefreshTimeMS());
  }

  public void commit(String indexName) {
    logger.info("Will try to commit index: " + indexName);
    CommitRequest request = CommitRequest.newBuilder().setIndexName(indexName).build();
    CommitResponse response;
    try {
      response = blockingStub.commit(request);
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {}", e.getStatus());
      return;
    }
    logger.info("Server returned sequence id: " + response.getGen());
  }

  public void stats(String indexName) {
    logger.info("Will try to retrieve stats for index: " + indexName);
    StatsRequest request = StatsRequest.newBuilder().setIndexName(indexName).build();
    StatsResponse response;
    try {
      response = blockingStub.stats(request);
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {}", e.getStatus());
      return;
    }
    logger.info("Server returned sequence id: " + response);
  }

  public void search(Path filePath) throws IOException {
    SearchRequest searchRequest =
        new LuceneServerClientBuilder.SearchClientBuilder().buildRequest(filePath);
    SearchResponse response;
    try {
      response = blockingStub.search(searchRequest);
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {}", e.getStatus());
      return;
    }
    logger.info("Server returned : " + response.toString());
  }

  public void delete(Path filePath) throws IOException {
    AddDocumentRequest addDocumentRequest =
        new LuceneServerClientBuilder.DeleteDocumentsBuilder().buildRequest(filePath);
    AddDocumentResponse response;
    try {
      response = blockingStub.delete(addDocumentRequest);
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {}", e.getStatus());
      return;
    }
    logger.info("Server returned indexGen : " + response.getGenId());
  }

  public void deleteIndex(String indexName) {
    DeleteIndexResponse response =
        blockingStub.deleteIndex(DeleteIndexRequest.newBuilder().setIndexName(indexName).build());
    logger.info("Server returned response : " + response.getOk());
  }

  public void deleteAllDocuments(String indexName) {
    DeleteAllDocumentsResponse response =
        blockingStub.deleteAll(
            DeleteAllDocumentsRequest.newBuilder().setIndexName(indexName).build());
    logger.info("Server returned genId : " + response.getGenId());
  }

  public void stopIndex(String indexName) {
    blockingStub.stopIndex(StopIndexRequest.newBuilder().setIndexName(indexName).build());
  }

  public void backupIndex(
      String indexName, String serviceName, String resourceName, boolean completeDirectory) {
    blockingStub.backupIndex(
        BackupIndexRequest.newBuilder()
            .setServiceName(serviceName)
            .setResourceName(resourceName)
            .setIndexName(indexName)
            .setCompleteDirectory(completeDirectory)
            .build());
  }

  public void status() throws InterruptedException {
    try {
      HealthCheckResponse status =
          blockingStub.status(HealthCheckRequest.newBuilder().setCheck(true).build());
      if (status.getHealth() == TransferStatusCode.Done) {
        logger.info("Host is up");
        return;
      }
    } catch (StatusRuntimeException e) {
      logger.info(e.getMessage());
    }
    this.shutdown();
    System.exit(1);
  }

  public void deleteIndexBackup(
      String indexName, String serviceName, String resourceName, int nDays) {
    DeleteIndexBackupRequest request =
        DeleteIndexBackupRequest.newBuilder()
            .setIndexName(indexName)
            .setServiceName(serviceName)
            .setResourceName(resourceName)
            .setNDays(nDays)
            .build();
    DeleteIndexBackupResponse deleteIndexBackupResponse = blockingStub.deleteIndexBackup(request);
    logger.info("Response: {}", deleteIndexBackupResponse);
  }

  private FieldDefRequest getFieldDefRequest(String jsonStr) {
    logger.info(String.format("Converting fields %s to proto FieldDefRequest", jsonStr));
    FieldDefRequest.Builder fieldDefRequestBuilder = FieldDefRequest.newBuilder();
    try {
      JsonFormat.parser().merge(jsonStr, fieldDefRequestBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    FieldDefRequest fieldDefRequest = fieldDefRequestBuilder.build();
    logger.info(
        String.format("jsonStr converted to proto FieldDefRequest %s", fieldDefRequest.toString()));
    return fieldDefRequest;
  }

  private SettingsRequest getSettingsRequest(String jsonStr) {
    logger.info(String.format("Converting fields %s to proto SettingsRequest", jsonStr));
    SettingsRequest.Builder settingsRequestBuilder = SettingsRequest.newBuilder();
    try {
      JsonFormat.parser().merge(jsonStr, settingsRequestBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    // set defaults
    if (settingsRequestBuilder.getNrtCachingDirectoryMaxMergeSizeMB() == 0) {
      settingsRequestBuilder.setNrtCachingDirectoryMaxMergeSizeMB(5.0);
    }
    if (settingsRequestBuilder.getNrtCachingDirectoryMaxSizeMB() == 0) {
      settingsRequestBuilder.setNrtCachingDirectoryMaxSizeMB(60.0);
    }
    if (settingsRequestBuilder.getDirectory().isEmpty()) {
      settingsRequestBuilder.setDirectory("FSDirectory");
    }
    if (settingsRequestBuilder.getNormsFormat().isEmpty()) {
      settingsRequestBuilder.setNormsFormat("Lucene80");
    }
    SettingsRequest settingsRequest = settingsRequestBuilder.build();
    logger.info(
        String.format("jsonStr converted to proto SettingsRequest %s", settingsRequest.toString()));
    return settingsRequest;
  }
}
