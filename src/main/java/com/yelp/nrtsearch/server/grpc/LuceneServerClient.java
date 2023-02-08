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
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A simple client that requests a greeting from the {@link LuceneServer}. */
public class LuceneServerClient implements Closeable {
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

  @Override
  public void close() {
    this.channel.shutdownNow();
  }

  public void createIndex(
      String indexName,
      String existsWithId,
      IndexSettings settings,
      IndexLiveSettings liveSettings,
      FieldDefRequest fields,
      boolean start) {
    logger.info("Will try to create index: " + indexName);
    CreateIndexRequest.Builder requestBuilder =
        CreateIndexRequest.newBuilder().setIndexName(indexName);
    if (existsWithId != null && !existsWithId.isEmpty()) {
      logger.info("Using existing id: " + existsWithId);
      requestBuilder.setExistsWithId(existsWithId);
    }
    if (settings != null) {
      requestBuilder.setSettings(settings);
    }
    if (liveSettings != null) {
      requestBuilder.setLiveSettings(liveSettings);
    }
    if (fields != null) {
      requestBuilder.addAllFields(fields.getFieldList());
    }
    requestBuilder.setStart(start);
    CreateIndexResponse response;
    try {
      response = blockingStub.createIndex(requestBuilder.build());
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
      int addDocumentsMaxBufferLen,
      int sliceMaxDocs,
      int sliceMaxSegments,
      int virtualShards,
      int maxMergedSegmentMB,
      int segmentsPerTier,
      double defaultSearchTimeoutSec,
      int defaultSearchTimeoutCheckEvery,
      int defaultTerminateAfter) {
    logger.info(
        String.format(
            "will try to update liveSettings for indexName: %s, "
                + "maxRefreshSec: %s, minRefreshSec: %s, maxSearcherAgeSec: %s, "
                + "indexRamBufferSizeMB: %s, addDocumentsMaxBufferLen: %s, sliceMaxDocs: %s, "
                + "sliceMaxSegments: %s, virtualShards: %s, maxMergedSegmentMB: %s, segmentsPerTier: %s, "
                + "defaultSearchTimeoutSec: %s, defaultSearchTimeoutCheckEvery: %s, defaultTerminateAfter: %s ",
            indexName,
            maxRefreshSec,
            minRefreshSec,
            maxSearcherAgeSec,
            indexRamBufferSizeMB,
            addDocumentsMaxBufferLen,
            sliceMaxDocs,
            sliceMaxSegments,
            virtualShards,
            maxMergedSegmentMB,
            segmentsPerTier,
            defaultSearchTimeoutSec,
            defaultSearchTimeoutCheckEvery,
            defaultTerminateAfter));
    LiveSettingsRequest request =
        LiveSettingsRequest.newBuilder()
            .setIndexName(indexName)
            .setMaxRefreshSec(maxRefreshSec)
            .setMinRefreshSec(minRefreshSec)
            .setMaxSearcherAgeSec(maxSearcherAgeSec)
            .setIndexRamBufferSizeMB(indexRamBufferSizeMB)
            .setAddDocumentsMaxBufferLen(addDocumentsMaxBufferLen)
            .setSliceMaxDocs(sliceMaxDocs)
            .setSliceMaxSegments(sliceMaxSegments)
            .setVirtualShards(virtualShards)
            .setMaxMergedSegmentMB(maxMergedSegmentMB)
            .setSegmentsPerTier(segmentsPerTier)
            .setDefaultSearchTimeoutSec(defaultSearchTimeoutSec)
            .setDefaultSearchTimeoutCheckEvery(defaultSearchTimeoutCheckEvery)
            .setDefaultTerminateAfter(defaultTerminateAfter)
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

  public void liveSettingsV2(LiveSettingsV2Request liveSettingsV2Request) {
    LiveSettingsV2Response response;
    try {
      response = blockingStub.liveSettingsV2(liveSettingsV2Request);
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {}", e.getStatus());
      return;
    }
    try {
      logger.info("Server returned : " + JsonFormat.printer().print(response.getLiveSettings()));
    } catch (Exception e) {
      logger.info("Error printing response message: " + response, e);
    }
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

  public void reloadState() {
    ReloadStateRequest reloadStateRequest = ReloadStateRequest.newBuilder().build();
    try {
      blockingStub.reloadState(reloadStateRequest);
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {}", e.getStatus());
    }
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

  public void settingsV2(String indexName, Path filePath) throws IOException {
    SettingsV2Request settingsRequest;
    if (filePath != null) {
      settingsRequest =
          new LuceneServerClientBuilder.SettingsV2ClientBuilder().buildRequest(filePath);
      settingsRequest = settingsRequest.toBuilder().setIndexName(indexName).build();
    } else {
      settingsRequest = SettingsV2Request.newBuilder().setIndexName(indexName).build();
    }
    SettingsV2Response response;
    try {
      response = blockingStub.settingsV2(settingsRequest);
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {}", e.getStatus());
      return;
    }
    try {
      logger.info("Server returned : " + JsonFormat.printer().print(response.getSettings()));
    } catch (Exception e) {
      logger.info("Error printing response message: " + response, e);
    }
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

  public void startIndexV2(String indexName) {
    StartIndexV2Request startIndexRequest =
        StartIndexV2Request.newBuilder().setIndexName(indexName).build();
    StartIndexResponse response;
    try {
      response = blockingStub.startIndexV2(startIndexRequest);
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
    logger.info(
        "Server returned sequence id: "
            + response.getGen()
            + ", primary id: "
            + response.getPrimaryId());
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

  public boolean ready(String indices) {
    logger.info("Will check if indices are ready: " + indices);
    ReadyCheckRequest request = ReadyCheckRequest.newBuilder().setIndexNames(indices).build();
    HealthCheckResponse response;
    try {
      response = blockingStub.ready(request);
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {}", e.getStatus());
      return false;
    }
    logger.info("Server returned response: " + response);
    return TransferStatusCode.Done.equals(response.getHealth());
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
    logger.info(
        "Server returned indexGen : "
            + response.getGenId()
            + ", primary id: "
            + response.getPrimaryId());
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
      String indexName,
      String serviceName,
      String resourceName,
      boolean completeDirectory,
      boolean stream) {
    blockingStub.backupIndex(
        BackupIndexRequest.newBuilder()
            .setServiceName(serviceName)
            .setResourceName(resourceName)
            .setIndexName(indexName)
            .setCompleteDirectory(completeDirectory)
            .setStream(stream)
            .build());
  }

  public void backupWarmingQueries(
      String index, String service, int numQueriesThreshold, int uptimeMinutesThreshold) {
    blockingStub.backupWarmingQueries(
        BackupWarmingQueriesRequest.newBuilder()
            .setIndex(index)
            .setServiceName(service)
            .setNumQueriesThreshold(numQueriesThreshold)
            .setUptimeMinutesThreshold(uptimeMinutesThreshold)
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

  public List<String> getIndices() {
    return blockingStub.indices(IndicesRequest.newBuilder().build()).getIndicesResponseList()
        .stream()
        .map(IndexStatsResponse::getIndexName)
        .collect(Collectors.toList());
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
