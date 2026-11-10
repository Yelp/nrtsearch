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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.grpc.ReplicationServerGrpc.ReplicationServerBlockingStub;
import com.yelp.nrtsearch.server.grpc.discovery.PrimaryFileNameResolverProvider;
import com.yelp.nrtsearch.server.nrt.jobs.SimpleCopyJob.FileChunkStreamingIterator;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationServerClient implements Closeable {
  public static final int BINARY_MAGIC = 0x3414f5c;
  public static final int MAX_MESSAGE_BYTES_SIZE = 1 * 1024 * 1024 * 1024;
  public static final int FILE_UPDATE_INTERVAL_MS = 10 * 1000; // 10 seconds
  public static final long NO_DEADLINE = 0;

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  private static final Logger logger = LoggerFactory.getLogger(ReplicationServerClient.class);

  private final String host;
  private final int port;
  private final String discoveryFile;
  private int discoveryFileUpdateIntervalMs;
  private final ManagedChannel channel;
  private final long deadlineMs;

  public ReplicationServerGrpc.ReplicationServerBlockingStub getBlockingStub() {
    return blockingStub;
  }

  public ReplicationServerGrpc.ReplicationServerStub getAsyncStub() {
    return asyncStub;
  }

  private final ReplicationServerGrpc.ReplicationServerBlockingStub blockingStub;
  private final ReplicationServerGrpc.ReplicationServerStub asyncStub;

  /**
   * Container class the hold the path to a service discovery file and a port. If the port is <= 0,
   * the port value from the discovery file is used.
   */
  public static class DiscoveryFileAndPort {
    public final String discoveryFile;
    public final int port;

    /**
     * Contructor.
     *
     * @param discoveryFile path to service discovery file
     * @param port port
     */
    public DiscoveryFileAndPort(String discoveryFile, int port) {
      this.discoveryFile = discoveryFile;
      this.port = port;
    }
  }

  /** Construct client connecting to ReplicationServer server at {@code host:port}. */
  public ReplicationServerClient(String host, int port) {
    this(host, port, false);
  }

  /** Construct client connecting to ReplicationServer server at {@code host:port}. */
  public ReplicationServerClient(String host, int port, boolean useKeepAlive) {
    this(host, port, useKeepAlive, NO_DEADLINE);
  }

  /**
   * Construct client connecting to ReplicationServer server at {@code host:port}.
   *
   * @param host server host
   * @param port server port
   * @param useKeepAlive if keep alive should be enabled on the channel
   * @param deadlineMs deadline applied to each unary rpc (newNRTPoint, recvCopyState, addReplicas,
   *     etc.), or <= 0 for no deadline. Not applied to file streaming rpcs (copyFiles, recvRawFile,
   *     recvRawFileV2).
   */
  public ReplicationServerClient(String host, int port, boolean useKeepAlive, long deadlineMs) {
    this(createManagedChannel(host, port, useKeepAlive), host, port, "", deadlineMs);
  }

  private static ManagedChannel createManagedChannel(String host, int port, boolean useKeepAlive) {
    ManagedChannelBuilder<?> managedChannelBuilder =
        ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .maxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE);
    setKeepAlive(managedChannelBuilder, useKeepAlive);
    return managedChannelBuilder.build();
  }

  static void setKeepAlive(ManagedChannelBuilder<?> managedChannelBuilder, boolean useKeepAlive) {
    if (useKeepAlive) {
      managedChannelBuilder
          .keepAliveTime(1, TimeUnit.MINUTES)
          .keepAliveTimeout(10, TimeUnit.SECONDS)
          .keepAliveWithoutCalls(true);
    }
  }

  /**
   * Construct client connecting to a ReplicationServer based on the host/port in a service
   * discovery file.
   *
   * @param discoveryFileAndPort discovery file with potential port override
   */
  public ReplicationServerClient(DiscoveryFileAndPort discoveryFileAndPort) {
    this(discoveryFileAndPort, FILE_UPDATE_INTERVAL_MS);
  }

  /**
   * Construct client connecting to a ReplicationServer based on the host/port in a service
   * discovery file.
   *
   * @param discoveryFileAndPort discovery file with potential port override
   * @param updateIntervalMs how often to check if the primary address has been updated
   */
  public ReplicationServerClient(DiscoveryFileAndPort discoveryFileAndPort, int updateIntervalMs) {
    this(discoveryFileAndPort, updateIntervalMs, NO_DEADLINE);
  }

  /**
   * Construct client connecting to a ReplicationServer based on the host/port in a service
   * discovery file.
   *
   * @param discoveryFileAndPort discovery file with potential port override
   * @param updateIntervalMs how often to check if the primary address has been updated
   * @param deadlineMs deadline applied to each unary rpc (newNRTPoint, recvCopyState, addReplicas,
   *     etc.), or <= 0 for no deadline. Not applied to file streaming rpcs (copyFiles, recvRawFile,
   *     recvRawFileV2).
   */
  public ReplicationServerClient(
      DiscoveryFileAndPort discoveryFileAndPort, int updateIntervalMs, long deadlineMs) {
    this(
        ManagedChannelBuilder.forTarget(discoveryFileAndPort.discoveryFile)
            .usePlaintext()
            .maxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
            .nameResolverFactory(
                new PrimaryFileNameResolverProvider(
                    OBJECT_MAPPER, updateIntervalMs, discoveryFileAndPort.port))
            .build(),
        "",
        discoveryFileAndPort.port,
        discoveryFileAndPort.discoveryFile,
        deadlineMs);
    this.discoveryFileUpdateIntervalMs = updateIntervalMs;
  }

  /** Construct client for accessing ReplicationServer server using the existing channel. */
  ReplicationServerClient(ManagedChannel channel, String host, int port, String discoveryFile) {
    this(channel, host, port, discoveryFile, NO_DEADLINE);
  }

  /** Construct client for accessing ReplicationServer server using the existing channel. */
  ReplicationServerClient(
      ManagedChannel channel, String host, int port, String discoveryFile, long deadlineMs) {
    this.channel = channel;
    this.deadlineMs = deadlineMs;
    blockingStub =
        ReplicationServerGrpc.newBlockingStub(channel)
            .withMaxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
            .withMaxOutboundMessageSize(MAX_MESSAGE_BYTES_SIZE);
    asyncStub =
        ReplicationServerGrpc.newStub(channel)
            .withMaxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
            .withMaxOutboundMessageSize(MAX_MESSAGE_BYTES_SIZE);
    this.host = host;
    this.port = port;
    this.discoveryFile = discoveryFile;
  }

  @VisibleForTesting
  int getDiscoveryFileUpdateIntervalMs() {
    return discoveryFileUpdateIntervalMs;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getDiscoveryFile() {
    return discoveryFile;
  }

  /** Get the deadline applied to unary rpcs, <= 0 means no deadline. */
  public long getDeadlineMs() {
    return deadlineMs;
  }

  /**
   * Get the blocking stub to use for a unary rpc, with the client deadline applied if one is
   * configured.
   */
  private ReplicationServerBlockingStub unaryStub() {
    if (deadlineMs > 0) {
      return blockingStub.withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS);
    }
    return blockingStub;
  }

  @Override
  public void close() {
    try {
      shutdown();
    } catch (InterruptedException e) {
      logger.warn("channel shutdown interrupted.", e);
      Thread.currentThread().interrupt();
    }
  }

  private void shutdown() throws InterruptedException {
    channel.shutdown();
    boolean res = channel.awaitTermination(10, TimeUnit.SECONDS);
    if (!res) {
      logger.warn(String.format("channel on %s shutdown was not shutdown cleanly", this));
    }
  }

  public void addReplicas(
      String indexName, String indexId, String nodeName, String hostName, int port) {
    AddReplicaRequest addReplicaRequest =
        AddReplicaRequest.newBuilder()
            .setMagicNumber(BINARY_MAGIC)
            .setIndexName(indexName)
            .setIndexId(indexId)
            .setNodeName(nodeName)
            .setHostName(hostName)
            .setPort(port)
            .build();
    try {
      unaryStub().addReplicas(addReplicaRequest);
    } catch (Exception e) {
      /* Note this should allow the replica to start, but it means it will not be able to get new index updates
       * from Primary: https://github.com/Yelp/nrtsearch/issues/86 */
      logger.warn("Replica could NOT register itself with Primary ", e);
    }
  }

  public Iterator<RawFileChunk> recvRawFile(
      String fileName, long fpOffset, String indexName, String indexId) {
    FileInfo fileInfo =
        FileInfo.newBuilder()
            .setFileName(fileName)
            .setFpStart(fpOffset)
            .setIndexName(indexName)
            .setIndexId(indexId)
            .build();
    return this.blockingStub.recvRawFile(fileInfo);
  }

  public void recvRawFileV2(
      String fileName,
      long fpOffset,
      String indexName,
      String indexId,
      FileChunkStreamingIterator observer) {
    FileInfo fileInfoV2 =
        FileInfo.newBuilder()
            .setFileName(fileName)
            .setFpStart(fpOffset)
            .setIndexName(indexName)
            .setIndexId(indexId)
            .build();
    StreamObserver<FileInfo> responseObserver = this.asyncStub.recvRawFileV2(observer);
    observer.init(responseObserver);
    responseObserver.onNext(fileInfoV2);
  }

  public CopyState recvCopyState(String indexName, String indexId, int replicaId) {
    CopyStateRequest.Builder builder = CopyStateRequest.newBuilder();
    CopyStateRequest copyStateRequest =
        builder
            .setMagicNumber(BINARY_MAGIC)
            .setReplicaId(replicaId)
            .setIndexName(indexName)
            .setIndexId(indexId)
            .build();
    return unaryStub().recvCopyState(copyStateRequest);
  }

  public Iterator<TransferStatus> copyFiles(
      String indexName,
      String indexId,
      long primaryGen,
      FilesMetadata filesMetadata,
      Deadline deadline,
      String primaryId,
      String timeString) {
    CopyFiles.Builder copyFilesBuilder = CopyFiles.newBuilder();
    copyFilesBuilder
        .setMagicNumber(BINARY_MAGIC)
        .setIndexName(indexName)
        .setIndexId(indexId)
        .setPrimaryGen(primaryGen)
        .setFilesMetadata(filesMetadata);
    if (primaryId != null) {
      copyFilesBuilder.setPrimaryId(primaryId);
    }
    if (timeString != null) {
      copyFilesBuilder.setTimeString(timeString);
    }
    CopyFiles copyFiles = copyFilesBuilder.build();
    ReplicationServerBlockingStub blockingStub = this.blockingStub;
    if (deadline != null) {
      blockingStub = blockingStub.withDeadline(deadline);
    }
    return blockingStub.copyFiles(copyFiles);
  }

  public TransferStatus newNRTPoint(
      String indexName, String indexId, long primaryGen, long version) {
    NewNRTPoint.Builder builder = NewNRTPoint.newBuilder();
    NewNRTPoint request =
        builder
            .setMagicNumber(BINARY_MAGIC)
            .setIndexName(indexName)
            .setIndexId(indexId)
            .setPrimaryGen(primaryGen)
            .setVersion(version)
            .build();
    return unaryStub().newNRTPoint(request);
  }

  public SearcherVersion writeNRTPoint(String indexName) {
    return unaryStub()
        .writeNRTPoint(
            IndexName.newBuilder().setMagicNumber(BINARY_MAGIC).setIndexName(indexName).build());
  }

  public SearcherVersion getCurrentSearcherVersion(String indexName) {
    return unaryStub()
        .getCurrentSearcherVersion(
            IndexName.newBuilder().setMagicNumber(BINARY_MAGIC).setIndexName(indexName).build());
  }

  public GetNodesResponse getConnectedNodes(String indexName) {
    return unaryStub()
        .getConnectedNodes(GetNodesRequest.newBuilder().setIndexName(indexName).build());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReplicationServerClient that = (ReplicationServerClient) o;
    return port == that.port
        && Objects.equals(host, that.host)
        && Objects.equals(discoveryFile, that.discoveryFile)
        && Objects.equals(channel, that.channel)
        && Objects.equals(blockingStub, that.blockingStub)
        && Objects.equals(asyncStub, that.asyncStub);
  }

  @Override
  public int hashCode() {
    return Objects.hash(logger, host, port, channel, blockingStub, asyncStub);
  }

  @Override
  public String toString() {
    return String.format(
        "ReplicationServerClient(host=%s, port=%d, discoveryFile=%s)", host, port, discoveryFile);
  }
}
