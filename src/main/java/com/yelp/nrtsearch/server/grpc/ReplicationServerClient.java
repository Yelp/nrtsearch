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
import com.google.protobuf.GeneratedMessageV3;
import com.yelp.nrtsearch.server.grpc.ReplicationServerGrpc.ReplicationServerBlockingStub;
import com.yelp.nrtsearch.server.grpc.discovery.PrimaryFileNameResolverProvider;
import com.yelp.nrtsearch.server.luceneserver.SimpleCopyJob.FileChunkStreamingIterator;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationServerClient implements Closeable {
  public static final int BINARY_MAGIC = 0x3414f5c;
  public static final int MAX_MESSAGE_BYTES_SIZE = 1 * 1024 * 1024 * 1024;
  public static final int FILE_UPDATE_INTERVAL_MS = 10 * 1000; // 10 seconds

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);;
  private static final Logger logger = LoggerFactory.getLogger(ReplicationServerClient.class);

  private final String host;
  private final int port;
  private final String discoveryFile;
  private int discoveryFileUpdateIntervalMs;
  private final ManagedChannel channel;

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
    this(
        ManagedChannelBuilder.forAddress(host, port)
            // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
            // needing certificates.
            .usePlaintext()
            .maxInboundMessageSize(MAX_MESSAGE_BYTES_SIZE)
            .build(),
        host,
        port,
        "");
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
        discoveryFileAndPort.discoveryFile);
    this.discoveryFileUpdateIntervalMs = updateIntervalMs;
  }

  /** Construct client for accessing ReplicationServer server using the existing channel. */
  ReplicationServerClient(ManagedChannel channel, String host, int port, String discoveryFile) {
    this.channel = channel;
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

  public void addReplicas(String indexName, int replicaId, String hostName, int port) {
    AddReplicaRequest addReplicaRequest =
        AddReplicaRequest.newBuilder()
            .setMagicNumber(BINARY_MAGIC)
            .setIndexName(indexName)
            .setReplicaId(replicaId)
            .setHostName(hostName)
            .setPort(port)
            .build();
    try {
      this.blockingStub.addReplicas(addReplicaRequest);
    } catch (Exception e) {
      /* Note this should allow the replica to start, but it means it will not be able to get new index updates
       * from Primary: https://github.com/Yelp/nrtsearch/issues/86 */
      logger.warn("Replica could NOT register itself with Primary ", e);
    }
  }

  public Iterator<RawFileChunk> recvRawFile(String fileName, long fpOffset, String indexName) {
    FileInfo fileInfo =
        FileInfo.newBuilder()
            .setFileName(fileName)
            .setFpStart(fpOffset)
            .setIndexName(indexName)
            .build();
    return this.blockingStub.recvRawFile(fileInfo);
  }

  public void recvRawFileV2(
      String fileName, long fpOffset, String indexName, FileChunkStreamingIterator observer) {
    FileInfo fileInfoV2 =
        FileInfo.newBuilder()
            .setFileName(fileName)
            .setFpStart(fpOffset)
            .setIndexName(indexName)
            .build();
    StreamObserver<FileInfo> responseObserver = this.asyncStub.recvRawFileV2(observer);
    observer.init(responseObserver);
    responseObserver.onNext(fileInfoV2);
  }

  public CopyState recvCopyState(String indexName, int replicaId) {
    CopyStateRequest.Builder builder = CopyStateRequest.newBuilder();
    CopyStateRequest copyStateRequest =
        builder
            .setMagicNumber(BINARY_MAGIC)
            .setReplicaId(replicaId)
            .setIndexName(indexName)
            .build();
    return this.blockingStub.recvCopyState(copyStateRequest);
  }

  public Iterator<TransferStatus> copyFiles(
      String indexName, long primaryGen, FilesMetadata filesMetadata, Deadline deadline) {
    CopyFiles.Builder copyFilesBuilder = CopyFiles.newBuilder();
    CopyFiles copyFiles =
        copyFilesBuilder
            .setMagicNumber(BINARY_MAGIC)
            .setIndexName(indexName)
            .setPrimaryGen(primaryGen)
            .setFilesMetadata(filesMetadata)
            .build();
    ReplicationServerBlockingStub blockingStub = this.blockingStub;
    if (deadline != null) {
      blockingStub = blockingStub.withDeadline(deadline);
    }
    return blockingStub.copyFiles(copyFiles);
  }

  public TransferStatus newNRTPoint(String indexName, long primaryGen, long version) {
    NewNRTPoint.Builder builder = NewNRTPoint.newBuilder();
    NewNRTPoint request =
        builder
            .setMagicNumber(BINARY_MAGIC)
            .setIndexName(indexName)
            .setPrimaryGen(primaryGen)
            .setVersion(version)
            .build();
    return this.blockingStub.newNRTPoint(request);
  }

  public SearcherVersion writeNRTPoint(String indexName) {
    return blockingStub.writeNRTPoint(
        IndexName.newBuilder().setMagicNumber(BINARY_MAGIC).setIndexName(indexName).build());
  }

  public SearcherVersion getCurrentSearcherVersion(String indexName) {
    return blockingStub.getCurrentSearcherVersion(
        IndexName.newBuilder().setMagicNumber(BINARY_MAGIC).setIndexName(indexName).build());
  }

  public GetNodesResponse getConnectedNodes(String indexName) {
    return blockingStub.getConnectedNodes(
        GetNodesRequest.newBuilder().setIndexName(indexName).build());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReplicationServerClient that = (ReplicationServerClient) o;
    return port == that.port
        && Objects.equals(logger, that.logger)
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

  public static class ReplicationServerClientManager<T extends GeneratedMessageV3> {
    Logger logger = LoggerFactory.getLogger(ReplicationServerClientManager.class);
    private static final String LOCALHOST = "localhost";
    private static final int PORT = 50052;
    Map<RequestType, ReplicationServerClient> replicationServerClients = new ConcurrentHashMap<>();

    public enum RequestType {
      ADD_REPLICAS,
      COPY_FILES,
      WRT_NRT_POINT,
      NEW_NRT_POINT,
      SEND_ME_FILES;
    }

    /* Each requestType reuses its channel (connection), so we need to make sure only one
     * instance of ReplicationServerClientManager is used by callers */
    @SuppressWarnings("unchecked")
    public T sendRequest(RequestType requestType, T request) {
      ReplicationServerClient client;
      switch (requestType) {
        case ADD_REPLICAS:
          client =
              replicationServerClients.computeIfAbsent(
                  RequestType.ADD_REPLICAS, c -> new ReplicationServerClient(LOCALHOST, PORT));
          AddReplicaRequest addReplicaRequest = (AddReplicaRequest) request;
          AddReplicaResponse response = client.blockingStub.addReplicas(addReplicaRequest);
          return (T) response;
        case COPY_FILES:
          client =
              replicationServerClients.computeIfAbsent(
                  RequestType.COPY_FILES, c -> new ReplicationServerClient(LOCALHOST, PORT));
          break;
        case WRT_NRT_POINT:
          client =
              replicationServerClients.computeIfAbsent(
                  RequestType.WRT_NRT_POINT, c -> new ReplicationServerClient(LOCALHOST, PORT));
          break;
        case NEW_NRT_POINT:
          client =
              replicationServerClients.computeIfAbsent(
                  RequestType.NEW_NRT_POINT, c -> new ReplicationServerClient(LOCALHOST, PORT));
          break;
        case SEND_ME_FILES:
          client =
              replicationServerClients.computeIfAbsent(
                  RequestType.SEND_ME_FILES, c -> new ReplicationServerClient(LOCALHOST, PORT));
          break;
        default:
          logger.info(
              String.format(
                  String.format(
                      "Invalid request type %s Supported requestTypes: %s",
                      requestType, RequestType.values())));
          break;
      }
      return null;
    }
  }

  @Override
  public String toString() {
    return String.format(
        "ReplicationServerClient(host=%s, port=%d, discoveryFile=%s)", host, port, discoveryFile);
  }
}
