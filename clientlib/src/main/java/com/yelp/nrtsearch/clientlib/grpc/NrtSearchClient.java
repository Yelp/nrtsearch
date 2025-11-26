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
package com.yelp.nrtsearch.clientlib.grpc;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.clientlib.NodeAddressesFileNameResolverProvider;
import com.yelp.nrtsearch.server.grpc.ChannelConfig;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import com.yelp.nrtsearch.server.grpc.codec.LZ4Codec;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Client for interacting with the NRT Search server via gRPC. This class provides methods to
 * establish connections and obtain stubs for making synchronous, asynchronous, and future-based RPC
 * calls to the server.
 *
 * <p>The client supports various connection methods including direct host/port connection,
 * connection through a managed channel, or connection through a node address file for service
 * discovery.
 *
 * <p>It also includes support for LZ4 compression for efficient data transfer.
 */
public class NrtSearchClient {

  // Create registries with LZ4 compression support
  public static final CompressorRegistry COMPRESSOR_REGISTRY =
      CompressorRegistry.getDefaultInstance();
  public static final DecompressorRegistry DECOMPRESSOR_REGISTRY =
      DecompressorRegistry.getDefaultInstance().with(LZ4Codec.INSTANCE, true);

  static {
    COMPRESSOR_REGISTRY.register(LZ4Codec.INSTANCE);
  }

  /** The gRPC managed channel used for communication with the server. */
  private final ManagedChannel channel;

  /** Blocking stub for synchronous RPC calls. */
  private final LuceneServerGrpc.LuceneServerBlockingStub blockingStub;

  /** Asynchronous stub for non-blocking RPC calls with callbacks. */
  private final LuceneServerGrpc.LuceneServerStub asyncStub;

  /** Future stub for non-blocking RPC calls returning ListenableFuture. */
  private final LuceneServerGrpc.LuceneServerFutureStub futureStub;

  /**
   * Private constructor used by the factory methods and Builder.
   *
   * @param channel The managed channel to use for communication with the server
   */
  private NrtSearchClient(ManagedChannel channel) {
    this.channel = channel;
    this.blockingStub = LuceneServerGrpc.newBlockingStub(channel);
    this.asyncStub = LuceneServerGrpc.newStub(channel);
    this.futureStub = LuceneServerGrpc.newFutureStub(channel);
  }

  /**
   * Returns the blocking stub for making synchronous RPC calls.
   *
   * @return The blocking stub for the Lucene server
   */
  public LuceneServerGrpc.LuceneServerBlockingStub getBlockingStub() {
    return blockingStub;
  }

  /**
   * Returns the asynchronous stub for making non-blocking RPC calls with callbacks.
   *
   * @return The asynchronous stub for the Lucene server
   */
  public LuceneServerGrpc.LuceneServerStub getAsyncStub() {
    return asyncStub;
  }

  /**
   * Returns the future stub for making non-blocking RPC calls that return ListenableFuture.
   *
   * @return The future stub for the Lucene server
   */
  public LuceneServerGrpc.LuceneServerFutureStub getFutureStub() {
    return futureStub;
  }

  /**
   * Initiates an orderly shutdown of the channel. The channel is closed and no new calls will be
   * accepted. Existing calls may continue until they complete.
   */
  public void shutdown() {
    if (channel != null && !channel.isShutdown()) {
      channel.shutdown();
    }
  }

  /**
   * Waits for the channel to be terminated with a default timeout of 1 minute.
   *
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  public void awaitTermination() throws InterruptedException {
    awaitTermination(1, TimeUnit.MINUTES);
  }

  /**
   * Waits for the channel to be terminated with the specified timeout.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    if (channel != null) {
      channel.awaitTermination(timeout, unit);
    }
  }

  /**
   * Creates a new NrtSearchClient from an existing managed channel.
   *
   * @param channel the managed channel to use for communication
   * @return a new NrtSearchClient instance
   */
  public static NrtSearchClient fromChannel(ManagedChannel channel) {
    return new NrtSearchClient(channel);
  }

  /**
   * Builder class for creating NrtSearchClient instances with various configuration options.
   * Supports creating clients using direct host/port connection, managed channel, or node address
   * file for service discovery.
   */
  public static class Builder {
    private static final ObjectMapper DEFAULT_OBJECT_MAPPER =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final Supplier<ManagedChannelBuilder<?>> channelBuilderSupplier;

    private ObjectMapper objectMapper = DEFAULT_OBJECT_MAPPER;
    private ChannelConfig channelConfig;

    /** Class for specifying host and port connection details. */
    public static class HostPort {
      private final String host;
      private final int port;

      /**
       * Constructor.
       *
       * @param host nrtsearch host
       * @param port nrtsearch port
       */
      public HostPort(String host, int port) {
        this.host = host;
        this.port = port;
      }

      /** Get nrtsearch host. */
      public String getHost() {
        return host;
      }

      /** Get nrtsearch port */
      public int getPort() {
        return port;
      }
    }

    /** Class for specifying node address file details for service discovery. */
    public static class NodeAddressFile {
      private static final int DEFAULT_UPDATE_INTERVAL = 10 * 1000; // 10 seconds
      private final String nodeAddressFile;
      private final int updateInterval;

      /**
       * Constructor
       *
       * @param nodeAddressFile path to the node address file
       * @param updateInterval interval in milliseconds to check for updates to the node address
       *     file
       */
      public NodeAddressFile(String nodeAddressFile, int updateInterval) {
        this.nodeAddressFile = nodeAddressFile;
        this.updateInterval = updateInterval;
      }

      /**
       * Constructor
       *
       * @param nodeAddressFile path to the node address file
       */
      public NodeAddressFile(String nodeAddressFile) {
        this(nodeAddressFile, DEFAULT_UPDATE_INTERVAL);
      }

      public String getNodeAddressFile() {
        return nodeAddressFile;
      }

      public int getUpdateInterval() {
        return updateInterval;
      }
    }

    /**
     * Creates a new Builder using an existing ManagedChannelBuilder.
     *
     * @param channel the channel builder to use
     */
    public Builder(ManagedChannelBuilder<?> channel) {
      channelBuilderSupplier = () -> channel;
    }

    /**
     * Creates a new Builder using host and port information.
     *
     * @param hostPort the host and port information
     */
    public Builder(HostPort hostPort) {
      channelBuilderSupplier =
          () -> ManagedChannelBuilder.forAddress(hostPort.getHost(), hostPort.getPort());
    }

    /**
     * Creates a new Builder using a node address file for service discovery.
     *
     * @param nodeAddressFile the node address file information
     */
    public Builder(NodeAddressFile nodeAddressFile) {
      channelBuilderSupplier =
          () ->
              ManagedChannelBuilder.forTarget(nodeAddressFile.getNodeAddressFile())
                  .defaultLoadBalancingPolicy("round_robin")
                  .nameResolverFactory(
                      new NodeAddressesFileNameResolverProvider(
                          getObjectMapper(), nodeAddressFile.getUpdateInterval()));
    }

    /**
     * Sets a custom ObjectMapper for JSON serialization/deserialization.
     *
     * @param objectMapper the ObjectMapper to use
     * @return this builder instance
     */
    public Builder setObjectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    /**
     * Returns the ObjectMapper used by this builder.
     *
     * @return the ObjectMapper instance
     */
    private ObjectMapper getObjectMapper() {
      return objectMapper;
    }

    /**
     * Sets the channel configuration for customizing the gRPC channel.
     *
     * @param channelConfig the channel configuration
     * @return this builder instance
     */
    public Builder setChannelConfig(ChannelConfig channelConfig) {
      this.channelConfig = channelConfig;
      return this;
    }

    /**
     * Builds and returns a new NrtSearchClient instance with the configured settings.
     *
     * @return a new NrtSearchClient instance
     */
    public NrtSearchClient build() {
      ManagedChannelBuilder<?> channelBuilder = channelBuilderSupplier.get();
      if (channelConfig != null) {
        channelBuilder = channelConfig.configureChannelBuilder(channelBuilder, getObjectMapper());
      }
      channelBuilder =
          channelBuilder
              .decompressorRegistry(DECOMPRESSOR_REGISTRY)
              .compressorRegistry(COMPRESSOR_REGISTRY)
              .usePlaintext();
      return new NrtSearchClient(channelBuilder.build());
    }
  }
}
