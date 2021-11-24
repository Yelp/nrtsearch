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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.clientlib.NodeAddressesFileNameResolverProvider;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc.LuceneServerBlockingStub;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc.LuceneServerFutureStub;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc.LuceneServerStub;
import com.yelp.nrtsearch.server.grpc.codec.LZ4Codec;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** Easy entrypoint for clients to create a Lucene Server Stub. */
public class LuceneServerStubBuilder implements Closeable {
  // Create registries with LZ4 compression support
  public static final CompressorRegistry COMPRESSOR_REGISTRY =
      CompressorRegistry.getDefaultInstance();
  public static final DecompressorRegistry DECOMPRESSOR_REGISTRY =
      DecompressorRegistry.getDefaultInstance().with(LZ4Codec.INSTANCE, true);

  static {
    COMPRESSOR_REGISTRY.register(LZ4Codec.INSTANCE);
  }

  private static final int DEFAULT_UPDATE_INTERVAL = 10 * 1000; // 10 seconds

  public ManagedChannel channel;

  /**
   * Constructor that accepts a channel. For use in case client needs more options such as SSL
   *
   * @param channel custom server channel
   */
  public LuceneServerStubBuilder(ManagedChannel channel) {
    this.channel = channel;
  }

  /**
   * Constructor that accepts a host and port and creates a plaintext netty channel
   *
   * @param host server host
   * @param port server port
   */
  public LuceneServerStubBuilder(String host, int port) {
    this(
        ManagedChannelBuilder.forAddress(host, port)
            .decompressorRegistry(DECOMPRESSOR_REGISTRY)
            .compressorRegistry(COMPRESSOR_REGISTRY)
            .usePlaintext()
            .build());
  }

  /**
   * Constructor that accepts a host and port and creates a plaintext netty channel
   *
   * @param host server host
   * @param port server port
   * @param objectMapper object mapper
   * @param channelConfig additional channel configuration
   */
  public LuceneServerStubBuilder(
      String host, int port, ObjectMapper objectMapper, ChannelConfig channelConfig) {
    this(
        channelConfig
            .configureChannelBuilder(ManagedChannelBuilder.forAddress(host, port), objectMapper)
            .decompressorRegistry(DECOMPRESSOR_REGISTRY)
            .compressorRegistry(COMPRESSOR_REGISTRY)
            .usePlaintext()
            .build());
  }

  /**
   * Create {@link LuceneServerStubBuilder} with a {@link io.grpc.Channel} which finds Nrtsearch
   * node addresses from a file. The file must contain host and port as properties in list in json
   * format. The file be checked for updates every {@link #DEFAULT_UPDATE_INTERVAL} milliseconds.
   * E.g.: [{"host":"10.10.1.1", "port": 12000}, {"host":"10.20.1.1", "port": 14000}] If there are
   * additional properties than host and port, the {@link ObjectMapper} must be configured to ignore
   * them before passing it to this constructor.
   *
   * @param nodeAddressFile Path to file containing node addresses
   * @param objectMapper {@link ObjectMapper} to use to deserialize the json file
   */
  public LuceneServerStubBuilder(String nodeAddressFile, ObjectMapper objectMapper) {
    this(nodeAddressFile, objectMapper, DEFAULT_UPDATE_INTERVAL);
  }

  /**
   * Create {@link LuceneServerStubBuilder} with a {@link io.grpc.Channel} which finds Nrtsearch
   * node addresses from a file. The file must contain host and port as properties in list in json
   * format. The file be checked for updates every {@link #DEFAULT_UPDATE_INTERVAL} milliseconds.
   * E.g.: [{"host":"10.10.1.1", "port": 12000}, {"host":"10.20.1.1", "port": 14000}] If there are
   * additional properties than host and port, the {@link ObjectMapper} must be configured to ignore
   * them before passing it to this constructor.
   *
   * @param nodeAddressFile Path to file containing node addresses
   * @param objectMapper {@link ObjectMapper} to use to deserialize the json file
   * @param channelConfig additional channel configuration
   */
  public LuceneServerStubBuilder(
      String nodeAddressFile, ObjectMapper objectMapper, ChannelConfig channelConfig) {
    this(nodeAddressFile, objectMapper, DEFAULT_UPDATE_INTERVAL, channelConfig);
  }

  /**
   * Like {@link #LuceneServerStubBuilder(String, ObjectMapper)} but additionally provide an
   * interval to check the node address file for updates.
   *
   * @param nodeAddressFile Path to file containing node addresses
   * @param objectMapper {@link ObjectMapper} to use to deserialize the json file
   * @param updateInterval Time-between checks for changes to node addresses file in milli-seconds
   */
  public LuceneServerStubBuilder(
      String nodeAddressFile, ObjectMapper objectMapper, int updateInterval) {
    this(
        ManagedChannelBuilder.forTarget(nodeAddressFile)
            .defaultLoadBalancingPolicy("round_robin")
            .nameResolverFactory(
                new NodeAddressesFileNameResolverProvider(objectMapper, updateInterval))
            .decompressorRegistry(DECOMPRESSOR_REGISTRY)
            .compressorRegistry(COMPRESSOR_REGISTRY)
            .usePlaintext()
            .build());
  }

  /**
   * Like {@link #LuceneServerStubBuilder(String, ObjectMapper)} but additionally provide an
   * interval to check the node address file for updates.
   *
   * @param nodeAddressFile Path to file containing node addresses
   * @param objectMapper {@link ObjectMapper} to use to deserialize the json file
   * @param updateInterval Time-between checks for changes to node addresses file in milli-seconds
   * @param channelConfig additional channel configuration
   */
  public LuceneServerStubBuilder(
      String nodeAddressFile,
      ObjectMapper objectMapper,
      int updateInterval,
      ChannelConfig channelConfig) {
    this(
        channelConfig
            .configureChannelBuilder(ManagedChannelBuilder.forTarget(nodeAddressFile), objectMapper)
            .defaultLoadBalancingPolicy("round_robin")
            .decompressorRegistry(DECOMPRESSOR_REGISTRY)
            .compressorRegistry(COMPRESSOR_REGISTRY)
            .nameResolverFactory(
                new NodeAddressesFileNameResolverProvider(objectMapper, updateInterval))
            .usePlaintext()
            .build());
  }

  /**
   * Create a blocking stub for LuceneServer
   *
   * @return blocking stub
   */
  public LuceneServerBlockingStub createBlockingStub() {
    return LuceneServerGrpc.newBlockingStub(channel);
  }

  /**
   * Create a async stub for LuceneServer Note here that you don't get return values back on an
   * async stub
   *
   * @return async stub
   */
  public LuceneServerStub createAsyncStub() {
    return LuceneServerGrpc.newStub(channel);
  }

  /**
   * Create a future stub for LuceneServer This is better when you want to be event oriented and
   * register callbacks
   *
   * @return blocking stub
   */
  public LuceneServerFutureStub createFutureStub() {
    return LuceneServerGrpc.newFutureStub(channel);
  }

  @Override
  public void close() throws IOException {
    if (channel != null && !channel.isShutdown()) {
      channel.shutdown();
    }
  }

  public void waitUntilClosed(long timeout, TimeUnit unit)
      throws IOException, InterruptedException {
    if (!channel.isShutdown()) {
      close();
    }
    channel.awaitTermination(timeout, unit);
  }
}
