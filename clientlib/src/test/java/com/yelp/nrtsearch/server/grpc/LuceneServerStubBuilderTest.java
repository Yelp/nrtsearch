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

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.server.grpc.codec.LZ4Codec;
import io.grpc.Codec;
import io.grpc.ManagedChannelBuilder;
import java.util.function.Consumer;
import org.junit.Test;
import org.mockito.stubbing.Answer;

public class LuceneServerStubBuilderTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final String HOST = "host";
  public static final int PORT = 9999;

  @Test
  public void testChannelCreation() {
    LuceneServerStubBuilder stubBuilder = new LuceneServerStubBuilder(HOST, PORT);
    assert stubBuilder.channel.authority().equals(String.format("%s:%d", HOST, PORT));
  }

  @Test
  public void testBlockingStubCreation() {
    LuceneServerStubBuilder stubBuilder = new LuceneServerStubBuilder(HOST, PORT);
    assert stubBuilder
        .createBlockingStub()
        .getChannel()
        .authority()
        .equals(String.format("%s:%d", HOST, PORT));
  }

  @Test
  public void testAsyncStubCreation() {
    LuceneServerStubBuilder stubBuilder = new LuceneServerStubBuilder(HOST, PORT);
    assert stubBuilder
        .createAsyncStub()
        .getChannel()
        .authority()
        .equals(String.format("%s:%d", HOST, PORT));
  }

  @Test
  public void testFutureStubCreation() {
    LuceneServerStubBuilder stubBuilder = new LuceneServerStubBuilder(HOST, PORT);
    assert stubBuilder
        .createFutureStub()
        .getChannel()
        .authority()
        .equals(String.format("%s:%d", HOST, PORT));
  }

  @Test
  public void testAppliesChannelConfig() {
    verifyAppliesChannelConfig(
        (channelConfig -> new LuceneServerStubBuilder(HOST, PORT, OBJECT_MAPPER, channelConfig)));
    verifyAppliesChannelConfig(
        (channelConfig -> new LuceneServerStubBuilder("node_file", OBJECT_MAPPER, channelConfig)));
    verifyAppliesChannelConfig(
        (channelConfig ->
            new LuceneServerStubBuilder("node_file", OBJECT_MAPPER, 10, channelConfig)));
  }

  @Test
  public void testCompressorRegistry() {
    assertTrue(
        LuceneServerStubBuilder.COMPRESSOR_REGISTRY.lookupCompressor("identity")
            instanceof Codec.Identity);
    assertTrue(
        LuceneServerStubBuilder.COMPRESSOR_REGISTRY.lookupCompressor("gzip") instanceof Codec.Gzip);
    assertTrue(
        LuceneServerStubBuilder.COMPRESSOR_REGISTRY.lookupCompressor("lz4") instanceof LZ4Codec);
  }

  @Test
  public void testDecompressorRegistry() {
    assertTrue(
        LuceneServerStubBuilder.DECOMPRESSOR_REGISTRY.lookupDecompressor("identity")
            instanceof Codec.Identity);
    assertTrue(
        LuceneServerStubBuilder.DECOMPRESSOR_REGISTRY.lookupDecompressor("gzip")
            instanceof Codec.Gzip);
    assertTrue(
        LuceneServerStubBuilder.DECOMPRESSOR_REGISTRY.lookupDecompressor("lz4")
            instanceof LZ4Codec);
  }

  private void verifyAppliesChannelConfig(Consumer<ChannelConfig> createStubBuilder) {
    ChannelConfig mockConfig = mock(ChannelConfig.class);
    when(mockConfig.configureChannelBuilder(
            any(ManagedChannelBuilder.class), any(ObjectMapper.class)))
        .thenAnswer(
            (Answer<ManagedChannelBuilder<?>>)
                invocation -> {
                  Object[] args = invocation.getArguments();
                  return (ManagedChannelBuilder<?>) args[0];
                });
    createStubBuilder.accept(mockConfig);
    verify(mockConfig, times(1))
        .configureChannelBuilder(any(ManagedChannelBuilder.class), any(ObjectMapper.class));
    verifyNoMoreInteractions(mockConfig);
  }
}
