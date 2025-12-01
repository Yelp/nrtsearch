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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import com.yelp.nrtsearch.server.grpc.codec.LZ4Codec;
import io.grpc.ManagedChannel;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class NrtSearchClientTest {

  @Mock private ManagedChannel mockChannel;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testFromChannel() {
    NrtSearchClient client = NrtSearchClient.fromChannel(mockChannel);
    assertNotNull(client);
  }

  @Test
  public void testGetBlockingStub() {
    NrtSearchClient client = NrtSearchClient.fromChannel(mockChannel);
    assertNotNull(client.getBlockingStub());
    assertTrue(client.getBlockingStub() instanceof LuceneServerGrpc.LuceneServerBlockingStub);
  }

  @Test
  public void testGetAsyncStub() {
    NrtSearchClient client = NrtSearchClient.fromChannel(mockChannel);
    assertNotNull(client.getAsyncStub());
    assertTrue(client.getAsyncStub() instanceof LuceneServerGrpc.LuceneServerStub);
  }

  @Test
  public void testGetFutureStub() {
    NrtSearchClient client = NrtSearchClient.fromChannel(mockChannel);
    assertNotNull(client.getFutureStub());
    assertTrue(client.getFutureStub() instanceof LuceneServerGrpc.LuceneServerFutureStub);
  }

  @Test
  public void testShutdown() {
    when(mockChannel.isShutdown()).thenReturn(false);
    NrtSearchClient client = NrtSearchClient.fromChannel(mockChannel);
    client.shutdown();
    verify(mockChannel, times(1)).shutdown();
  }

  @Test
  public void testShutdownWhenAlreadyShutdown() {
    when(mockChannel.isShutdown()).thenReturn(true);
    NrtSearchClient client = NrtSearchClient.fromChannel(mockChannel);
    client.shutdown();
    verify(mockChannel, times(0)).shutdown();
  }

  @Test
  public void testAwaitTermination() throws InterruptedException {
    NrtSearchClient client = NrtSearchClient.fromChannel(mockChannel);
    client.awaitTermination();
    verify(mockChannel, times(1)).awaitTermination(1, TimeUnit.MINUTES);
  }

  @Test
  public void testAwaitTerminationWithCustomTimeout() throws InterruptedException {
    NrtSearchClient client = NrtSearchClient.fromChannel(mockChannel);
    client.awaitTermination(30, TimeUnit.SECONDS);
    verify(mockChannel, times(1)).awaitTermination(30, TimeUnit.SECONDS);
  }

  @Test
  public void testCompressorRegistry() {
    assertTrue(NrtSearchClient.COMPRESSOR_REGISTRY.lookupCompressor("lz4") instanceof LZ4Codec);
  }

  @Test
  public void testDecompressorRegistry() {
    assertTrue(NrtSearchClient.DECOMPRESSOR_REGISTRY.lookupDecompressor("lz4") instanceof LZ4Codec);
  }
}
