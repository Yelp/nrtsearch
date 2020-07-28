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

import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc.LuceneServerBlockingStub;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc.LuceneServerFutureStub;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc.LuceneServerStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/** Easy entrypoint for clients to create a Lucene Server Stub. */
public class LuceneServerStubBuilder {
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
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
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
}
