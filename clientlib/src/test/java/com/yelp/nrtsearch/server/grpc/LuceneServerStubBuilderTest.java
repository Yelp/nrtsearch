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

import org.junit.Test;

public class LuceneServerStubBuilderTest {
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
}
