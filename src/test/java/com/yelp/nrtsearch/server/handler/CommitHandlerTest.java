/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.handler;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.grpc.CommitRequest;
import com.yelp.nrtsearch.server.grpc.CommitResponse;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Future;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class CommitHandlerTest {

  @Test
  public void testHandle_usesCommitExecutor() throws Exception {
    // Setup
    GlobalState mockGlobalState = mock(GlobalState.class);
    IndexState mockIndexState = mock(IndexState.class);
    StreamObserver<CommitResponse> mockResponseObserver = mock(StreamObserver.class);

    // Create a spy of CommitHandler so we can mock the getIndexState method
    CommitHandler handler = spy(new CommitHandler(mockGlobalState));

    // Configure mocks
    doReturn(mockIndexState).when(handler).getIndexState(anyString());
    when(mockIndexState.commit()).thenReturn(42L);
    when(mockGlobalState.getEphemeralId()).thenReturn("test_id");
    when(mockGlobalState.getConfiguration()).thenReturn(null); // Not used in this test

    // Mock submitCommitTask to execute the runnable immediately
    when(mockGlobalState.submitCommitTask(any(Runnable.class)))
        .thenAnswer(
            new Answer<Future<?>>() {
              @Override
              public Future<?> answer(InvocationOnMock invocation) {
                Runnable runnable = invocation.getArgument(0);
                runnable.run();
                return mock(Future.class);
              }
            });

    // Execute
    CommitRequest request = CommitRequest.newBuilder().setIndexName("test_index").build();
    handler.handle(request, mockResponseObserver);

    // Verify interactions
    verify(mockGlobalState).submitCommitTask(any(Runnable.class));
    verify(mockIndexState).commit();

    // Capture and verify response
    ArgumentCaptor<CommitResponse> responseCaptor = ArgumentCaptor.forClass(CommitResponse.class);
    verify(mockResponseObserver).onNext(responseCaptor.capture());
    verify(mockResponseObserver).onCompleted();

    CommitResponse response = responseCaptor.getValue();
    assertEquals(42L, response.getGen());
    assertEquals("test_id", response.getPrimaryId());
  }
}
