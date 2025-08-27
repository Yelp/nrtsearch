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
package com.yelp.nrtsearch.server.ingestion;

import static org.mockito.Mockito.*;

import com.yelp.nrtsearch.server.plugins.IngestionPlugin;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Test;

public class IngestionPluginUtilsTest {

  private IngestionPlugin mockPlugin;
  private AbstractIngestor mockIngestor;
  private ExecutorService mockExecutor;
  private GlobalState mockGlobalState;

  @Before
  public void setUp() {
    mockPlugin = mock(IngestionPlugin.class);
    mockIngestor = mock(AbstractIngestor.class);
    mockExecutor = mock(ExecutorService.class);
    mockGlobalState = mock(GlobalState.class);

    when(mockPlugin.getIngestor()).thenReturn(mockIngestor);
    when(mockPlugin.getIngestionExecutor()).thenReturn(mockExecutor);
  }

  @Test
  public void testInitializeAndStart_submitsStartTask() throws IOException {
    // Capture the submitted Runnable
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              task.run(); // simulate executor running the task
              return null;
            })
        .when(mockExecutor)
        .submit(any(Runnable.class));

    IngestionPluginUtils.initializeAndStart(mockPlugin, mockGlobalState);

    // Verify that initialize and start were called
    verify(mockIngestor).initialize(mockGlobalState);
    verify(mockIngestor).start();
    verify(mockExecutor).submit(any(Runnable.class));
  }

  @Test
  public void testInitializeAndStart_nonAbstractIngestor() throws IOException {
    Ingestor mockBasicIngestor = mock(Ingestor.class);
    when(mockPlugin.getIngestor()).thenReturn(mockBasicIngestor);

    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              task.run();
              return null;
            })
        .when(mockExecutor)
        .submit(any(Runnable.class));

    IngestionPluginUtils.initializeAndStart(mockPlugin, mockGlobalState);

    verify(mockBasicIngestor, never()).initialize(any());
    verify(mockBasicIngestor).start();
  }
}
