/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.concurrent.ExecutorFactory;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.config.ThreadPoolConfiguration;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GlobalStateTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() {
    // Initialize ExecutorFactory with default configuration
    NrtsearchConfig defaultConfig = getConfig("nodeName: test");
    ExecutorFactory.init(defaultConfig.getThreadPoolConfiguration());
  }

  private NrtsearchConfig getConfig(String config) {
    return new NrtsearchConfig(new ByteArrayInputStream(config.getBytes()));
  }

  @Test
  public void testCreateBackendGlobalState() throws IOException {
    String configFile =
        String.join(
            "\n",
            "stateConfig:",
            "  backendType: LOCAL",
            "stateDir: " + folder.getRoot().getAbsolutePath());
    NrtsearchConfig configuration = getConfig(configFile);
    GlobalState globalState = GlobalState.createState(configuration, null);
    assertTrue(globalState instanceof BackendGlobalState);
  }

  @Test
  public void testGetGeneration() throws IOException {
    String configFile = String.join("\n", "stateConfig:", "  backendType: LOCAL");
    NrtsearchConfig configuration = getConfig(configFile);
    GlobalState globalState = GlobalState.createState(configuration, null);
    long gen = globalState.getGeneration();
    assertTrue(gen > 0);
    assertEquals(gen, globalState.getGeneration());

    try {
      Thread.sleep(50);
    } catch (InterruptedException ignore) {
    }
    GlobalState globalState2 = GlobalState.createState(configuration, null);
    assertTrue(globalState2.getGeneration() > gen);
  }

  @Test
  public void testSubmitCommitTask_executesTask() throws IOException, InterruptedException {
    String configFile = String.join("\n", "stateConfig:", "  backendType: LOCAL");
    NrtsearchConfig configuration = getConfig(configFile);
    GlobalState globalState = GlobalState.createState(configuration, null);

    // Create a task that will modify this array when executed
    boolean[] taskExecuted = {false};
    globalState.submitCommitTask(() -> taskExecuted[0] = true);

    // Wait a short time for the task to be executed
    Thread.sleep(100);

    // Verify the task was executed
    assertTrue("Commit task was not executed", taskExecuted[0]);
  }

  @Test
  public void testSubmitCommitTask_usesCorrectExecutor() throws IOException {
    String configFile =
        String.join(
            "\n",
            "stateConfig:",
            "  backendType: LOCAL",
            "threadPoolConfiguration:",
            "  commit:",
            "    maxThreads: 3",
            "    maxBufferedItems: 25",
            "    threadNamePrefix: TestCommitExecutor");
    NrtsearchConfig configuration = getConfig(configFile);

    // Initialize ExecutorFactory with our custom configuration
    ExecutorFactory.init(configuration.getThreadPoolConfiguration());

    GlobalState globalState = GlobalState.createState(configuration, null);

    // Execute a simple task to ensure the executor is created
    globalState.submitCommitTask(() -> {});

    // The executor should now use the custom configuration
    ThreadPoolConfiguration.ThreadPoolSettings settings =
        globalState
            .getThreadPoolConfiguration()
            .getThreadPoolSettings(ExecutorFactory.ExecutorType.COMMIT);

    assertEquals(3, settings.maxThreads());
    assertEquals(25, settings.maxBufferedItems());
    assertEquals("TestCommitExecutor", settings.threadNamePrefix());
  }

  @Test
  public void testSubmitCommitTask_withToggleDisabled() throws IOException, InterruptedException {
    // Create configuration with useSeparateCommitExecutor = false
    String configFile =
        String.join(
            "\n", "stateConfig:", "  backendType: LOCAL", "useSeparateCommitExecutor: false");
    NrtsearchConfig configuration = getConfig(configFile);
    GlobalState globalState = GlobalState.createState(configuration, null);

    boolean[] taskExecuted = {false};
    globalState.submitCommitTask(() -> taskExecuted[0] = true);

    // Wait a short time for the task to be executed
    Thread.sleep(100);

    // Verify the task was executed by the indexing executor
    assertTrue("Commit task was not executed", taskExecuted[0]);
  }

  @Test
  public void testSubmitCommitTask_withToggleEnabled() throws IOException, InterruptedException {
    // Create configuration with useSeparateCommitExecutor = true
    String configFile =
        String.join(
            "\n", "stateConfig:", "  backendType: LOCAL", "useSeparateCommitExecutor: true");
    NrtsearchConfig configuration = getConfig(configFile);
    GlobalState globalState = GlobalState.createState(configuration, null);

    boolean[] taskExecuted = {false};
    globalState.submitCommitTask(() -> taskExecuted[0] = true);

    // Wait a short time for the task to be executed
    Thread.sleep(100);

    // Verify the task was executed by the commit executor
    assertTrue("Commit task was not executed", taskExecuted[0]);
  }
}
