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
package com.yelp.nrtsearch.server.luceneserver.state.backend;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.PersistentGlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.PersistentGlobalState.IndexInfo;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LocalStateBackendTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private LuceneServerConfiguration getConfig() throws IOException {
    String configFile =
        String.join(
            "\n",
            "stateConfig:",
            "  backendType: LOCAL",
            "stateDir: " + folder.getRoot().getAbsolutePath());
    return new LuceneServerConfiguration(new ByteArrayInputStream(configFile.getBytes()));
  }

  private GlobalState getMockGlobalState() throws IOException {
    GlobalState mockState = mock(GlobalState.class);
    LuceneServerConfiguration serverConfiguration = getConfig();
    when(mockState.getConfiguration()).thenReturn(serverConfiguration);
    when(mockState.getStateDir()).thenReturn(Paths.get(serverConfiguration.getStateDir()));
    return mockState;
  }

  private Path getStateFilePath() {
    return Paths.get(
        folder.getRoot().getAbsolutePath(),
        StateUtils.GLOBAL_STATE_FOLDER,
        StateUtils.GLOBAL_STATE_FILE);
  }

  @Test
  public void testCreatesGlobalStateDir() throws IOException {
    Path stateDir = Paths.get(folder.getRoot().getAbsolutePath(), StateUtils.GLOBAL_STATE_FOLDER);
    assertFalse(stateDir.toFile().exists());

    new LocalStateBackend(getMockGlobalState());
    assertTrue(stateDir.toFile().exists());
    assertTrue(stateDir.toFile().isDirectory());
  }

  @Test
  public void testCreatesDefaultState() throws IOException {
    StateBackend stateBackend = new LocalStateBackend(getMockGlobalState());
    Path filePath = getStateFilePath();
    assertFalse(filePath.toFile().exists());

    PersistentGlobalState globalState = stateBackend.loadOrCreateGlobalState();
    assertEquals(globalState, new PersistentGlobalState());

    assertTrue(filePath.toFile().exists());
    assertTrue(filePath.toFile().isFile());

    PersistentGlobalState loadedState = StateUtils.readStateFromFile(filePath);
    assertEquals(globalState, loadedState);
  }

  @Test
  public void testLoadsSavedState() throws IOException {
    StateBackend stateBackend = new LocalStateBackend(getMockGlobalState());
    Path filePath = getStateFilePath();

    Map<String, IndexInfo> indicesMap = new HashMap<>();
    indicesMap.put("test_index", new IndexInfo());
    indicesMap.put("test_index_2", new IndexInfo());
    PersistentGlobalState initialState = new PersistentGlobalState(indicesMap);
    StateUtils.writeStateToFile(
        initialState,
        Paths.get(folder.getRoot().getAbsolutePath(), StateUtils.GLOBAL_STATE_FOLDER),
        StateUtils.GLOBAL_STATE_FILE);
    assertTrue(filePath.toFile().exists());
    assertTrue(filePath.toFile().isFile());

    PersistentGlobalState loadedState = stateBackend.loadOrCreateGlobalState();
    assertEquals(initialState, loadedState);
  }

  @Test
  public void testCommitGlobalState() throws IOException {
    StateBackend stateBackend = new LocalStateBackend(getMockGlobalState());
    Path filePath = getStateFilePath();
    PersistentGlobalState initialState = stateBackend.loadOrCreateGlobalState();

    Map<String, IndexInfo> indicesMap = new HashMap<>();
    indicesMap.put("test_index", new IndexInfo());
    indicesMap.put("test_index_2", new IndexInfo());
    PersistentGlobalState updatedState = new PersistentGlobalState(indicesMap);
    assertNotEquals(initialState, updatedState);

    stateBackend.commitGlobalState(updatedState);
    PersistentGlobalState loadedState = StateUtils.readStateFromFile(filePath);
    assertEquals(updatedState, loadedState);

    indicesMap = new HashMap<>();
    indicesMap.put("test_index_3", new IndexInfo());
    PersistentGlobalState updatedState2 = new PersistentGlobalState(indicesMap);
    assertNotEquals(updatedState, updatedState2);
    stateBackend.commitGlobalState(updatedState2);

    loadedState = StateUtils.readStateFromFile(filePath);
    assertEquals(updatedState2, loadedState);
  }

  @Test(expected = NullPointerException.class)
  public void testCommitNullState() throws IOException {
    StateBackend stateBackend = new LocalStateBackend(getMockGlobalState());
    stateBackend.loadOrCreateGlobalState();
    stateBackend.commitGlobalState(null);
  }

  @Test
  public void testStateFileIsDirectory() throws IOException {
    StateBackend stateBackend = new LocalStateBackend(getMockGlobalState());
    StateUtils.ensureDirectory(
        Paths.get(
            folder.getRoot().getAbsolutePath(),
            StateUtils.GLOBAL_STATE_FOLDER,
            StateUtils.GLOBAL_STATE_FILE));
    try {
      stateBackend.loadOrCreateGlobalState();
      fail();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("state.json is a directory"));
    }
  }
}
