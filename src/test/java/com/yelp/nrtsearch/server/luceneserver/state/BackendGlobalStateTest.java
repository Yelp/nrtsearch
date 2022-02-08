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
package com.yelp.nrtsearch.server.luceneserver.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefCreator;
import com.yelp.nrtsearch.server.luceneserver.similarity.SimilarityCreator;
import com.yelp.nrtsearch.server.luceneserver.state.PersistentGlobalState.IndexInfo;
import com.yelp.nrtsearch.server.luceneserver.state.backend.LocalStateBackend;
import com.yelp.nrtsearch.server.luceneserver.state.backend.RemoteStateBackend;
import com.yelp.nrtsearch.server.luceneserver.state.backend.StateBackend;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

public class BackendGlobalStateTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void setup() {
    String configFile = "nodeName: \"lucene_server_foo\"";
    LuceneServerConfiguration dummyConfig =
        new LuceneServerConfiguration(new ByteArrayInputStream(configFile.getBytes()));
    List<Plugin> dummyPlugins = Collections.emptyList();
    // these must be initialized to create an IndexState
    FieldDefCreator.initialize(dummyConfig, dummyPlugins);
    SimilarityCreator.initialize(dummyConfig, dummyPlugins);
  }

  static class MockBackendGlobalState extends BackendGlobalState {
    public static StateBackend stateBackend;

    /**
     * Constructor.
     *
     * @param luceneServerConfiguration server config
     * @param incArchiver archiver for remote backends
     * @throws IOException on filesystem error
     */
    public MockBackendGlobalState(
        LuceneServerConfiguration luceneServerConfiguration, Archiver incArchiver)
        throws IOException {
      super(luceneServerConfiguration, incArchiver);
    }

    @Override
    public StateBackend createStateBackend() {
      return stateBackend;
    }
  }

  private LuceneServerConfiguration getConfig() throws IOException {
    String configFile =
        String.join(
            "\n",
            "stateConfig:",
            "  backendType: LOCAL",
            "stateDir: " + folder.newFolder("state").getAbsolutePath(),
            "indexDir: " + folder.newFolder("index").getAbsolutePath());
    return new LuceneServerConfiguration(new ByteArrayInputStream(configFile.getBytes()));
  }

  @Test
  public void testCreateNewIndex() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    PersistentGlobalState initialState = new PersistentGlobalState();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.createIndex("test_index");

    assertEquals(1, backendGlobalState.getIndexNames().size());
    assertTrue(backendGlobalState.getIndexNames().contains("test_index"));
    assertNotNull(backendGlobalState.getIndex("test_index"));

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<PersistentGlobalState> stateArgumentCaptor =
        ArgumentCaptor.forClass(PersistentGlobalState.class);
    verify(mockBackend, times(1)).commitGlobalState(stateArgumentCaptor.capture());
    PersistentGlobalState committedState = stateArgumentCaptor.getValue();
    assertEquals(1, committedState.getIndices().size());
    assertNotNull(committedState.getIndices().get("test_index"));

    verifyNoMoreInteractions(mockBackend);
  }

  @Test
  public void testCreateIndexMultiple() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    PersistentGlobalState initialState = new PersistentGlobalState();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.createIndex("test_index");
    backendGlobalState.createIndex("test_index_2");

    assertEquals(2, backendGlobalState.getIndexNames().size());
    assertTrue(backendGlobalState.getIndexNames().contains("test_index"));
    assertTrue(backendGlobalState.getIndexNames().contains("test_index_2"));
    assertNotNull(backendGlobalState.getIndex("test_index"));
    assertNotNull(backendGlobalState.getIndex("test_index_2"));

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<PersistentGlobalState> stateArgumentCaptor =
        ArgumentCaptor.forClass(PersistentGlobalState.class);
    verify(mockBackend, times(2)).commitGlobalState(stateArgumentCaptor.capture());
    PersistentGlobalState committedState = stateArgumentCaptor.getAllValues().get(0);
    assertEquals(1, committedState.getIndices().size());
    assertNotNull(committedState.getIndices().get("test_index"));
    committedState = stateArgumentCaptor.getAllValues().get(1);
    assertEquals(2, committedState.getIndices().size());
    assertNotNull(committedState.getIndices().get("test_index"));
    assertNotNull(committedState.getIndices().get("test_index_2"));

    verifyNoMoreInteractions(mockBackend);
  }

  @Test
  public void testCreateIndexFails() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    PersistentGlobalState initialState = new PersistentGlobalState();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.createIndex("test_index");
    try {
      backendGlobalState.createIndex("test_index");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("index \"test_index\" already exists", e.getMessage());
    }
  }

  @Test
  public void testCreateIndexFailsFromLoadedState() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    Map<String, IndexInfo> initialIndices = new HashMap<>();
    initialIndices.put("test_index", new IndexInfo());
    PersistentGlobalState initialState = new PersistentGlobalState(initialIndices);
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    try {
      backendGlobalState.createIndex("test_index");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("index \"test_index\" already exists", e.getMessage());
    }
  }

  @Test
  public void testGetCreatedIndex() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    PersistentGlobalState initialState = new PersistentGlobalState();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.createIndex("test_index");

    assertEquals(1, backendGlobalState.getIndexNames().size());
    assertTrue(backendGlobalState.getIndexNames().contains("test_index"));
    IndexState createdState = backendGlobalState.getIndex("test_index");
    assertNotNull(createdState);

    IndexState getIndexState = backendGlobalState.getIndex("test_index");
    assertSame(createdState, getIndexState);

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<PersistentGlobalState> stateArgumentCaptor =
        ArgumentCaptor.forClass(PersistentGlobalState.class);
    verify(mockBackend, times(1)).commitGlobalState(stateArgumentCaptor.capture());
    PersistentGlobalState committedState = stateArgumentCaptor.getValue();
    assertEquals(1, committedState.getIndices().size());
    assertNotNull(committedState.getIndices().get("test_index"));

    verifyNoMoreInteractions(mockBackend);
  }

  @Test
  public void testGetRestoredStateIndex() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    Map<String, IndexInfo> initialIndices = new HashMap<>();
    initialIndices.put("test_index", new IndexInfo());
    PersistentGlobalState initialState = new PersistentGlobalState(initialIndices);
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    assertNotNull(backendGlobalState.getIndex("test_index"));

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    verifyNoMoreInteractions(mockBackend);
  }

  @Test
  public void testGetIndexNotExists() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    PersistentGlobalState initialState = new PersistentGlobalState();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    try {
      backendGlobalState.getIndex("test_index");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("index \"test_index\" was not saved or committed", e.getMessage());
    }
  }

  @Test
  public void testDeleteCreatedIndex() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    PersistentGlobalState initialState = new PersistentGlobalState();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.createIndex("test_index");
    backendGlobalState.createIndex("test_index_2");

    assertEquals(2, backendGlobalState.getIndexNames().size());
    assertTrue(backendGlobalState.getIndexNames().contains("test_index"));
    assertTrue(backendGlobalState.getIndexNames().contains("test_index_2"));
    assertNotNull(backendGlobalState.getIndex("test_index"));
    assertNotNull(backendGlobalState.getIndex("test_index_2"));

    backendGlobalState.deleteIndex("test_index");
    assertEquals(1, backendGlobalState.getIndexNames().size());
    assertNotNull(backendGlobalState.getIndex("test_index_2"));

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<PersistentGlobalState> stateArgumentCaptor =
        ArgumentCaptor.forClass(PersistentGlobalState.class);
    verify(mockBackend, times(3)).commitGlobalState(stateArgumentCaptor.capture());
    PersistentGlobalState committedState = stateArgumentCaptor.getAllValues().get(0);
    assertEquals(1, committedState.getIndices().size());
    assertNotNull(committedState.getIndices().get("test_index"));
    committedState = stateArgumentCaptor.getAllValues().get(1);
    assertEquals(2, committedState.getIndices().size());
    assertNotNull(committedState.getIndices().get("test_index"));
    assertNotNull(committedState.getIndices().get("test_index_2"));
    committedState = stateArgumentCaptor.getAllValues().get(2);
    assertEquals(1, committedState.getIndices().size());
    assertNotNull(committedState.getIndices().get("test_index_2"));

    verifyNoMoreInteractions(mockBackend);
  }

  @Test
  public void testDeleteRestoredStateIndex() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    Map<String, IndexInfo> initialIndices = new HashMap<>();
    initialIndices.put("test_index", new IndexInfo());
    initialIndices.put("test_index_2", new IndexInfo());
    PersistentGlobalState initialState = new PersistentGlobalState(initialIndices);
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);

    backendGlobalState.deleteIndex("test_index_2");
    assertEquals(Set.of("test_index"), backendGlobalState.getIndexNames());

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<PersistentGlobalState> stateArgumentCaptor =
        ArgumentCaptor.forClass(PersistentGlobalState.class);
    verify(mockBackend, times(1)).commitGlobalState(stateArgumentCaptor.capture());
    PersistentGlobalState committedState = stateArgumentCaptor.getValue();
    assertEquals(1, committedState.getIndices().size());
    assertNotNull(committedState.getIndices().get("test_index"));

    verifyNoMoreInteractions(mockBackend);
  }

  @Test
  public void testIndexClosed() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    PersistentGlobalState initialState = new PersistentGlobalState();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.createIndex("test_index");

    IndexState state1 = backendGlobalState.getIndex("test_index");
    IndexState state2 = backendGlobalState.getIndex("test_index");
    assertSame(state1, state2);

    backendGlobalState.indexClosed("test_index");
    IndexState state3 = backendGlobalState.getIndex("test_index");
    assertNotSame(state1, state3);

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<PersistentGlobalState> stateArgumentCaptor =
        ArgumentCaptor.forClass(PersistentGlobalState.class);
    verify(mockBackend, times(1)).commitGlobalState(stateArgumentCaptor.capture());
    PersistentGlobalState committedState = stateArgumentCaptor.getValue();
    assertEquals(1, committedState.getIndices().size());
    assertNotNull(committedState.getIndices().get("test_index"));

    verifyNoMoreInteractions(mockBackend);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testSetStateDir() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    PersistentGlobalState initialState = new PersistentGlobalState();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.setStateDir(folder.getRoot().toPath());
  }

  @Test
  public void testUseLocalBackend() throws IOException {
    String configFile =
        String.join(
            "\n",
            "stateConfig:",
            "  backendType: LOCAL",
            "stateDir: " + folder.newFolder("state").getAbsolutePath(),
            "indexDir: " + folder.newFolder("index").getAbsolutePath());
    LuceneServerConfiguration config =
        new LuceneServerConfiguration(new ByteArrayInputStream(configFile.getBytes()));
    BackendGlobalState backendGlobalState = new BackendGlobalState(config, null);
    assertTrue(backendGlobalState.getStateBackend() instanceof LocalStateBackend);
  }

  @Test
  public void testUseRemoteBackend() throws IOException {
    String configFile =
        String.join(
            "\n",
            "stateConfig:",
            "  backendType: REMOTE",
            "stateDir: " + folder.newFolder("state").getAbsolutePath(),
            "indexDir: " + folder.newFolder("index").getAbsolutePath());
    LuceneServerConfiguration config =
        new LuceneServerConfiguration(new ByteArrayInputStream(configFile.getBytes()));

    Path tmpStateFolder =
        Paths.get(folder.getRoot().getAbsolutePath(), StateUtils.GLOBAL_STATE_FOLDER);
    StateUtils.ensureDirectory(tmpStateFolder);
    StateUtils.writeStateToFile(
        new PersistentGlobalState(), tmpStateFolder, StateUtils.GLOBAL_STATE_FILE);
    Archiver archiver = mock(Archiver.class);
    when(archiver.download(any(), any())).thenReturn(Paths.get(folder.getRoot().getAbsolutePath()));

    BackendGlobalState backendGlobalState = new BackendGlobalState(config, archiver);
    assertTrue(backendGlobalState.getStateBackend() instanceof RemoteStateBackend);
  }

  @Test
  public void testInvalidBackend() throws IOException {
    String configFile =
        String.join(
            "\n",
            "stateConfig:",
            "  backendType: LEGACY",
            "stateDir: " + folder.newFolder("state").getAbsolutePath(),
            "indexDir: " + folder.newFolder("index").getAbsolutePath());
    LuceneServerConfiguration config =
        new LuceneServerConfiguration(new ByteArrayInputStream(configFile.getBytes()));
    try {
      new BackendGlobalState(config, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Unsupported state backend type: LEGACY", e.getMessage());
    }
  }
}
