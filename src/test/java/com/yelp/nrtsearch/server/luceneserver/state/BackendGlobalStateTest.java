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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexGlobalState;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import com.yelp.nrtsearch.server.grpc.StopIndexRequest;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefCreator;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import com.yelp.nrtsearch.server.luceneserver.similarity.SimilarityCreator;
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
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Before;
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

  @Before
  public void setupTest() {
    MockBackendGlobalState.idCounter = 0;
  }

  static class MockBackendGlobalState extends BackendGlobalState {
    public static StateBackend stateBackend;
    public static Map<String, IndexStateManager> stateManagers;
    public static int idCounter = 0;

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

    @Override
    public IndexStateManager createIndexStateManager(
        String indexName, String indexId, StateBackend stateBackend) {
      return stateManagers.get(BackendGlobalState.getUniqueIndexName(indexName, indexId));
    }

    public String getIndexId() {
      String id = String.valueOf(idCounter);
      idCounter++;
      return id;
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
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "0"), mockManager);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.createIndex("test_index");

    assertEquals(1, backendGlobalState.getIndexNames().size());
    assertTrue(backendGlobalState.getIndexNames().contains("test_index"));
    assertSame(mockState, backendGlobalState.getIndex("test_index"));

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<GlobalStateInfo> stateArgumentCaptor =
        ArgumentCaptor.forClass(GlobalStateInfo.class);
    verify(mockBackend, times(1)).commitGlobalState(stateArgumentCaptor.capture());
    GlobalStateInfo committedState = stateArgumentCaptor.getValue();
    assertEquals(1, committedState.getGen());
    assertEquals(1, committedState.getIndicesMap().size());
    assertEquals(
        IndexGlobalState.newBuilder().setId("0").setStarted(false).build(),
        committedState.getIndicesMap().get("test_index"));

    verify(mockManager, times(2)).getCurrent();
    verify(mockManager, times(1)).create();

    verifyNoMoreInteractions(mockBackend, mockManager, mockState);
  }

  @Test
  public void testGetIndexStateManager() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "0"), mockManager);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.createIndex("test_index");

    assertSame(mockManager, backendGlobalState.getIndexStateManager("test_index"));

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<GlobalStateInfo> stateArgumentCaptor =
        ArgumentCaptor.forClass(GlobalStateInfo.class);
    verify(mockBackend, times(1)).commitGlobalState(stateArgumentCaptor.capture());
    GlobalStateInfo committedState = stateArgumentCaptor.getValue();
    assertEquals(1, committedState.getGen());
    assertEquals(1, committedState.getIndicesMap().size());
    assertEquals(
        IndexGlobalState.newBuilder().setId("0").setStarted(false).build(),
        committedState.getIndicesMap().get("test_index"));

    verify(mockManager, times(1)).getCurrent();
    verify(mockManager, times(1)).create();

    verifyNoMoreInteractions(mockBackend, mockManager, mockState);
  }

  @Test
  public void testGetIndexStateManagerNotPresent() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    try {
      backendGlobalState.getIndexStateManager("invalid");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("index \"invalid\" was not saved or committed", e.getMessage());
    }
  }

  @Test
  public void testCreateIndexMultiple() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "0"), mockManager);

    IndexStateManager mockManager2 = mock(IndexStateManager.class);
    IndexState mockState2 = mock(IndexState.class);
    when(mockManager2.getCurrent()).thenReturn(mockState2);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index_2", "1"), mockManager2);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.createIndex("test_index");
    backendGlobalState.createIndex("test_index_2");

    assertEquals(2, backendGlobalState.getIndexNames().size());
    assertTrue(backendGlobalState.getIndexNames().contains("test_index"));
    assertTrue(backendGlobalState.getIndexNames().contains("test_index_2"));
    assertSame(mockState, backendGlobalState.getIndex("test_index"));
    assertSame(mockState2, backendGlobalState.getIndex("test_index_2"));

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<GlobalStateInfo> stateArgumentCaptor =
        ArgumentCaptor.forClass(GlobalStateInfo.class);
    verify(mockBackend, times(2)).commitGlobalState(stateArgumentCaptor.capture());
    GlobalStateInfo committedState = stateArgumentCaptor.getAllValues().get(0);
    assertEquals(1, committedState.getGen());
    assertEquals(1, committedState.getIndicesMap().size());
    assertEquals(
        IndexGlobalState.newBuilder().setId("0").setStarted(false).build(),
        committedState.getIndicesMap().get("test_index"));
    committedState = stateArgumentCaptor.getAllValues().get(1);
    assertEquals(2, committedState.getGen());
    assertEquals(2, committedState.getIndicesMap().size());
    assertEquals(
        IndexGlobalState.newBuilder().setId("0").setStarted(false).build(),
        committedState.getIndicesMap().get("test_index"));
    assertEquals(
        IndexGlobalState.newBuilder().setId("1").setStarted(false).build(),
        committedState.getIndicesMap().get("test_index_2"));

    verify(mockManager, times(2)).getCurrent();
    verify(mockManager, times(1)).create();
    verify(mockManager2, times(2)).getCurrent();
    verify(mockManager2, times(1)).create();

    verifyNoMoreInteractions(mockBackend, mockManager, mockManager2, mockState, mockState2);
  }

  @Test
  public void testCreateIndexFails() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "0"), mockManager);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
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
    GlobalStateInfo initialState =
        GlobalStateInfo.newBuilder()
            .setGen(10)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(false).build())
            .build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "test_id_1"), mockManager);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
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
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "0"), mockManager);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.createIndex("test_index");

    assertEquals(1, backendGlobalState.getIndexNames().size());
    assertTrue(backendGlobalState.getIndexNames().contains("test_index"));
    IndexState createdState = backendGlobalState.getIndex("test_index");
    assertSame(mockState, createdState);

    IndexState getIndexState = backendGlobalState.getIndex("test_index");
    assertSame(createdState, getIndexState);

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<GlobalStateInfo> stateArgumentCaptor =
        ArgumentCaptor.forClass(GlobalStateInfo.class);
    verify(mockBackend, times(1)).commitGlobalState(stateArgumentCaptor.capture());
    GlobalStateInfo committedState = stateArgumentCaptor.getValue();
    assertEquals(1, committedState.getGen());
    assertEquals(1, committedState.getIndicesMap().size());
    assertEquals(
        IndexGlobalState.newBuilder().setId("0").setStarted(false).build(),
        committedState.getIndicesMap().get("test_index"));

    verify(mockManager, times(3)).getCurrent();
    verify(mockManager, times(1)).create();

    verifyNoMoreInteractions(mockBackend, mockManager, mockState);
  }

  @Test
  public void testGetRestoredStateIndex() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState =
        GlobalStateInfo.newBuilder()
            .setGen(11)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(false).build())
            .build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "test_id_1"), mockManager);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    assertSame(mockState, backendGlobalState.getIndex("test_index"));

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    verify(mockManager, times(1)).load();
    verify(mockManager, times(1)).getCurrent();

    verifyNoMoreInteractions(mockBackend, mockManager, mockState);
  }

  @Test
  public void testGetIndexNotExists() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
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
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "0"), mockManager);

    IndexStateManager mockManager2 = mock(IndexStateManager.class);
    IndexState mockState2 = mock(IndexState.class);
    when(mockManager2.getCurrent()).thenReturn(mockState2);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index_2", "1"), mockManager2);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.createIndex("test_index");
    backendGlobalState.createIndex("test_index_2");

    assertEquals(2, backendGlobalState.getIndexNames().size());
    assertTrue(backendGlobalState.getIndexNames().contains("test_index"));
    assertTrue(backendGlobalState.getIndexNames().contains("test_index_2"));
    assertSame(mockState, backendGlobalState.getIndex("test_index"));
    assertSame(mockState2, backendGlobalState.getIndex("test_index_2"));

    backendGlobalState.deleteIndex("test_index");
    assertEquals(1, backendGlobalState.getIndexNames().size());
    assertSame(mockState2, backendGlobalState.getIndex("test_index_2"));

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<GlobalStateInfo> stateArgumentCaptor =
        ArgumentCaptor.forClass(GlobalStateInfo.class);
    verify(mockBackend, times(3)).commitGlobalState(stateArgumentCaptor.capture());
    GlobalStateInfo committedState = stateArgumentCaptor.getAllValues().get(0);
    assertEquals(1, committedState.getGen());
    assertEquals(1, committedState.getIndicesMap().size());
    assertEquals(
        IndexGlobalState.newBuilder().setId("0").setStarted(false).build(),
        committedState.getIndicesMap().get("test_index"));
    committedState = stateArgumentCaptor.getAllValues().get(1);
    assertEquals(2, committedState.getGen());
    assertEquals(2, committedState.getIndicesMap().size());
    assertEquals(
        IndexGlobalState.newBuilder().setId("0").setStarted(false).build(),
        committedState.getIndicesMap().get("test_index"));
    assertEquals(
        IndexGlobalState.newBuilder().setId("1").setStarted(false).build(),
        committedState.getIndicesMap().get("test_index_2"));
    committedState = stateArgumentCaptor.getAllValues().get(2);
    assertEquals(3, committedState.getGen());
    assertEquals(1, committedState.getIndicesMap().size());
    assertEquals(
        IndexGlobalState.newBuilder().setId("1").setStarted(false).build(),
        committedState.getIndicesMap().get("test_index_2"));

    verify(mockManager, times(1)).create();
    verify(mockManager, times(2)).getCurrent();
    verify(mockManager, times(1)).close();

    verify(mockManager2, times(1)).create();
    verify(mockManager2, times(3)).getCurrent();

    verifyNoMoreInteractions(mockBackend, mockManager, mockManager2, mockState, mockState2);
  }

  @Test
  public void testDeleteRestoredStateIndex() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState =
        GlobalStateInfo.newBuilder()
            .setGen(14)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(false).build())
            .putIndices(
                "test_index_2",
                IndexGlobalState.newBuilder().setId("test_id_2").setStarted(false).build())
            .build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "test_id_1"), mockManager);

    IndexStateManager mockManager2 = mock(IndexStateManager.class);
    IndexState mockState2 = mock(IndexState.class);
    when(mockManager2.getCurrent()).thenReturn(mockState2);
    mockManagers.put(
        BackendGlobalState.getUniqueIndexName("test_index_2", "test_id_2"), mockManager2);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);

    backendGlobalState.deleteIndex("test_index_2");
    assertEquals(Set.of("test_index"), backendGlobalState.getIndexNames());

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<GlobalStateInfo> stateArgumentCaptor =
        ArgumentCaptor.forClass(GlobalStateInfo.class);
    verify(mockBackend, times(1)).commitGlobalState(stateArgumentCaptor.capture());
    GlobalStateInfo committedState = stateArgumentCaptor.getValue();
    assertEquals(15, committedState.getGen());
    assertEquals(1, committedState.getIndicesMap().size());
    assertEquals(
        committedState.getIndicesMap().get("test_index"),
        IndexGlobalState.newBuilder().setId("test_id_1").setStarted(false).build());

    verify(mockManager, times(1)).load();

    verify(mockManager2, times(1)).load();
    verify(mockManager2, times(1)).close();

    verifyNoMoreInteractions(mockBackend, mockManager, mockManager2, mockState, mockState2);
  }

  @Test
  public void testRecreateIndex() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "0"), mockManager);

    IndexStateManager mockManager2 = mock(IndexStateManager.class);
    IndexState mockState2 = mock(IndexState.class);
    when(mockManager2.getCurrent()).thenReturn(mockState2);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "1"), mockManager2);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.createIndex("test_index");

    assertEquals(1, backendGlobalState.getIndexNames().size());
    assertTrue(backendGlobalState.getIndexNames().contains("test_index"));
    assertSame(mockState, backendGlobalState.getIndex("test_index"));

    backendGlobalState.deleteIndex("test_index");
    assertEquals(0, backendGlobalState.getIndexNames().size());

    backendGlobalState.createIndex("test_index");

    assertEquals(1, backendGlobalState.getIndexNames().size());
    assertTrue(backendGlobalState.getIndexNames().contains("test_index"));
    assertSame(mockState2, backendGlobalState.getIndex("test_index"));

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<GlobalStateInfo> stateArgumentCaptor =
        ArgumentCaptor.forClass(GlobalStateInfo.class);
    verify(mockBackend, times(3)).commitGlobalState(stateArgumentCaptor.capture());
    GlobalStateInfo committedState = stateArgumentCaptor.getAllValues().get(0);
    assertEquals(1, committedState.getGen());
    assertEquals(1, committedState.getIndicesMap().size());
    assertEquals(
        IndexGlobalState.newBuilder().setId("0").build(),
        committedState.getIndicesMap().get("test_index"));
    committedState = stateArgumentCaptor.getAllValues().get(1);
    assertEquals(2, committedState.getGen());
    assertEquals(0, committedState.getIndicesMap().size());
    committedState = stateArgumentCaptor.getAllValues().get(2);
    assertEquals(3, committedState.getGen());
    assertEquals(1, committedState.getIndicesMap().size());
    assertEquals(
        IndexGlobalState.newBuilder().setId("1").build(),
        committedState.getIndicesMap().get("test_index"));

    verify(mockManager, times(1)).create();
    verify(mockManager, times(2)).getCurrent();
    verify(mockManager, times(1)).close();

    verify(mockManager2, times(1)).create();
    verify(mockManager2, times(2)).getCurrent();

    verifyNoMoreInteractions(mockBackend, mockManager, mockManager2, mockState, mockState2);
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
        GlobalStateInfo.newBuilder().build(), tmpStateFolder, StateUtils.GLOBAL_STATE_FILE);
    Archiver archiver = mock(Archiver.class);
    when(archiver.download(any(), any())).thenReturn(Paths.get(folder.getRoot().getAbsolutePath()));

    BackendGlobalState backendGlobalState = new BackendGlobalState(config, archiver);
    assertTrue(backendGlobalState.getStateBackend() instanceof RemoteStateBackend);
  }

  @Test
  public void testGetUniqueIndexName() {
    String uniqueName = BackendGlobalState.getUniqueIndexName("test_index", "test_id");
    assertEquals("test_index-test_id", uniqueName);
  }

  @Test(expected = NullPointerException.class)
  public void testGetUniqueIndexNameNullIndexName() {
    BackendGlobalState.getUniqueIndexName(null, "test_id");
  }

  @Test(expected = NullPointerException.class)
  public void testGetUniqueIndexNameNullId() {
    BackendGlobalState.getUniqueIndexName("test_index", null);
  }

  @Test
  public void testGetDataResourceForIndex() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "0"), mockManager);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.createIndex("test_index");

    assertEquals(
        BackendGlobalState.getUniqueIndexName("test_index", "0"),
        backendGlobalState.getDataResourceForIndex("test_index"));

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<GlobalStateInfo> stateArgumentCaptor =
        ArgumentCaptor.forClass(GlobalStateInfo.class);
    verify(mockBackend, times(1)).commitGlobalState(stateArgumentCaptor.capture());
    GlobalStateInfo committedState = stateArgumentCaptor.getValue();
    assertEquals(1, committedState.getGen());
    assertEquals(1, committedState.getIndicesMap().size());
    assertEquals(
        IndexGlobalState.newBuilder().setId("0").build(),
        committedState.getIndicesMap().get("test_index"));

    verify(mockManager, times(1)).getCurrent();
    verify(mockManager, times(1)).create();

    verifyNoMoreInteractions(mockBackend, mockManager, mockState);
  }

  @Test
  public void testGetDataResourceForIndexNotPresent() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    try {
      backendGlobalState.getDataResourceForIndex("invalid");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("index \"invalid\" was not saved or committed", e.getMessage());
    }
  }

  @Test
  public void testGetIndexDir() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    Path indexPath = Paths.get("test_path");
    when(mockState.getRootDir()).thenReturn(indexPath);
    when(mockManager.getCurrent()).thenReturn(mockState);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "0"), mockManager);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    backendGlobalState.createIndex("test_index");

    assertSame(indexPath, backendGlobalState.getIndexDir("test_index"));

    verify(mockBackend, times(1)).loadOrCreateGlobalState();

    ArgumentCaptor<GlobalStateInfo> stateArgumentCaptor =
        ArgumentCaptor.forClass(GlobalStateInfo.class);
    verify(mockBackend, times(1)).commitGlobalState(stateArgumentCaptor.capture());
    GlobalStateInfo committedState = stateArgumentCaptor.getValue();
    assertEquals(1, committedState.getGen());
    assertEquals(1, committedState.getIndicesMap().size());
    assertEquals(
        IndexGlobalState.newBuilder().setId("0").build(),
        committedState.getIndicesMap().get("test_index"));

    verify(mockManager, times(2)).getCurrent();
    verify(mockManager, times(1)).create();

    verify(mockState, times(1)).getRootDir();

    verifyNoMoreInteractions(mockBackend, mockManager, mockState);
  }

  @Test
  public void testStartIndexNotExists() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    StartIndexRequest request = StartIndexRequest.newBuilder().setIndexName("test_index").build();
    try {
      backendGlobalState.startIndex(request);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("index \"test_index\" was not saved or committed", e.getMessage());
    }
  }

  @Test
  public void testStartIndexAlreadyStarted() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState =
        GlobalStateInfo.newBuilder()
            .setGen(14)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(true).build())
            .build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    when(mockState.isStarted()).thenReturn(true);
    when(mockState.getName()).thenReturn("test_index");
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "test_id_1"), mockManager);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    StartIndexRequest request = StartIndexRequest.newBuilder().setIndexName("test_index").build();
    try {
      backendGlobalState.startIndex(request);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Index test_index is already started", e.getMessage());
    }
  }

  @Test
  public void testStartIndex() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState =
        GlobalStateInfo.newBuilder()
            .setGen(15)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(false).build())
            .build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    when(mockState.isStarted()).thenReturn(false);
    when(mockState.getName()).thenReturn("test_index");
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "test_id_1"), mockManager);

    ShardState mockShardState = mock(ShardState.class);
    IndexReader mockReader = mock(IndexReader.class);
    IndexSearcher mockSearcher = mock(IndexSearcher.class);
    when(mockState.getShard(0)).thenReturn(mockShardState);
    when(mockSearcher.getIndexReader()).thenReturn(mockReader);
    when(mockShardState.acquire()).thenReturn(new SearcherAndTaxonomy(mockSearcher, null));

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    StartIndexRequest request = StartIndexRequest.newBuilder().setIndexName("test_index").build();
    backendGlobalState.startIndex(request);

    GlobalStateInfo updatedGlobalState =
        GlobalStateInfo.newBuilder()
            .setGen(16)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(true).build())
            .build();

    verify(mockBackend, times(1)).loadOrCreateGlobalState();
    verify(mockBackend, times(1)).commitGlobalState(updatedGlobalState);
    verify(mockManager, times(1)).load();
    verify(mockManager, times(1)).getCurrent();
    verify(mockManager, times(1)).start(eq(Mode.STANDALONE), eq(null), eq(-1L), eq(null));
    verify(mockState, times(1)).isStarted();
    verify(mockState, times(1)).getShard(0);
    verify(mockShardState, times(1)).acquire();
    verify(mockShardState, times(1)).release(any(SearcherAndTaxonomy.class));
    verify(mockSearcher, times(1)).getIndexReader();
    verify(mockReader, times(1)).numDocs();
    verify(mockReader, times(1)).maxDoc();

    verifyNoMoreInteractions(
        mockBackend, mockManager, mockState, mockShardState, mockSearcher, mockReader);
  }

  @Test
  public void testStartIndexPrimary() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState =
        GlobalStateInfo.newBuilder()
            .setGen(15)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(false).build())
            .build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    when(mockState.isStarted()).thenReturn(false);
    when(mockState.getName()).thenReturn("test_index");
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "test_id_1"), mockManager);

    ShardState mockShardState = mock(ShardState.class);
    IndexReader mockReader = mock(IndexReader.class);
    IndexSearcher mockSearcher = mock(IndexSearcher.class);
    when(mockState.getShard(0)).thenReturn(mockShardState);
    when(mockSearcher.getIndexReader()).thenReturn(mockReader);
    when(mockShardState.acquire()).thenReturn(new SearcherAndTaxonomy(mockSearcher, null));

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    StartIndexRequest request =
        StartIndexRequest.newBuilder()
            .setIndexName("test_index")
            .setMode(Mode.PRIMARY)
            .setPrimaryGen(1)
            .build();
    backendGlobalState.startIndex(request);

    GlobalStateInfo updatedGlobalState =
        GlobalStateInfo.newBuilder()
            .setGen(16)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(true).build())
            .build();

    verify(mockBackend, times(1)).loadOrCreateGlobalState();
    verify(mockBackend, times(1)).commitGlobalState(updatedGlobalState);
    verify(mockManager, times(1)).load();
    verify(mockManager, times(1)).getCurrent();
    verify(mockManager, times(1)).start(eq(Mode.PRIMARY), eq(null), eq(1L), eq(null));
    verify(mockState, times(1)).isStarted();
    verify(mockState, times(1)).getShard(0);
    verify(mockShardState, times(1)).acquire();
    verify(mockShardState, times(1)).release(any(SearcherAndTaxonomy.class));
    verify(mockSearcher, times(1)).getIndexReader();
    verify(mockReader, times(1)).numDocs();
    verify(mockReader, times(1)).maxDoc();

    verifyNoMoreInteractions(
        mockBackend, mockManager, mockState, mockShardState, mockSearcher, mockReader);
  }

  @Test
  public void testStartIndexReplica() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState =
        GlobalStateInfo.newBuilder()
            .setGen(15)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(false).build())
            .build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    when(mockState.isStarted()).thenReturn(false);
    when(mockState.getName()).thenReturn("test_index");
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "test_id_1"), mockManager);

    ShardState mockShardState = mock(ShardState.class);
    IndexReader mockReader = mock(IndexReader.class);
    IndexSearcher mockSearcher = mock(IndexSearcher.class);
    when(mockState.getShard(0)).thenReturn(mockShardState);
    when(mockSearcher.getIndexReader()).thenReturn(mockReader);
    when(mockShardState.acquire()).thenReturn(new SearcherAndTaxonomy(mockSearcher, null));

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    StartIndexRequest request =
        StartIndexRequest.newBuilder()
            .setIndexName("test_index")
            .setMode(Mode.REPLICA)
            .setPrimaryGen(1)
            .setPrimaryAddress("localhost")
            .build();
    backendGlobalState.startIndex(request);

    verify(mockBackend, times(1)).loadOrCreateGlobalState();
    verify(mockManager, times(1)).load();
    verify(mockManager, times(1)).getCurrent();
    verify(mockManager, times(1))
        .start(eq(Mode.REPLICA), eq(null), eq(1L), any(ReplicationServerClient.class));
    verify(mockState, times(1)).isStarted();
    verify(mockState, times(1)).getShard(0);
    verify(mockState, times(1)).initWarmer(any());
    verify(mockShardState, times(1)).acquire();
    verify(mockShardState, times(1)).release(any(SearcherAndTaxonomy.class));
    verify(mockSearcher, times(1)).getIndexReader();
    verify(mockReader, times(1)).numDocs();
    verify(mockReader, times(1)).maxDoc();

    verifyNoMoreInteractions(
        mockBackend, mockManager, mockState, mockShardState, mockSearcher, mockReader);
  }

  @Test
  public void testStopIndexNotExists() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    StopIndexRequest request = StopIndexRequest.newBuilder().setIndexName("test_index").build();
    try {
      backendGlobalState.stopIndex(request);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("index \"test_index\" was not saved or committed", e.getMessage());
    }
  }

  @Test
  public void testStopIndexNotStarted() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState =
        GlobalStateInfo.newBuilder()
            .setGen(14)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(false).build())
            .build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    when(mockState.isStarted()).thenReturn(false);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "test_id_1"), mockManager);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    StopIndexRequest request = StopIndexRequest.newBuilder().setIndexName("test_index").build();
    try {
      backendGlobalState.stopIndex(request);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Index \"test_index\" is not started", e.getMessage());
    }
  }

  @Test
  public void testStopIndex() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState =
        GlobalStateInfo.newBuilder()
            .setGen(21)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(true).build())
            .build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    when(mockState.isStarted()).thenReturn(true);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "test_id_1"), mockManager);

    ShardState mockShardState = mock(ShardState.class);
    when(mockState.getShard(0)).thenReturn(mockShardState);
    when(mockShardState.isReplica()).thenReturn(false);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    StopIndexRequest request = StopIndexRequest.newBuilder().setIndexName("test_index").build();

    backendGlobalState.stopIndex(request);

    GlobalStateInfo updatedGlobalState =
        GlobalStateInfo.newBuilder()
            .setGen(22)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(false).build())
            .build();

    verify(mockBackend, times(1)).loadOrCreateGlobalState();
    verify(mockBackend, times(1)).commitGlobalState(updatedGlobalState);
    verify(mockManager, times(1)).load();
    verify(mockManager, times(1)).close();
    verify(mockManager, times(2)).getCurrent();
    verify(mockState, times(1)).isStarted();
    verify(mockState, times(1)).getShard(0);
    verify(mockShardState, times(1)).isReplica();

    verifyNoMoreInteractions(mockBackend, mockManager, mockState, mockShardState);
  }

  @Test
  public void testStopIndexReplica() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState =
        GlobalStateInfo.newBuilder()
            .setGen(21)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(true).build())
            .build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    when(mockState.isStarted()).thenReturn(true);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "test_id_1"), mockManager);

    ShardState mockShardState = mock(ShardState.class);
    when(mockState.getShard(0)).thenReturn(mockShardState);
    when(mockShardState.isReplica()).thenReturn(true);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    StopIndexRequest request = StopIndexRequest.newBuilder().setIndexName("test_index").build();

    backendGlobalState.stopIndex(request);

    verify(mockBackend, times(1)).loadOrCreateGlobalState();
    verify(mockManager, times(1)).load();
    verify(mockManager, times(1)).close();
    verify(mockManager, times(2)).getCurrent();
    verify(mockState, times(1)).isStarted();
    verify(mockState, times(1)).getShard(0);
    verify(mockShardState, times(1)).isReplica();

    verifyNoMoreInteractions(mockBackend, mockManager, mockState, mockShardState);
  }

  @Test
  public void testGetIndicesToStartEmpty() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    MockBackendGlobalState.stateBackend = mockBackend;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);

    assertEquals(Collections.emptySet(), backendGlobalState.getIndexNames());
    assertEquals(Collections.emptySet(), backendGlobalState.getIndicesToStart());
  }

  @Test
  public void testGetIndicesToStart() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState =
        GlobalStateInfo.newBuilder()
            .setGen(21)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(true).build())
            .build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    when(mockState.isStarted()).thenReturn(true);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "test_id_1"), mockManager);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);

    assertEquals(Collections.singleton("test_index"), backendGlobalState.getIndexNames());
    assertEquals(Collections.singleton("test_index"), backendGlobalState.getIndicesToStart());
  }

  @Test
  public void testGetIndicesToStartOnlyStarted() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState =
        GlobalStateInfo.newBuilder()
            .setGen(21)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(false).build())
            .putIndices(
                "test_index_2",
                IndexGlobalState.newBuilder().setId("test_id_2").setStarted(true).build())
            .build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    when(mockState.isStarted()).thenReturn(false);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "test_id_1"), mockManager);
    IndexStateManager mockManager2 = mock(IndexStateManager.class);
    IndexState mockState2 = mock(IndexState.class);
    when(mockManager2.getCurrent()).thenReturn(mockState2);
    when(mockState2.isStarted()).thenReturn(true);
    mockManagers.put(
        BackendGlobalState.getUniqueIndexName("test_index_2", "test_id_2"), mockManager2);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);

    assertEquals(Set.of("test_index", "test_index_2"), backendGlobalState.getIndexNames());
    assertEquals(Collections.singleton("test_index_2"), backendGlobalState.getIndicesToStart());
  }

  @Test
  public void testGetResolvedReplicationPort() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalStateInfo initialState = GlobalStateInfo.newBuilder().build();
    when(mockBackend.loadOrCreateGlobalState()).thenReturn(initialState);

    Map<String, IndexStateManager> mockManagers = new HashMap<>();
    IndexStateManager mockManager = mock(IndexStateManager.class);
    IndexState mockState = mock(IndexState.class);
    when(mockManager.getCurrent()).thenReturn(mockState);
    mockManagers.put(BackendGlobalState.getUniqueIndexName("test_index", "0"), mockManager);

    MockBackendGlobalState.stateBackend = mockBackend;
    MockBackendGlobalState.stateManagers = mockManagers;
    BackendGlobalState backendGlobalState = new MockBackendGlobalState(getConfig(), null);
    assertEquals(0, backendGlobalState.getReplicationPort());
    backendGlobalState.replicationStarted(100);
    assertEquals(100, backendGlobalState.getReplicationPort());
  }
}
