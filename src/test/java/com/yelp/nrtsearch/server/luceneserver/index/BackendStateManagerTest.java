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
package com.yelp.nrtsearch.server.luceneserver.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.BoolValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefCreator;
import com.yelp.nrtsearch.server.luceneserver.similarity.SimilarityCreator;
import com.yelp.nrtsearch.server.luceneserver.state.BackendGlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.backend.StateBackend;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BackendStateManagerTest {

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
    MockStateManager.verifyFieldsState = Assert::assertNotNull;
  }

  private static class MockStateManager extends BackendStateManager {
    public static ImmutableIndexState nextState;
    public static IndexStateInfo expectedState;
    public static Consumer<FieldAndFacetState> verifyFieldsState = Assert::assertNotNull;
    public static FieldAndFacetState lastFieldAndFacetState;

    /**
     * Constructor
     *
     * @param indexName index name
     * @param id index instance id
     * @param stateBackend state backend
     * @param globalState global state
     */
    public MockStateManager(
        String indexName, String id, StateBackend stateBackend, GlobalState globalState) {
      super(indexName, id, stateBackend, globalState);
    }

    @Override
    public ImmutableIndexState createIndexState(
        IndexStateInfo indexStateInfo, FieldAndFacetState fieldAndFacetState) {
      assertEquals(expectedState, indexStateInfo);
      verifyFieldsState.accept(fieldAndFacetState);
      lastFieldAndFacetState = fieldAndFacetState;
      return nextState;
    }
  }

  @Test
  public void testLoadsExistingState() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder().setIndexName("test_index").setCommitted(false).build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState);
  }

  @Test
  public void testLoadFixesIndexName() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder().setIndexName("not_test_index").setCommitted(false).build();
    IndexStateInfo expectedState =
        IndexStateInfo.newBuilder().setIndexName("test_index").setCommitted(false).build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = expectedState;

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState);
  }

  @Test
  public void testLoadNoExistingState() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(null);

    try {
      stateManager.load();
      fail();
    } catch (IllegalStateException e) {
      assertEquals("No committed state for index: test_index", e.getMessage());
    }
  }

  @Test
  public void testCreateIndexState() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(null);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = stateManager.getDefaultStateInfo();

    stateManager.create();
    assertSame(mockState, stateManager.getCurrent());

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));
    verify(mockBackend, times(1))
        .commitIndexState(
            BackendGlobalState.getUniqueIndexName("test_index", "test_id"),
            stateManager.getDefaultStateInfo());

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState);
  }

  @Test
  public void testCreateIndexExists() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder().setIndexName("test_index").setCommitted(true).build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    try {
      stateManager.create();
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Creating index, but state already exists for: test_index", e.getMessage());
    }
  }

  @Test
  public void testGetSettings() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexSettings indexSettings =
        IndexSettings.newBuilder()
            .setConcurrentMergeSchedulerMaxMergeCount(Int32Value.newBuilder().setValue(10).build())
            .setConcurrentMergeSchedulerMaxThreadCount(Int32Value.newBuilder().setValue(5).build())
            .setDirectory(StringValue.newBuilder().setValue("MMapDirectory").build())
            .build();
    IndexStateInfo initialState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index")
            .setCommitted(true)
            .setSettings(indexSettings)
            .build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.getMergedSettings()).thenReturn(indexSettings);
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    assertEquals(indexSettings, stateManager.getSettings());

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));
    verify(mockState, times(1)).getMergedSettings();

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState);
  }

  @Test
  public void testGetSettingsNoExistingState() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(null);

    try {
      stateManager.getSettings();
      fail();
    } catch (IllegalStateException e) {
      assertEquals("No state for index: test_index", e.getMessage());
    }
  }

  @Test
  public void testUpdateSettingsNoop() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index")
            .setCommitted(true)
            .setSettings(ImmutableIndexState.DEFAULT_INDEX_SETTINGS)
            .build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.isStarted()).thenReturn(false);
    when(mockState.getCurrentStateInfo()).thenReturn(initialState);
    when(mockState.getFieldAndFacetState()).thenReturn(mock(FieldAndFacetState.class));
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    ImmutableIndexState mockState2 = mock(ImmutableIndexState.class);
    when(mockState2.getMergedSettings()).thenReturn(ImmutableIndexState.DEFAULT_INDEX_SETTINGS);

    IndexStateInfo expectedStateInfo = initialState.toBuilder().setGen(1).build();
    MockStateManager.nextState = mockState2;
    MockStateManager.expectedState = expectedStateInfo;

    assertEquals(
        ImmutableIndexState.DEFAULT_INDEX_SETTINGS,
        stateManager.updateSettings(IndexSettings.newBuilder().build()));

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));
    verify(mockBackend, times(1))
        .commitIndexState(
            BackendGlobalState.getUniqueIndexName("test_index", "test_id"), expectedStateInfo);
    verify(mockState, times(1)).isStarted();
    verify(mockState, times(1)).getCurrentStateInfo();
    verify(mockState, times(1)).getFieldAndFacetState();
    verify(mockState2, times(1)).getMergedSettings();

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState, mockState2);
  }

  @Test
  public void testUpdateEmptySettings() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index")
            .setCommitted(true)
            .setSettings(IndexSettings.newBuilder().build())
            .build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.isStarted()).thenReturn(false);
    when(mockState.getCurrentStateInfo()).thenReturn(initialState);
    when(mockState.getFieldAndFacetState()).thenReturn(mock(FieldAndFacetState.class));
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    IndexSettings settingsUpdate =
        IndexSettings.newBuilder()
            .setDirectory(StringValue.newBuilder().setValue("MMapDirectory").build())
            .setIndexMergeSchedulerAutoThrottle(BoolValue.newBuilder().setValue(true).build())
            .setNrtCachingDirectoryMaxSizeMB(DoubleValue.newBuilder().setValue(75.0).build())
            .build();
    IndexSettings expectedMergedSettings =
        ImmutableIndexState.DEFAULT_INDEX_SETTINGS
            .toBuilder()
            .setDirectory(StringValue.newBuilder().setValue("MMapDirectory").build())
            .setIndexMergeSchedulerAutoThrottle(BoolValue.newBuilder().setValue(true).build())
            .setNrtCachingDirectoryMaxSizeMB(DoubleValue.newBuilder().setValue(75.0).build())
            .build();

    ImmutableIndexState mockState2 = mock(ImmutableIndexState.class);
    when(mockState2.getMergedSettings()).thenReturn(expectedMergedSettings);

    IndexStateInfo expectedStateInfo =
        initialState.toBuilder().setGen(1).setSettings(settingsUpdate).build();
    MockStateManager.nextState = mockState2;
    MockStateManager.expectedState = expectedStateInfo;

    assertEquals(expectedMergedSettings, stateManager.updateSettings(settingsUpdate));

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));
    verify(mockBackend, times(1))
        .commitIndexState(
            BackendGlobalState.getUniqueIndexName("test_index", "test_id"), expectedStateInfo);
    verify(mockState, times(1)).isStarted();
    verify(mockState, times(1)).getCurrentStateInfo();
    verify(mockState, times(1)).getFieldAndFacetState();
    verify(mockState2, times(1)).getMergedSettings();

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState, mockState2);
  }

  @Test
  public void testUpdateExistingSettings() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index")
            .setCommitted(true)
            .setSettings(
                IndexSettings.newBuilder()
                    .setConcurrentMergeSchedulerMaxThreadCount(
                        Int32Value.newBuilder().setValue(10).build())
                    .setConcurrentMergeSchedulerMaxMergeCount(
                        Int32Value.newBuilder().setValue(5).build())
                    .setNrtCachingDirectoryMaxSizeMB(
                        DoubleValue.newBuilder().setValue(100.0).build())
                    .build())
            .build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.isStarted()).thenReturn(false);
    when(mockState.getCurrentStateInfo()).thenReturn(initialState);
    when(mockState.getFieldAndFacetState()).thenReturn(mock(FieldAndFacetState.class));
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    IndexSettings settingsUpdate =
        IndexSettings.newBuilder()
            .setDirectory(StringValue.newBuilder().setValue("MMapDirectory").build())
            .setIndexMergeSchedulerAutoThrottle(BoolValue.newBuilder().setValue(true).build())
            .setNrtCachingDirectoryMaxSizeMB(DoubleValue.newBuilder().setValue(75.0).build())
            .build();
    IndexSettings expectedSavedSettings =
        IndexSettings.newBuilder()
            .setDirectory(StringValue.newBuilder().setValue("MMapDirectory").build())
            .setIndexMergeSchedulerAutoThrottle(BoolValue.newBuilder().setValue(true).build())
            .setNrtCachingDirectoryMaxSizeMB(DoubleValue.newBuilder().setValue(75.0).build())
            .setConcurrentMergeSchedulerMaxThreadCount(Int32Value.newBuilder().setValue(10).build())
            .setConcurrentMergeSchedulerMaxMergeCount(Int32Value.newBuilder().setValue(5).build())
            .build();
    IndexSettings expectedMergedSettings =
        ImmutableIndexState.DEFAULT_INDEX_SETTINGS
            .toBuilder()
            .setDirectory(StringValue.newBuilder().setValue("MMapDirectory").build())
            .setIndexMergeSchedulerAutoThrottle(BoolValue.newBuilder().setValue(true).build())
            .setNrtCachingDirectoryMaxSizeMB(DoubleValue.newBuilder().setValue(75.0).build())
            .setConcurrentMergeSchedulerMaxThreadCount(Int32Value.newBuilder().setValue(10).build())
            .setConcurrentMergeSchedulerMaxMergeCount(Int32Value.newBuilder().setValue(5).build())
            .build();

    ImmutableIndexState mockState2 = mock(ImmutableIndexState.class);
    when(mockState2.getMergedSettings()).thenReturn(expectedMergedSettings);

    IndexStateInfo expectedStateInfo =
        initialState.toBuilder().setGen(1).setSettings(expectedSavedSettings).build();
    MockStateManager.nextState = mockState2;
    MockStateManager.expectedState = expectedStateInfo;

    assertEquals(expectedMergedSettings, stateManager.updateSettings(settingsUpdate));

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));
    verify(mockBackend, times(1))
        .commitIndexState(
            BackendGlobalState.getUniqueIndexName("test_index", "test_id"), expectedStateInfo);
    verify(mockState, times(1)).isStarted();
    verify(mockState, times(1)).getCurrentStateInfo();
    verify(mockState, times(1)).getFieldAndFacetState();
    verify(mockState2, times(1)).getMergedSettings();

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState, mockState2);
  }

  @Test
  public void testUpdateSettingsNoExistingState() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(null);

    try {
      stateManager.updateSettings(IndexSettings.newBuilder().build());
      fail();
    } catch (IllegalStateException e) {
      assertEquals("No state for index: test_index", e.getMessage());
    }
  }

  @Test
  public void testUpdateSettingsIndexStarted() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index")
            .setCommitted(true)
            .setSettings(ImmutableIndexState.DEFAULT_INDEX_SETTINGS)
            .build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.isStarted()).thenReturn(true);
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    try {
      stateManager.updateSettings(IndexSettings.newBuilder().build());
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Cannot change setting for started index: test_index", e.getMessage());
    }
  }

  @Test
  public void testGetLiveSettings() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexLiveSettings indexLiveSettings =
        IndexLiveSettings.newBuilder()
            .setMaxRefreshSec(DoubleValue.newBuilder().setValue(10.0).build())
            .setSegmentsPerTier(Int32Value.newBuilder().setValue(20).build())
            .build();
    IndexStateInfo initialState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index")
            .setCommitted(true)
            .setLiveSettings(indexLiveSettings)
            .build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.getMergedLiveSettings()).thenReturn(indexLiveSettings);
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    assertEquals(indexLiveSettings, stateManager.getLiveSettings());

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));
    verify(mockState, times(1)).getMergedLiveSettings();

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState);
  }

  @Test
  public void testGetLiveSettingsNoExistingState() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(null);

    try {
      stateManager.getLiveSettings();
      fail();
    } catch (IllegalStateException e) {
      assertEquals("No state for index: test_index", e.getMessage());
    }
  }

  @Test
  public void testUpdateLiveSettingsNoop() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index")
            .setCommitted(true)
            .setLiveSettings(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS)
            .build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.getCurrentStateInfo()).thenReturn(initialState);
    when(mockState.getFieldAndFacetState()).thenReturn(mock(FieldAndFacetState.class));
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    ImmutableIndexState mockState2 = mock(ImmutableIndexState.class);
    when(mockState2.getMergedLiveSettings())
        .thenReturn(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS);
    ShardState mockShard = mock(ShardState.class);
    Map<Integer, ShardState> mockShardMap =
        ImmutableMap.<Integer, ShardState>builder().put(0, mockShard).build();
    when(mockState2.getShards()).thenReturn(mockShardMap);

    IndexStateInfo expectedStateInfo = initialState.toBuilder().setGen(1).build();
    MockStateManager.nextState = mockState2;
    MockStateManager.expectedState = expectedStateInfo;

    assertEquals(
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS,
        stateManager.updateLiveSettings(IndexLiveSettings.newBuilder().build()));

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));
    verify(mockBackend, times(1))
        .commitIndexState(
            BackendGlobalState.getUniqueIndexName("test_index", "test_id"), expectedStateInfo);
    verify(mockState, times(1)).getCurrentStateInfo();
    verify(mockState, times(1)).getFieldAndFacetState();
    verify(mockState2, times(1)).getMergedLiveSettings();
    verify(mockState2, times(1)).getShards();
    verify(mockShard, times(1)).updatedLiveSettings(IndexLiveSettings.newBuilder().build());

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState, mockState2, mockShard);
  }

  @Test
  public void testUpdateEmptyLiveSettings() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index")
            .setCommitted(true)
            .setLiveSettings(IndexLiveSettings.newBuilder().build())
            .build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.getCurrentStateInfo()).thenReturn(initialState);
    when(mockState.getFieldAndFacetState()).thenReturn(mock(FieldAndFacetState.class));
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    IndexLiveSettings settingsUpdate =
        IndexLiveSettings.newBuilder()
            .setMaxRefreshSec(DoubleValue.newBuilder().setValue(15.0).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(10).build())
            .setAddDocumentsMaxBufferLen(Int32Value.newBuilder().setValue(250).build())
            .build();
    IndexLiveSettings expectedMergedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS
            .toBuilder()
            .setMaxRefreshSec(DoubleValue.newBuilder().setValue(15.0).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(10).build())
            .setAddDocumentsMaxBufferLen(Int32Value.newBuilder().setValue(250).build())
            .build();

    ImmutableIndexState mockState2 = mock(ImmutableIndexState.class);
    when(mockState2.getMergedLiveSettings()).thenReturn(expectedMergedSettings);
    ShardState mockShard = mock(ShardState.class);
    Map<Integer, ShardState> mockShardMap =
        ImmutableMap.<Integer, ShardState>builder().put(0, mockShard).build();
    when(mockState2.getShards()).thenReturn(mockShardMap);

    IndexStateInfo expectedStateInfo =
        initialState.toBuilder().setGen(1).setLiveSettings(settingsUpdate).build();
    MockStateManager.nextState = mockState2;
    MockStateManager.expectedState = expectedStateInfo;

    assertEquals(expectedMergedSettings, stateManager.updateLiveSettings(settingsUpdate));

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));
    verify(mockBackend, times(1))
        .commitIndexState(
            BackendGlobalState.getUniqueIndexName("test_index", "test_id"), expectedStateInfo);
    verify(mockState, times(1)).getCurrentStateInfo();
    verify(mockState, times(1)).getFieldAndFacetState();
    verify(mockState2, times(1)).getMergedLiveSettings();
    verify(mockState2, times(1)).getShards();
    verify(mockShard, times(1)).updatedLiveSettings(settingsUpdate);

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState, mockState2, mockShard);
  }

  @Test
  public void testUpdateExistingLiveSettings() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index")
            .setCommitted(true)
            .setLiveSettings(
                IndexLiveSettings.newBuilder()
                    .setMaxRefreshSec(DoubleValue.newBuilder().setValue(15.0).build())
                    .setSliceMaxSegments(Int32Value.newBuilder().setValue(10).build())
                    .setAddDocumentsMaxBufferLen(Int32Value.newBuilder().setValue(250).build())
                    .build())
            .build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.getCurrentStateInfo()).thenReturn(initialState);
    when(mockState.getFieldAndFacetState()).thenReturn(mock(FieldAndFacetState.class));
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    IndexLiveSettings settingsUpdate =
        IndexLiveSettings.newBuilder()
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(3).build())
            .setSliceMaxDocs(Int32Value.newBuilder().setValue(10000).build())
            .setIndexRamBufferSizeMB(DoubleValue.newBuilder().setValue(512.0).build())
            .build();
    IndexLiveSettings expectedSavedSettings =
        IndexLiveSettings.newBuilder()
            .setMaxRefreshSec(DoubleValue.newBuilder().setValue(15.0).build())
            .setAddDocumentsMaxBufferLen(Int32Value.newBuilder().setValue(250).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(3).build())
            .setSliceMaxDocs(Int32Value.newBuilder().setValue(10000).build())
            .setIndexRamBufferSizeMB(DoubleValue.newBuilder().setValue(512.0).build())
            .build();
    IndexLiveSettings expectedMergedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS
            .toBuilder()
            .setMaxRefreshSec(DoubleValue.newBuilder().setValue(15.0).build())
            .setAddDocumentsMaxBufferLen(Int32Value.newBuilder().setValue(250).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(3).build())
            .setSliceMaxDocs(Int32Value.newBuilder().setValue(10000).build())
            .setIndexRamBufferSizeMB(DoubleValue.newBuilder().setValue(512.0).build())
            .build();

    ImmutableIndexState mockState2 = mock(ImmutableIndexState.class);
    when(mockState2.getMergedLiveSettings()).thenReturn(expectedMergedSettings);
    ShardState mockShard = mock(ShardState.class);
    Map<Integer, ShardState> mockShardMap =
        ImmutableMap.<Integer, ShardState>builder().put(0, mockShard).build();
    when(mockState2.getShards()).thenReturn(mockShardMap);

    IndexStateInfo expectedStateInfo =
        initialState.toBuilder().setGen(1).setLiveSettings(expectedSavedSettings).build();
    MockStateManager.nextState = mockState2;
    MockStateManager.expectedState = expectedStateInfo;

    assertEquals(expectedMergedSettings, stateManager.updateLiveSettings(settingsUpdate));

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));
    verify(mockBackend, times(1))
        .commitIndexState(
            BackendGlobalState.getUniqueIndexName("test_index", "test_id"), expectedStateInfo);
    verify(mockState, times(1)).getCurrentStateInfo();
    verify(mockState, times(1)).getFieldAndFacetState();
    verify(mockState2, times(1)).getMergedLiveSettings();
    verify(mockState2, times(1)).getShards();
    verify(mockShard, times(1)).updatedLiveSettings(settingsUpdate);

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState, mockState2, mockShard);
  }

  @Test
  public void testUpdateLiveSettingsNoExistingState() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(null);

    try {
      stateManager.updateLiveSettings(IndexLiveSettings.newBuilder().build());
      fail();
    } catch (IllegalStateException e) {
      assertEquals("No state for index: test_index", e.getMessage());
    }
  }

  @Test
  public void testUpdateFields() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder().setIndexName("test_index").setCommitted(true).build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.getCurrentStateInfo()).thenReturn(initialState);
    when(mockState.getFieldAndFacetState()).thenReturn(new FieldAndFacetState());
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;
    MockStateManager.verifyFieldsState =
        (fieldState) -> assertEquals(0, fieldState.getFields().keySet().size());

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    List<Field> addFields = new ArrayList<>();
    addFields.add(
        Field.newBuilder()
            .setName("field1")
            .setType(FieldType.FLOAT)
            .setStoreDocValues(true)
            .setMultiValued(true)
            .build());
    addFields.add(
        Field.newBuilder()
            .setName("field2")
            .setType(FieldType.ATOM)
            .setStoreDocValues(true)
            .setMultiValued(false)
            .build());

    IndexStateInfo expectedState =
        initialState
            .toBuilder()
            .setGen(1)
            .putFields("field1", addFields.get(0))
            .putFields("field2", addFields.get(1))
            .build();

    ImmutableIndexState mockState2 = mock(ImmutableIndexState.class);
    MockStateManager.nextState = mockState2;
    MockStateManager.expectedState = expectedState;
    MockStateManager.verifyFieldsState =
        (fieldState) -> {
          assertEquals(2, fieldState.getFields().size());
          assertTrue(fieldState.getFields().containsKey("field1"));
          assertTrue(fieldState.getFields().containsKey("field2"));
        };

    stateManager.updateFields(addFields);
    assertSame(mockState2, stateManager.getCurrent());

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));
    verify(mockBackend, times(1))
        .commitIndexState(
            BackendGlobalState.getUniqueIndexName("test_index", "test_id"), expectedState);
    verify(mockState, times(2)).getCurrentStateInfo();
    verify(mockState, times(1)).getFieldAndFacetState();
    verify(mockState2, times(1)).getAllFieldsJSON();

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState, mockState2);
  }

  @Test
  public void testUpdateFieldsAddToExisting() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index")
            .setCommitted(true)
            .putFields(
                "field1",
                Field.newBuilder()
                    .setName("field1")
                    .setType(FieldType.FLOAT)
                    .setStoreDocValues(true)
                    .setMultiValued(true)
                    .build())
            .putFields(
                "field2",
                Field.newBuilder()
                    .setName("field2")
                    .setType(FieldType.ATOM)
                    .setStoreDocValues(true)
                    .setMultiValued(false)
                    .build())
            .build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.getCurrentStateInfo()).thenReturn(initialState);
    when(mockState.getFieldAndFacetState()).thenReturn(new FieldAndFacetState());
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;
    MockStateManager.verifyFieldsState =
        (fieldState) -> {
          assertEquals(2, fieldState.getFields().size());
          assertTrue(fieldState.getFields().containsKey("field1"));
          assertTrue(fieldState.getFields().containsKey("field2"));
        };

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    when(mockState.getFieldAndFacetState()).thenReturn(MockStateManager.lastFieldAndFacetState);

    List<Field> addFields = new ArrayList<>();
    addFields.add(
        Field.newBuilder()
            .setName("field3")
            .setType(FieldType.DOUBLE)
            .setStoreDocValues(false)
            .setMultiValued(true)
            .setSearch(true)
            .build());
    addFields.add(
        Field.newBuilder()
            .setName("field4")
            .setType(FieldType.BOOLEAN)
            .setStoreDocValues(true)
            .setMultiValued(true)
            .build());

    IndexStateInfo expectedState =
        initialState
            .toBuilder()
            .setGen(1)
            .putFields("field3", addFields.get(0))
            .putFields("field4", addFields.get(1))
            .build();

    ImmutableIndexState mockState2 = mock(ImmutableIndexState.class);
    MockStateManager.nextState = mockState2;
    MockStateManager.expectedState = expectedState;
    MockStateManager.verifyFieldsState =
        (fieldState) -> {
          assertEquals(4, fieldState.getFields().size());
          assertTrue(fieldState.getFields().containsKey("field1"));
          assertTrue(fieldState.getFields().containsKey("field2"));
          assertTrue(fieldState.getFields().containsKey("field3"));
          assertTrue(fieldState.getFields().containsKey("field4"));
        };

    stateManager.updateFields(addFields);
    assertSame(mockState2, stateManager.getCurrent());

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));
    verify(mockBackend, times(1))
        .commitIndexState(
            BackendGlobalState.getUniqueIndexName("test_index", "test_id"), expectedState);
    verify(mockState, times(2)).getCurrentStateInfo();
    verify(mockState, times(1)).getFieldAndFacetState();
    verify(mockState2, times(1)).getAllFieldsJSON();

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState, mockState2);
  }

  @Test
  public void testLoadFields() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    List<Field> initialFields = new ArrayList<>();
    initialFields.add(
        Field.newBuilder()
            .setName("field1")
            .setType(FieldType.DOUBLE)
            .setStoreDocValues(false)
            .setMultiValued(true)
            .setSearch(true)
            .build());
    initialFields.add(
        Field.newBuilder()
            .setName("field2")
            .setType(FieldType.BOOLEAN)
            .setStoreDocValues(true)
            .setMultiValued(true)
            .build());

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index")
            .setCommitted(true)
            .putFields("field1", initialFields.get(0))
            .putFields("field2", initialFields.get(1))
            .build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.getCurrentStateInfo()).thenReturn(initialState);
    when(mockState.getFieldAndFacetState()).thenReturn(new FieldAndFacetState());
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;
    MockStateManager.verifyFieldsState =
        (fieldState) -> {
          assertEquals(2, fieldState.getFields().size());
          assertTrue(fieldState.getFields().containsKey("field1"));
          assertTrue(fieldState.getFields().containsKey("field2"));
        };

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState);
  }

  @Test
  public void testUpdateFieldsNoExistingState() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(null);

    try {
      stateManager.updateFields(
          Collections.singletonList(Field.newBuilder().setName("field1").build()));
      fail();
    } catch (IllegalStateException e) {
      assertEquals("No state for index: test_index", e.getMessage());
    }
  }

  @Test
  public void testStartIndex() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder().setIndexName("test_index").setCommitted(true).build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.getCurrentStateInfo()).thenReturn(initialState);
    when(mockState.isStarted()).thenReturn(false);
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    ReplicationServerClient mockReplicationClient = mock(ReplicationServerClient.class);
    stateManager.start(Mode.PRIMARY, Path.of("/tmp"), 1, mockReplicationClient);

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));
    verify(mockState, times(1)).getCurrentStateInfo();
    verify(mockState, times(1)).isStarted();
    verify(mockState, times(1)).start(Mode.PRIMARY, Path.of("/tmp"), 1, mockReplicationClient);

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState);
  }

  @Test
  public void testStartIndexReplicaSkipCommit() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder().setIndexName("test_index").setCommitted(false).build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    String configFile = String.join("\n", "backupWithIncArchiver: true");
    LuceneServerConfiguration configuration =
        new LuceneServerConfiguration(new ByteArrayInputStream(configFile.getBytes()));
    when(mockGlobalState.getConfiguration()).thenReturn(configuration);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.isStarted()).thenReturn(false);
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    ReplicationServerClient mockReplicationClient = mock(ReplicationServerClient.class);
    stateManager.start(Mode.REPLICA, Path.of("/tmp"), 1, mockReplicationClient);

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));
    verify(mockState, times(1)).isStarted();
    verify(mockState, times(1)).start(Mode.REPLICA, Path.of("/tmp"), 1, mockReplicationClient);

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState);
  }

  @Test
  public void testStartIndexInitialCommit() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder().setIndexName("test_index").setCommitted(false).build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    String configFile = String.join("\n", "backupWithIncArchiver: true");
    LuceneServerConfiguration configuration =
        new LuceneServerConfiguration(new ByteArrayInputStream(configFile.getBytes()));
    when(mockGlobalState.getConfiguration()).thenReturn(configuration);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.getFieldAndFacetState()).thenReturn(new FieldAndFacetState());
    when(mockState.getCurrentStateInfo()).thenReturn(initialState);
    when(mockState.isStarted()).thenReturn(false);
    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;

    stateManager.load();
    assertSame(mockState, stateManager.getCurrent());

    ImmutableIndexState mockState2 = mock(ImmutableIndexState.class);
    IndexStateInfo updatedState = initialState.toBuilder().setGen(1).setCommitted(true).build();
    MockStateManager.nextState = mockState2;
    MockStateManager.expectedState = updatedState;

    ReplicationServerClient mockReplicationClient = mock(ReplicationServerClient.class);
    stateManager.start(Mode.PRIMARY, Path.of("/tmp"), 1, mockReplicationClient);
    assertSame(mockState2, stateManager.getCurrent());

    verify(mockBackend, times(1))
        .loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id"));
    verify(mockBackend, times(1))
        .commitIndexState(
            BackendGlobalState.getUniqueIndexName("test_index", "test_id"), updatedState);
    verify(mockGlobalState, times(1)).getConfiguration();
    verify(mockState, times(3)).getCurrentStateInfo();
    verify(mockState, times(1)).isStarted();
    verify(mockState, times(1)).start(Mode.PRIMARY, Path.of("/tmp"), 1, mockReplicationClient);
    verify(mockState, times(1)).commit(true);
    verify(mockState, times(1)).getFieldAndFacetState();

    verifyNoMoreInteractions(mockBackend, mockGlobalState, mockState, mockState2);
  }

  @Test
  public void testStartIndexNoExistingState() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(null);

    try {
      stateManager.start(Mode.PRIMARY, Path.of("/tmp"), 1, mock(ReplicationServerClient.class));
      fail();
    } catch (IllegalStateException e) {
      assertEquals("No state for index: test_index", e.getMessage());
    }
  }

  @Test
  public void testStartIndexAlreadyStarted() throws IOException {
    StateBackend mockBackend = mock(StateBackend.class);
    GlobalState mockGlobalState = mock(GlobalState.class);
    BackendStateManager stateManager =
        new MockStateManager("test_index", "test_id", mockBackend, mockGlobalState);

    ImmutableIndexState mockState = mock(ImmutableIndexState.class);
    when(mockState.isStarted()).thenReturn(true);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder().setIndexName("test_index").setCommitted(false).build();
    when(mockBackend.loadIndexState(BackendGlobalState.getUniqueIndexName("test_index", "test_id")))
        .thenReturn(initialState);

    MockStateManager.nextState = mockState;
    MockStateManager.expectedState = initialState;
    stateManager.load();

    try {
      stateManager.start(Mode.PRIMARY, Path.of("/tmp"), 1, mock(ReplicationServerClient.class));
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Index already started: test_index", e.getMessage());
    }
  }

  @Test
  public void testFixIndexName_updatesName() {
    IndexStateInfo initialInfo =
        IndexStateInfo.newBuilder().setIndexName("test_index").setCommitted(true).build();
    IndexStateInfo updatedInfo = BackendStateManager.fixIndexName(initialInfo, "test_index_1");
    IndexStateInfo expectedInfo =
        IndexStateInfo.newBuilder().setIndexName("test_index_1").setCommitted(true).build();
    assertEquals(expectedInfo, updatedInfo);
  }

  @Test
  public void testFixIndexName_noop() {
    IndexStateInfo initialInfo =
        IndexStateInfo.newBuilder().setIndexName("test_index").setCommitted(true).build();
    IndexStateInfo updatedInfo = BackendStateManager.fixIndexName(initialInfo, "test_index");
    assertEquals(initialInfo, updatedInfo);
  }

  @Test(expected = NullPointerException.class)
  public void testFixIndexName_nullStateInfo() {
    BackendStateManager.fixIndexName(null, "test_index");
  }

  @Test(expected = NullPointerException.class)
  public void testFixIndexName_nullIndexName() {
    BackendStateManager.fixIndexName(IndexStateInfo.newBuilder().build(), null);
  }
}
