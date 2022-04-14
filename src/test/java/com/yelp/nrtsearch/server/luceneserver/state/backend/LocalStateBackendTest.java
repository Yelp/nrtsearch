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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.BoolValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexGlobalState;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.grpc.SortFields;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.BackendGlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
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

  private Path getIndexStateFilePath(String indexIdentifier) {
    return Paths.get(
        folder.getRoot().getAbsolutePath(), indexIdentifier, StateUtils.INDEX_STATE_FILE);
  }

  private void ensureIndexStateDirectory(String indexIdentifier) {
    StateUtils.ensureDirectory(Paths.get(folder.getRoot().getAbsolutePath(), indexIdentifier));
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

    GlobalStateInfo globalState = stateBackend.loadOrCreateGlobalState();
    assertEquals(globalState, GlobalStateInfo.newBuilder().build());

    assertTrue(filePath.toFile().exists());
    assertTrue(filePath.toFile().isFile());

    GlobalStateInfo loadedState = StateUtils.readStateFromFile(filePath);
    assertEquals(globalState, loadedState);
  }

  @Test
  public void testLoadsSavedState() throws IOException {
    StateBackend stateBackend = new LocalStateBackend(getMockGlobalState());
    Path filePath = getStateFilePath();

    GlobalStateInfo initialState =
        GlobalStateInfo.newBuilder()
            .setGen(20)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(false).build())
            .putIndices(
                "test_index_2",
                IndexGlobalState.newBuilder().setId("test_id_2").setStarted(true).build())
            .build();
    StateUtils.writeStateToFile(
        initialState,
        Paths.get(folder.getRoot().getAbsolutePath(), StateUtils.GLOBAL_STATE_FOLDER),
        StateUtils.GLOBAL_STATE_FILE);
    assertTrue(filePath.toFile().exists());
    assertTrue(filePath.toFile().isFile());

    GlobalStateInfo loadedState = stateBackend.loadOrCreateGlobalState();
    assertEquals(initialState, loadedState);
  }

  @Test
  public void testCommitGlobalState() throws IOException {
    StateBackend stateBackend = new LocalStateBackend(getMockGlobalState());
    Path filePath = getStateFilePath();
    GlobalStateInfo initialState = stateBackend.loadOrCreateGlobalState();

    GlobalStateInfo updatedState =
        GlobalStateInfo.newBuilder()
            .setGen(21)
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id_1").setStarted(false).build())
            .putIndices(
                "test_index_2",
                IndexGlobalState.newBuilder().setId("test_id_2").setStarted(true).build())
            .build();
    assertNotEquals(initialState, updatedState);

    stateBackend.commitGlobalState(updatedState);
    GlobalStateInfo loadedState = StateUtils.readStateFromFile(filePath);
    assertEquals(updatedState, loadedState);

    GlobalStateInfo updatedState2 =
        GlobalStateInfo.newBuilder()
            .setGen(22)
            .putIndices(
                "test_index_3",
                IndexGlobalState.newBuilder().setId("test_id_3").setStarted(true).build())
            .build();
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

  @Test
  public void testIndexStateNotExist() throws IOException {
    StateBackend stateBackend = new LocalStateBackend(getMockGlobalState());
    String indexIdentifier =
        BackendGlobalState.getUniqueIndexName("test_index", UUID.randomUUID().toString());
    assertNull(stateBackend.loadIndexState(indexIdentifier));
  }

  @Test
  public void testLoadsSavedIndexState() throws IOException {
    StateBackend stateBackend = new LocalStateBackend(getMockGlobalState());
    String indexIdentifier =
        BackendGlobalState.getUniqueIndexName("test_index", UUID.randomUUID().toString());
    ensureIndexStateDirectory(indexIdentifier);
    Path filePath = getIndexStateFilePath(indexIdentifier);

    IndexStateInfo initialState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index_2")
            .setGen(5)
            .setCommitted(true)
            .setSettings(
                IndexSettings.newBuilder()
                    .setConcurrentMergeSchedulerMaxThreadCount(
                        Int32Value.newBuilder().setValue(15).build())
                    .setDirectory(StringValue.newBuilder().setValue("FSDirectory").build())
                    .setIndexMergeSchedulerAutoThrottle(
                        BoolValue.newBuilder().setValue(false).build())
                    .setIndexSort(
                        SortFields.newBuilder()
                            .addSortedFields(
                                SortType.newBuilder()
                                    .setFieldName("field1")
                                    .setReverse(false)
                                    .build())
                            .addSortedFields(SortType.newBuilder().setFieldName("field2").build())
                            .build())
                    .build())
            .setLiveSettings(
                IndexLiveSettings.newBuilder()
                    .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(200).build())
                    .setIndexRamBufferSizeMB(DoubleValue.newBuilder().setValue(100.0).build())
                    .setMaxRefreshSec(DoubleValue.newBuilder().setValue(50.0).build())
                    .build())
            .build();

    StateUtils.writeIndexStateToFile(
        initialState,
        Paths.get(folder.getRoot().getAbsolutePath(), indexIdentifier),
        StateUtils.INDEX_STATE_FILE);
    assertTrue(filePath.toFile().exists());
    assertTrue(filePath.toFile().isFile());

    IndexStateInfo loadedState = stateBackend.loadIndexState(indexIdentifier);
    assertEquals(initialState, loadedState);
  }

  @Test
  public void testCommitIndexState() throws IOException {
    StateBackend stateBackend = new LocalStateBackend(getMockGlobalState());
    String indexIdentifier =
        BackendGlobalState.getUniqueIndexName("test_index", UUID.randomUUID().toString());
    Path filePath = getIndexStateFilePath(indexIdentifier);
    assertNull(stateBackend.loadIndexState(indexIdentifier));

    IndexStateInfo updatedState =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index_2")
            .setGen(5)
            .setCommitted(true)
            .setSettings(
                IndexSettings.newBuilder()
                    .setConcurrentMergeSchedulerMaxThreadCount(
                        Int32Value.newBuilder().setValue(15).build())
                    .setDirectory(StringValue.newBuilder().setValue("FSDirectory").build())
                    .setIndexMergeSchedulerAutoThrottle(
                        BoolValue.newBuilder().setValue(false).build())
                    .setIndexSort(
                        SortFields.newBuilder()
                            .addSortedFields(
                                SortType.newBuilder()
                                    .setFieldName("field1")
                                    .setReverse(false)
                                    .build())
                            .addSortedFields(SortType.newBuilder().setFieldName("field2").build())
                            .build())
                    .build())
            .setLiveSettings(
                IndexLiveSettings.newBuilder()
                    .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(200).build())
                    .setIndexRamBufferSizeMB(DoubleValue.newBuilder().setValue(100.0).build())
                    .setMaxRefreshSec(DoubleValue.newBuilder().setValue(50.0).build())
                    .build())
            .build();

    stateBackend.commitIndexState(indexIdentifier, updatedState);
    IndexStateInfo loadedState = StateUtils.readIndexStateFromFile(filePath);
    assertEquals(updatedState, loadedState);

    IndexStateInfo updatedState2 =
        IndexStateInfo.newBuilder()
            .setIndexName("test_index_2")
            .setGen(6)
            .setCommitted(true)
            .setSettings(
                IndexSettings.newBuilder()
                    .setConcurrentMergeSchedulerMaxThreadCount(
                        Int32Value.newBuilder().setValue(16).build())
                    .setDirectory(StringValue.newBuilder().setValue("MMapDirectory").build())
                    .setIndexMergeSchedulerAutoThrottle(
                        BoolValue.newBuilder().setValue(true).build())
                    .setIndexSort(
                        SortFields.newBuilder()
                            .addSortedFields(
                                SortType.newBuilder()
                                    .setFieldName("field2")
                                    .setReverse(true)
                                    .build())
                            .build())
                    .build())
            .setLiveSettings(
                IndexLiveSettings.newBuilder()
                    .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(300).build())
                    .setIndexRamBufferSizeMB(DoubleValue.newBuilder().setValue(200.0).build())
                    .setMaxRefreshSec(DoubleValue.newBuilder().setValue(75.0).build())
                    .build())
            .build();

    assertNotEquals(updatedState, updatedState2);
    stateBackend.commitIndexState(indexIdentifier, updatedState2);

    loadedState = StateUtils.readIndexStateFromFile(filePath);
    assertEquals(updatedState2, loadedState);
  }

  @Test(expected = NullPointerException.class)
  public void testLoadNullIndexState() throws IOException {
    StateBackend stateBackend = new LocalStateBackend(getMockGlobalState());
    stateBackend.loadIndexState(null);
  }

  @Test
  public void testCommitNullIndexState() throws IOException {
    StateBackend stateBackend = new LocalStateBackend(getMockGlobalState());
    String indexIdentifier =
        BackendGlobalState.getUniqueIndexName("test_index", UUID.randomUUID().toString());
    stateBackend.loadIndexState(indexIdentifier);
    try {
      stateBackend.commitIndexState(indexIdentifier, null);
      fail();
    } catch (NullPointerException ignore) {

    }

    try {
      stateBackend.commitIndexState(null, IndexStateInfo.newBuilder().build());
      fail();
    } catch (NullPointerException ignore) {

    }
  }

  @Test
  public void testIndexStateFileIsDirectory() throws IOException {
    StateBackend stateBackend = new LocalStateBackend(getMockGlobalState());
    String indexIdentifier =
        BackendGlobalState.getUniqueIndexName("test_index", UUID.randomUUID().toString());
    StateUtils.ensureDirectory(getIndexStateFilePath(indexIdentifier));
    try {
      stateBackend.loadIndexState(indexIdentifier);
      fail();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("index_state.json is a directory"));
    }
  }
}
