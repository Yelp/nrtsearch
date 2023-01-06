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

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.config.IndexStartConfig;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.CreateIndexRequest;
import com.yelp.nrtsearch.server.grpc.DummyResponse;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexGlobalState;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.RestoreIndex;
import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import com.yelp.nrtsearch.server.grpc.StartIndexResponse;
import com.yelp.nrtsearch.server.grpc.StartIndexV2Request;
import com.yelp.nrtsearch.server.grpc.StopIndexRequest;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.StartIndexHandler;
import com.yelp.nrtsearch.server.luceneserver.StartIndexHandler.StartIndexHandlerException;
import com.yelp.nrtsearch.server.luceneserver.index.BackendStateManager;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import com.yelp.nrtsearch.server.luceneserver.state.backend.LocalStateBackend;
import com.yelp.nrtsearch.server.luceneserver.state.backend.RemoteStateBackend;
import com.yelp.nrtsearch.server.luceneserver.state.backend.StateBackend;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of GlobalState that uses a configurable {@link StateBackend} for storing/loading
 * state.
 */
public class BackendGlobalState extends GlobalState {
  private static final Logger logger = LoggerFactory.getLogger(BackendGlobalState.class);

  private int resolvedReplicationPort;
  /**
   * State class containing immutable persistent and ephemeral global state, stored together so that
   * they can be updated atomically.
   */
  private static class ImmutableState {
    public final GlobalStateInfo globalStateInfo;
    public final Map<String, IndexStateManager> indexStateManagerMap;

    ImmutableState(
        GlobalStateInfo globalStateInfo, Map<String, IndexStateManager> indexStateManagerMap) {
      this.globalStateInfo = globalStateInfo;
      this.indexStateManagerMap = Collections.unmodifiableMap(indexStateManagerMap);
    }
  }

  // volatile for atomic replacement
  private volatile ImmutableState immutableState;
  private final StateBackend stateBackend;
  private final Archiver legacyArchiver;

  /**
   * Build unique index name from index name and instance id (UUID).
   *
   * @param indexName index name
   * @param id instance id
   * @return unique index identifier
   * @throws NullPointerException if either parameter is null
   */
  public static String getUniqueIndexName(String indexName, String id) {
    Objects.requireNonNull(indexName);
    Objects.requireNonNull(id);
    return indexName + "-" + id;
  }

  /**
   * Constructor.
   *
   * @param luceneServerConfiguration server config
   * @param incArchiver archiver for remote backends
   * @throws IOException on filesystem error
   */
  public BackendGlobalState(
      LuceneServerConfiguration luceneServerConfiguration, Archiver incArchiver)
      throws IOException {
    this(luceneServerConfiguration, incArchiver, null);
  }

  /**
   * Constructor.
   *
   * @param luceneServerConfiguration server config
   * @param incArchiver archiver for remote backends
   * @param legacyArchiver legacy archiver
   * @throws IOException on filesystem error
   */
  public BackendGlobalState(
      LuceneServerConfiguration luceneServerConfiguration,
      Archiver incArchiver,
      Archiver legacyArchiver)
      throws IOException {
    super(luceneServerConfiguration, incArchiver);
    this.legacyArchiver = legacyArchiver;
    stateBackend = createStateBackend();
    GlobalStateInfo globalStateInfo = stateBackend.loadOrCreateGlobalState();
    // init index state managers
    Map<String, IndexStateManager> managerMap = new HashMap<>();
    for (Map.Entry<String, IndexGlobalState> entry : globalStateInfo.getIndicesMap().entrySet()) {
      IndexStateManager stateManager =
          createIndexStateManager(entry.getKey(), entry.getValue().getId(), stateBackend);
      stateManager.load();
      managerMap.put(entry.getKey(), stateManager);
    }
    immutableState = new ImmutableState(globalStateInfo, managerMap);
    // If any indices should be started, it will be done in the replicationStarted hook
  }

  /**
   * Create {@link StateBackend} based on the current configuration. Protected to allow injection
   * for testing.
   */
  protected StateBackend createStateBackend() {
    switch (getConfiguration().getStateConfig().getBackendType()) {
      case LOCAL:
        return new LocalStateBackend(this);
      case REMOTE:
        return new RemoteStateBackend(this);
      default:
        throw new IllegalArgumentException(
            "Unsupported state backend type: "
                + getConfiguration().getStateConfig().getBackendType());
    }
  }

  /**
   * Create {@link IndexStateManager} for index. Protected to allow injection for testing.
   *
   * @param indexName index name
   * @param indexId index instance id
   * @param stateBackend state backend
   * @return index state manager
   */
  protected IndexStateManager createIndexStateManager(
      String indexName, String indexId, StateBackend stateBackend) {
    return new BackendStateManager(indexName, indexId, stateBackend, this);
  }

  /**
   * Generate a unique id to identify an index instance. Protected to allow injection for testing.
   */
  protected String getIndexId() {
    return UUID.randomUUID().toString();
  }

  @VisibleForTesting
  StateBackend getStateBackend() {
    return stateBackend;
  }

  @Override
  public synchronized void reloadStateFromBackend() throws IOException {
    GlobalStateInfo newGlobalStateInfo = getStateBackend().loadOrCreateGlobalState();
    Map<String, IndexStateManager> newManagerMap = new HashMap<>();
    for (Map.Entry<String, IndexGlobalState> entry :
        newGlobalStateInfo.getIndicesMap().entrySet()) {
      String indexName = entry.getKey();
      IndexStateManager stateManager = immutableState.indexStateManagerMap.get(indexName);
      if (stateManager == null || !entry.getValue().getId().equals(stateManager.getIndexId())) {
        stateManager = createIndexStateManager(indexName, entry.getValue().getId(), stateBackend);
      }
      stateManager.load();
      newManagerMap.put(indexName, stateManager);
    }
    ImmutableState newImmutableState = new ImmutableState(newGlobalStateInfo, newManagerMap);
    if (getConfiguration().getIndexStartConfig().getAutoStart()) {
      updateStartedIndices(newImmutableState);
    }
    this.immutableState = newImmutableState;
  }

  @Override
  public int getReplicationPort() {
    return resolvedReplicationPort;
  }

  @Override
  public Path getIndexDir(String indexName) {
    try {
      return getIndex(indexName).getRootDir();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void replicationStarted(int replicationPort) throws IOException {
    this.resolvedReplicationPort = replicationPort;
    if (getConfiguration().getIndexStartConfig().getAutoStart()) {
      updateStartedIndices(immutableState);
    }
  }

  @Override
  public String getDataResourceForIndex(String indexName) {
    IndexGlobalState indexGlobalState =
        immutableState.globalStateInfo.getIndicesMap().get(indexName);
    if (indexGlobalState == null) {
      throw new IllegalArgumentException("index \"" + indexName + "\" was not saved or committed");
    }
    return getUniqueIndexName(indexName, indexGlobalState.getId());
  }

  @Override
  public Set<String> getIndexNames() {
    return immutableState.globalStateInfo.getIndicesMap().keySet();
  }

  @Override
  public Set<String> getIndicesToStart() {
    return immutableState.globalStateInfo.getIndicesMap().entrySet().stream()
        .filter(e -> e.getValue().getStarted())
        .map(Entry::getKey)
        .collect(Collectors.toSet());
  }

  @Override
  public synchronized IndexState createIndex(String name) throws IOException {
    return createIndex(CreateIndexRequest.newBuilder().setIndexName(name).build());
  }

  @Override
  public synchronized IndexState createIndex(CreateIndexRequest createIndexRequest)
      throws IOException {
    String indexName = createIndexRequest.getIndexName();
    if (immutableState.globalStateInfo.getIndicesMap().containsKey(indexName)) {
      throw new IllegalArgumentException("index \"" + indexName + "\" already exists");
    }

    String indexId;
    IndexStateManager stateManager;
    if (createIndexRequest.getExistsWithId().isEmpty()) {
      indexId = getIndexId();
      stateManager = createIndexStateManager(indexName, indexId, stateBackend);
      stateManager.create();
    } else {
      indexId = createIndexRequest.getExistsWithId();
      stateManager = createIndexStateManager(indexName, indexId, stateBackend);
      stateManager.load();
    }

    if (createIndexRequest.hasSettings()) {
      stateManager.updateSettings(createIndexRequest.getSettings());
    }
    if (createIndexRequest.hasLiveSettings()) {
      stateManager.updateLiveSettings(createIndexRequest.getLiveSettings());
    }
    if (!createIndexRequest.getFieldsList().isEmpty()) {
      stateManager.updateFields(createIndexRequest.getFieldsList());
    }

    IndexGlobalState newIndexState =
        IndexGlobalState.newBuilder()
            .setId(indexId)
            .setStarted(createIndexRequest.getStart())
            .build();
    if (createIndexRequest.getStart()) {
      startIndexFromConfig(indexName, stateManager, newIndexState);
    }

    GlobalStateInfo updatedState =
        immutableState
            .globalStateInfo
            .toBuilder()
            .putIndices(indexName, newIndexState)
            .setGen(immutableState.globalStateInfo.getGen() + 1)
            .build();
    stateBackend.commitGlobalState(updatedState);

    Map<String, IndexStateManager> updatedIndexStateManagerMap =
        new HashMap<>(immutableState.indexStateManagerMap);
    updatedIndexStateManagerMap.put(indexName, stateManager);
    immutableState = new ImmutableState(updatedState, updatedIndexStateManagerMap);

    return stateManager.getCurrent();
  }

  @Override
  public IndexState getIndex(String name, boolean hasRestore) throws IOException {
    return getIndexStateManager(name).getCurrent();
  }

  @Override
  public IndexState getIndex(String name) throws IOException {
    return getIndex(name, false);
  }

  @Override
  public IndexStateManager getIndexStateManager(String name) throws IOException {
    IndexStateManager stateManager = immutableState.indexStateManagerMap.get(name);
    if (stateManager == null) {
      throw new IllegalArgumentException("index \"" + name + "\" was not saved or committed");
    }
    return stateManager;
  }

  @Override
  public synchronized void deleteIndex(String name) throws IOException {
    GlobalStateInfo updatedState =
        immutableState
            .globalStateInfo
            .toBuilder()
            .removeIndices(name)
            .setGen(immutableState.globalStateInfo.getGen() + 1)
            .build();
    stateBackend.commitGlobalState(updatedState);

    IndexStateManager stateManager = immutableState.indexStateManagerMap.get(name);
    Map<String, IndexStateManager> updatedIndexStateManagerMap =
        new HashMap<>(immutableState.indexStateManagerMap);
    updatedIndexStateManagerMap.remove(name);

    immutableState = new ImmutableState(updatedState, updatedIndexStateManagerMap);
    stateManager.close();
  }

  @Override
  public synchronized StartIndexResponse startIndex(StartIndexRequest startIndexRequest)
      throws IOException {
    IndexStateManager indexStateManager = getIndexStateManager(startIndexRequest.getIndexName());
    IndexGlobalState indexGlobalState =
        immutableState.globalStateInfo.getIndicesMap().get(startIndexRequest.getIndexName());

    // this limitation exists because we do not handle backup/restore of the taxonomy index
    // properly, which is only used in STANDALONE mode
    if (startIndexRequest.getMode().equals(Mode.STANDALONE)
        && getConfiguration().getIndexStartConfig().getAutoStart()
        && getConfiguration()
            .getIndexStartConfig()
            .getDataLocationType()
            .equals(IndexDataLocationType.REMOTE)) {
      throw new IllegalArgumentException(
          "STANDALONE index mode cannot be used with REMOTE data location type");
    }

    // If only the index name is given in the restore, rewrite to include current id
    StartIndexRequest request;
    if (startIndexRequest.hasRestore()
        && startIndexRequest
            .getRestore()
            .getResourceName()
            .equals(startIndexRequest.getIndexName())) {
      request =
          startIndexRequest
              .toBuilder()
              .setRestore(
                  startIndexRequest
                      .getRestore()
                      .toBuilder()
                      .setResourceName(
                          getUniqueIndexName(
                              startIndexRequest.getIndexName(), indexGlobalState.getId()))
                      .build())
              .build();
    } else {
      request = startIndexRequest;
    }
    StartIndexResponse response = startIndex(indexStateManager, request);

    // update started state of index
    if (startIndexRequest.getMode() != Mode.REPLICA && !indexGlobalState.getStarted()) {
      IndexGlobalState updatedIndexState = indexGlobalState.toBuilder().setStarted(true).build();
      GlobalStateInfo updatedGlobalState =
          immutableState
              .globalStateInfo
              .toBuilder()
              .putIndices(startIndexRequest.getIndexName(), updatedIndexState)
              .setGen(immutableState.globalStateInfo.getGen() + 1)
              .build();

      stateBackend.commitGlobalState(updatedGlobalState);
      immutableState = new ImmutableState(updatedGlobalState, immutableState.indexStateManagerMap);
    }
    return response;
  }

  @Override
  public synchronized StartIndexResponse startIndexV2(StartIndexV2Request startIndexRequest)
      throws IOException {
    IndexStateManager stateManager = getIndexStateManager(startIndexRequest.getIndexName());
    IndexGlobalState indexGlobalState =
        immutableState.globalStateInfo.getIndicesOrThrow(startIndexRequest.getIndexName());
    IndexGlobalState updatedIndexGlobalState =
        indexGlobalState.toBuilder().setStarted(true).build();
    StartIndexResponse response =
        startIndexFromConfig(
            startIndexRequest.getIndexName(), stateManager, updatedIndexGlobalState);

    if (getConfiguration().getIndexStartConfig().getMode() != Mode.REPLICA) {
      GlobalStateInfo updatedGlobalState =
          immutableState
              .globalStateInfo
              .toBuilder()
              .putIndices(startIndexRequest.getIndexName(), updatedIndexGlobalState)
              .setGen(immutableState.globalStateInfo.getGen() + 1)
              .build();

      stateBackend.commitGlobalState(updatedGlobalState);
      immutableState = new ImmutableState(updatedGlobalState, immutableState.indexStateManagerMap);
    }

    return response;
  }

  private StartIndexResponse startIndex(
      IndexStateManager indexStateManager, StartIndexRequest startIndexRequest) throws IOException {
    StartIndexHandler startIndexHandler =
        new StartIndexHandler(
            legacyArchiver,
            getIncArchiver().orElse(null),
            getConfiguration().getArchiveDirectory(),
            getConfiguration().getBackupWithInArchiver(),
            getConfiguration().getRestoreFromIncArchiver(),
            indexStateManager,
            getConfiguration().getDiscoveryFileUpdateIntervalMs());
    try {
      return startIndexHandler.handle(indexStateManager.getCurrent(), startIndexRequest);
    } catch (StartIndexHandlerException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized DummyResponse stopIndex(StopIndexRequest stopIndexRequest)
      throws IOException {
    IndexStateManager indexStateManager = getIndexStateManager(stopIndexRequest.getIndexName());
    if (!indexStateManager.getCurrent().isStarted()) {
      throw new IllegalArgumentException(
          "Index \"" + stopIndexRequest.getIndexName() + "\" is not started");
    }
    // update started state of index
    if (!indexStateManager.getCurrent().getShard(0).isReplica()) {
      IndexGlobalState updatedIndexState =
          immutableState
              .globalStateInfo
              .getIndicesMap()
              .get(stopIndexRequest.getIndexName())
              .toBuilder()
              .setStarted(false)
              .build();

      GlobalStateInfo updatedState =
          immutableState
              .globalStateInfo
              .toBuilder()
              .putIndices(stopIndexRequest.getIndexName(), updatedIndexState)
              .setGen(immutableState.globalStateInfo.getGen() + 1)
              .build();
      stateBackend.commitGlobalState(updatedState);
      immutableState = new ImmutableState(updatedState, immutableState.indexStateManagerMap);
    }
    indexStateManager.close();

    return DummyResponse.newBuilder().setOk("ok").build();
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      logger.info("GlobalState.close: indices=" + getIndexNames());
      IOUtils.close(immutableState.indexStateManagerMap.values());
    }
    super.close();
  }

  /**
   * Sync started indices to that of the given global state. If the state notes an index should be
   * started, ensure that it is, or start it using the {@link IndexStartConfig}.
   *
   * @param newState state to sync to
   * @throws IOException
   */
  private void updateStartedIndices(ImmutableState newState) throws IOException {
    for (Map.Entry<String, IndexGlobalState> entry :
        newState.globalStateInfo.getIndicesMap().entrySet()) {
      IndexStateManager indexStateManager = newState.indexStateManagerMap.get(entry.getKey());
      if (entry.getValue().getStarted() && !indexStateManager.getCurrent().isStarted()) {
        startIndexFromConfig(entry.getKey(), indexStateManager, entry.getValue());
      }
    }
  }

  /**
   * Start an index based on the {@link IndexStartConfig}.
   *
   * @param indexName index name
   * @param indexStateManager index state manager
   * @param indexGlobalState index global state
   * @return start response
   * @throws IOException
   */
  private StartIndexResponse startIndexFromConfig(
      String indexName, IndexStateManager indexStateManager, IndexGlobalState indexGlobalState)
      throws IOException {
    IndexStartConfig indexStartConfig = getConfiguration().getIndexStartConfig();
    StartIndexRequest.Builder requestBuilder =
        StartIndexRequest.newBuilder()
            .setIndexName(indexName)
            .setPrimaryGen(-1)
            .setMode(indexStartConfig.getMode());

    // set primary discovery config
    if (indexStartConfig.getMode().equals(Mode.REPLICA)) {
      requestBuilder
          .setPrimaryAddress(indexStartConfig.getDiscoveryHost())
          .setPort(indexStartConfig.getDiscoveryPort())
          .setPrimaryDiscoveryFile(indexStartConfig.getDiscoveryFile());
    }

    switch (indexStartConfig.getDataLocationType()) {
      case LOCAL:
        // data is present on local disk, no restore required
        break;
      case REMOTE:
        // restore previous remote backup
        requestBuilder.setRestore(
            RestoreIndex.newBuilder()
                .setServiceName(getConfiguration().getServiceName())
                .setResourceName(getUniqueIndexName(indexName, indexGlobalState.getId()))
                .setDeleteExistingData(true)
                .build());
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown index data location type: " + indexStartConfig.getDataLocationType());
    }

    StartIndexRequest startIndexRequest = requestBuilder.build();
    logger.info("Starting index: " + startIndexRequest);
    StartIndexResponse response = startIndex(indexStateManager, requestBuilder.build());
    logger.info("Index started: " + response);
    return response;
  }
}
