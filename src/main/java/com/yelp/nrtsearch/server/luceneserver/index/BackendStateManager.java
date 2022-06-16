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

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.index.handlers.FieldUpdateHandler;
import com.yelp.nrtsearch.server.luceneserver.index.handlers.FieldUpdateHandler.UpdatedFieldInfo;
import com.yelp.nrtsearch.server.luceneserver.state.BackendGlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.backend.StateBackend;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State manager implementation that uses a {@link StateBackend} implementation to load/store index
 * state.
 */
public class BackendStateManager implements IndexStateManager {
  private static final Logger logger = LoggerFactory.getLogger(BackendStateManager.class);

  private final String indexName;
  private final String indexUniqueName;
  private final String id;
  private final StateBackend stateBackend;
  private final GlobalState globalState;

  // volatile for atomic replacement
  private volatile ImmutableIndexState currentState;

  /**
   * Constructor
   *
   * @param indexName index name
   * @param id index instance id
   * @param stateBackend state backend
   * @param globalState global state
   */
  public BackendStateManager(
      String indexName, String id, StateBackend stateBackend, GlobalState globalState) {
    this.indexName = indexName;
    this.id = id;
    this.indexUniqueName = BackendGlobalState.getUniqueIndexName(indexName, id);
    this.stateBackend = stateBackend;
    this.globalState = globalState;
  }

  @Override
  public synchronized void load() throws IOException {
    logger.info("Loading state for index: " + indexName);
    IndexStateInfo stateInfo = stateBackend.loadIndexState(indexUniqueName);
    if (stateInfo == null) {
      throw new IllegalStateException("No committed state for index: " + indexName);
    }
    // If this state was restored from an index snapshot, it may have the previous index
    // name. Let's fix it here so that it updates on the next commit.
    stateInfo = fixIndexName(stateInfo, indexName);
    UpdatedFieldInfo updatedFieldInfo =
        FieldUpdateHandler.updateFields(
            new FieldAndFacetState(), Collections.emptyMap(), stateInfo.getFieldsMap().values());
    currentState = createIndexState(stateInfo, updatedFieldInfo.fieldAndFacetState);
  }

  /**
   * Update index name in {@link IndexStateInfo} to the given value, if it is different.
   *
   * @param indexStateInfo index state info
   * @param indexName index name
   * @return index state info with given index name set
   */
  @VisibleForTesting
  static IndexStateInfo fixIndexName(IndexStateInfo indexStateInfo, String indexName) {
    Objects.requireNonNull(indexStateInfo);
    Objects.requireNonNull(indexName);
    if (!indexName.equals(indexStateInfo.getIndexName())) {
      return indexStateInfo.toBuilder().setIndexName(indexName).build();
    } else {
      return indexStateInfo;
    }
  }

  @Override
  public synchronized void create() throws IOException {
    logger.info("Creating state for index: " + indexName);
    IndexStateInfo stateInfo = stateBackend.loadIndexState(indexUniqueName);
    // maybe we could make create requests idempotent by not failing if the existing state
    // is the same as the default state
    if (stateInfo != null) {
      throw new IllegalStateException("Creating index, but state already exists for: " + indexName);
    }
    stateInfo = getDefaultStateInfo();
    ImmutableIndexState indexState = createIndexState(stateInfo, new FieldAndFacetState());
    stateBackend.commitIndexState(indexUniqueName, stateInfo);
    currentState = indexState;
  }

  @Override
  public IndexSettings getSettings() {
    ImmutableIndexState indexState = currentState;
    if (indexState == null) {
      throw new IllegalStateException("No state for index: " + indexName);
    }
    return indexState.getMergedSettings();
  }

  @Override
  public synchronized IndexSettings updateSettings(IndexSettings settings) throws IOException {
    logger.info("Updating settings for index: " + indexName + " : " + settings);
    if (currentState == null) {
      throw new IllegalStateException("No state for index: " + indexName);
    }
    if (currentState.isStarted()) {
      throw new IllegalStateException("Cannot change setting for started index: " + indexName);
    }
    IndexStateInfo updatedStateInfo = mergeSettings(currentState.getCurrentStateInfo(), settings);
    ImmutableIndexState updatedIndexState =
        createIndexState(updatedStateInfo, currentState.getFieldAndFacetState());
    stateBackend.commitIndexState(indexUniqueName, updatedStateInfo);
    currentState = updatedIndexState;
    return updatedIndexState.getMergedSettings();
  }

  @Override
  public IndexLiveSettings getLiveSettings() {
    ImmutableIndexState indexState = currentState;
    if (indexState == null) {
      throw new IllegalStateException("No state for index: " + indexName);
    }
    return indexState.getMergedLiveSettings();
  }

  @Override
  public synchronized IndexLiveSettings updateLiveSettings(IndexLiveSettings liveSettings)
      throws IOException {
    logger.info("Updating live settings for index: " + indexName + " : " + liveSettings);
    if (currentState == null) {
      throw new IllegalStateException("No state for index: " + indexName);
    }
    IndexStateInfo updatedStateInfo =
        mergeLiveSettings(currentState.getCurrentStateInfo(), liveSettings);
    ImmutableIndexState updatedIndexState =
        createIndexState(updatedStateInfo, currentState.getFieldAndFacetState());
    stateBackend.commitIndexState(indexUniqueName, updatedStateInfo);
    currentState = updatedIndexState;
    for (Map.Entry<Integer, ShardState> entry : currentState.getShards().entrySet()) {
      entry.getValue().updatedLiveSettings(liveSettings);
    }
    return updatedIndexState.getMergedLiveSettings();
  }

  @Override
  public synchronized String updateFields(List<Field> fields) throws IOException {
    if (currentState == null) {
      throw new IllegalStateException("No state for index: " + indexName);
    }
    UpdatedFieldInfo updatedFieldInfo =
        FieldUpdateHandler.updateFields(
            currentState.getFieldAndFacetState(),
            currentState.getCurrentStateInfo().getFieldsMap(),
            fields);
    IndexStateInfo updatedStateInfo =
        replaceFields(currentState.getCurrentStateInfo(), updatedFieldInfo.fields);
    ImmutableIndexState updatedIndexState =
        createIndexState(updatedStateInfo, updatedFieldInfo.fieldAndFacetState);
    stateBackend.commitIndexState(indexUniqueName, updatedStateInfo);
    currentState = updatedIndexState;
    return updatedIndexState.getAllFieldsJSON();
  }

  @Override
  public synchronized void start(
      Mode serverMode, Path dataPath, long primaryGen, ReplicationServerClient primaryClient)
      throws IOException {
    if (currentState == null) {
      throw new IllegalStateException("No state for index: " + indexName);
    }
    if (currentState.isStarted()) {
      throw new IllegalStateException("Index already started: " + indexName);
    }
    currentState.start(serverMode, dataPath, primaryGen, primaryClient);
    if (serverMode != Mode.REPLICA && !currentState.getCurrentStateInfo().getCommitted()) {
      logger.info("Doing initial commit for index: " + indexName);
      currentState.commit(globalState.getConfiguration().getBackupWithInArchiver());
      IndexStateInfo updatedStateInfo =
          currentState
              .getCurrentStateInfo()
              .toBuilder()
              .setCommitted(true)
              .setGen(currentState.getCurrentStateInfo().getGen() + 1)
              .build();
      ImmutableIndexState updatedIndexState =
          createIndexState(updatedStateInfo, currentState.getFieldAndFacetState());
      stateBackend.commitIndexState(indexUniqueName, updatedStateInfo);
      currentState = updatedIndexState;
    }
  }

  @Override
  public IndexState getCurrent() {
    IndexState state = currentState;
    if (state == null) {
      throw new IllegalStateException("No state for index: " + indexName);
    }
    return state;
  }

  // Declared protected for use during testing
  protected ImmutableIndexState createIndexState(
      IndexStateInfo indexStateInfo, FieldAndFacetState fieldAndFacetState) throws IOException {
    Map<Integer, ShardState> previousShardState =
        currentState == null ? null : currentState.getShards();
    return new ImmutableIndexState(
        this,
        globalState,
        indexName,
        indexUniqueName,
        indexStateInfo,
        fieldAndFacetState,
        previousShardState);
  }

  IndexStateInfo getDefaultStateInfo() {
    return IndexStateInfo.newBuilder()
        .setIndexName(indexName)
        .setGen(1)
        .setCommitted(false)
        .setSettings(IndexSettings.newBuilder().build())
        .setLiveSettings(IndexLiveSettings.newBuilder().build())
        .build();
  }

  private static IndexStateInfo mergeSettings(
      IndexStateInfo currentStateInfo, IndexSettings settings) {
    IndexSettings mergedSettings =
        ImmutableIndexState.mergeSettings(currentStateInfo.getSettings(), settings);
    return currentStateInfo
        .toBuilder()
        .setSettings(mergedSettings)
        .setGen(currentStateInfo.getGen() + 1)
        .build();
  }

  private static IndexStateInfo mergeLiveSettings(
      IndexStateInfo currentStateInfo, IndexLiveSettings liveSettings) {
    IndexLiveSettings mergedLiveSettings =
        ImmutableIndexState.mergeLiveSettings(currentStateInfo.getLiveSettings(), liveSettings);
    return currentStateInfo
        .toBuilder()
        .setLiveSettings(mergedLiveSettings)
        .setGen(currentStateInfo.getGen() + 1)
        .build();
  }

  private static IndexStateInfo replaceFields(
      IndexStateInfo currentStateInfo, Map<String, Field> newFields) {
    IndexStateInfo.Builder stateInfoBuilder = currentStateInfo.toBuilder();
    stateInfoBuilder.putAllFields(newFields).setGen(currentStateInfo.getGen() + 1);
    return stateInfoBuilder.build();
  }

  @Override
  public synchronized void close() throws IOException {
    if (currentState != null) {
      currentState.close();
    }
  }

  @Override
  public String getIndexId() {
    return id;
  }
}
