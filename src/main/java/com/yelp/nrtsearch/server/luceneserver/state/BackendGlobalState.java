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
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.index.BackendStateManager;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import com.yelp.nrtsearch.server.luceneserver.state.PersistentGlobalState.IndexInfo;
import com.yelp.nrtsearch.server.luceneserver.state.backend.LocalStateBackend;
import com.yelp.nrtsearch.server.luceneserver.state.backend.RemoteStateBackend;
import com.yelp.nrtsearch.server.luceneserver.state.backend.StateBackend;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of GlobalState that uses a configurable {@link StateBackend} for storing/loading
 * state.
 */
public class BackendGlobalState extends GlobalState {
  private static final Logger logger = LoggerFactory.getLogger(BackendGlobalState.class);

  /**
   * State class containing immutable persistent and ephemeral global state, stored together so that
   * they can be updated atomically.
   */
  private static class ImmutableState {
    public final PersistentGlobalState persistentGlobalState;
    public final Map<String, IndexStateManager> indexStateManagerMap;

    ImmutableState(
        PersistentGlobalState persistentGlobalState,
        Map<String, IndexStateManager> indexStateManagerMap) {
      this.persistentGlobalState = persistentGlobalState;
      this.indexStateManagerMap = Collections.unmodifiableMap(indexStateManagerMap);
    }
  }

  // volatile for atomic replacement
  private volatile ImmutableState immutableState;
  private final StateBackend stateBackend;

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
    super(luceneServerConfiguration, incArchiver);
    stateBackend = createStateBackend();
    PersistentGlobalState persistentGlobalState = stateBackend.loadOrCreateGlobalState();
    // init index state managers
    Map<String, IndexStateManager> managerMap = new HashMap<>();
    for (Map.Entry<String, IndexInfo> entry : persistentGlobalState.getIndices().entrySet()) {
      IndexStateManager stateManager =
          createIndexStateManager(entry.getKey(), entry.getValue().getId(), stateBackend);
      stateManager.load();
      managerMap.put(entry.getKey(), stateManager);
    }
    immutableState = new ImmutableState(persistentGlobalState, managerMap);
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
  public Path getIndexDir(String indexName) {
    try {
      return getIndex(indexName).getRootDir();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getDataResourceForIndex(String indexName) {
    IndexInfo info = immutableState.persistentGlobalState.getIndices().get(indexName);
    if (info == null) {
      throw new IllegalArgumentException("index \"" + indexName + "\" was not saved or committed");
    }
    return getUniqueIndexName(indexName, info.getId());
  }

  @Override
  public void setStateDir(Path source) throws IOException {
    // only called by legacy restore
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getIndexNames() {
    return immutableState.persistentGlobalState.getIndices().keySet();
  }

  @Override
  public synchronized IndexState createIndex(String name) throws IOException {
    if (immutableState.persistentGlobalState.getIndices().containsKey(name)) {
      throw new IllegalArgumentException("index \"" + name + "\" already exists");
    }
    String indexId = getIndexId();
    IndexStateManager stateManager = createIndexStateManager(name, indexId, stateBackend);
    stateManager.create();

    Map<String, IndexInfo> updatedIndexInfoMap =
        new HashMap<>(immutableState.persistentGlobalState.getIndices());
    updatedIndexInfoMap.put(name, new IndexInfo(indexId));
    PersistentGlobalState updatedState =
        immutableState.persistentGlobalState.asBuilder().withIndices(updatedIndexInfoMap).build();
    stateBackend.commitGlobalState(updatedState);

    Map<String, IndexStateManager> updatedIndexStateManagerMap =
        new HashMap<>(immutableState.indexStateManagerMap);
    updatedIndexStateManagerMap.put(name, stateManager);
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
    Map<String, IndexInfo> updatedIndexInfoMap =
        new HashMap<>(immutableState.persistentGlobalState.getIndices());
    updatedIndexInfoMap.remove(name);
    PersistentGlobalState updatedState =
        immutableState.persistentGlobalState.asBuilder().withIndices(updatedIndexInfoMap).build();
    stateBackend.commitGlobalState(updatedState);

    IndexStateManager stateManager = immutableState.indexStateManagerMap.get(name);
    Map<String, IndexStateManager> updatedIndexStateManagerMap =
        new HashMap<>(immutableState.indexStateManagerMap);
    updatedIndexStateManagerMap.remove(name);

    immutableState = new ImmutableState(updatedState, updatedIndexStateManagerMap);
    stateManager.close();
  }

  @Override
  public synchronized void indexClosed(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      logger.info("GlobalState.close: indices=" + getIndexNames());
      IOUtils.close(immutableState.indexStateManagerMap.values());
    }
    super.close();
  }
}
