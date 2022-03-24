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
import com.yelp.nrtsearch.server.luceneserver.state.PersistentGlobalState.IndexInfo;
import com.yelp.nrtsearch.server.luceneserver.state.backend.LocalStateBackend;
import com.yelp.nrtsearch.server.luceneserver.state.backend.RemoteStateBackend;
import com.yelp.nrtsearch.server.luceneserver.state.backend.StateBackend;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
    public final Map<String, IndexState> indexStateMap;

    ImmutableState(
        PersistentGlobalState persistentGlobalState, Map<String, IndexState> indexStateMap) {
      this.persistentGlobalState = persistentGlobalState;
      this.indexStateMap = Collections.unmodifiableMap(indexStateMap);
    }
  }

  // volatile for atomic replacement
  private volatile ImmutableState immutableState;
  private final StateBackend stateBackend;

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
    immutableState =
        new ImmutableState(stateBackend.loadOrCreateGlobalState(), Collections.emptyMap());
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

  @VisibleForTesting
  StateBackend getStateBackend() {
    return stateBackend;
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
    Path rootDir = getIndexDir(name);
    if (immutableState.persistentGlobalState.getIndices().containsKey(name)) {
      throw new IllegalArgumentException("index \"" + name + "\" already exists");
    }
    if (Files.exists(rootDir)) {
      throw new IllegalArgumentException("rootDir \"" + rootDir + "\" already exists");
    }
    IndexState state = IndexState.createState(this, name, rootDir, true, false);

    Map<String, IndexInfo> updatedIndexInfoMap =
        new HashMap<>(immutableState.persistentGlobalState.getIndices());
    updatedIndexInfoMap.put(name, new IndexInfo());
    PersistentGlobalState updatedState =
        immutableState.persistentGlobalState.asBuilder().withIndices(updatedIndexInfoMap).build();
    stateBackend.commitGlobalState(updatedState);

    Map<String, IndexState> updatedIndexStateMap = new HashMap<>(immutableState.indexStateMap);
    updatedIndexStateMap.put(name, state);
    immutableState = new ImmutableState(updatedState, updatedIndexStateMap);

    return state;
  }

  @Override
  public IndexState getIndex(String name, boolean hasRestore) throws IOException {
    IndexState state = immutableState.indexStateMap.get(name);
    if (state != null) {
      return state;
    }
    synchronized (this) {
      state = immutableState.indexStateMap.get(name);
      if (state == null) {
        if (!immutableState.persistentGlobalState.getIndices().containsKey(name)) {
          throw new IllegalArgumentException("index \"" + name + "\" was not saved or committed");
        }
        Path rootPath = getIndexDir(name);
        state = IndexState.createState(this, name, rootPath, false, hasRestore);
        // nocommit we need to also persist which shards are here?
        state.addShard(0, false);

        Map<String, IndexState> updatedIndexStateMap = new HashMap<>(immutableState.indexStateMap);
        updatedIndexStateMap.put(name, state);
        immutableState =
            new ImmutableState(immutableState.persistentGlobalState, updatedIndexStateMap);
      }
      return state;
    }
  }

  @Override
  public IndexState getIndex(String name) throws IOException {
    return getIndex(name, false);
  }

  @Override
  public synchronized void deleteIndex(String name) throws IOException {
    Map<String, IndexInfo> updatedIndexInfoMap =
        new HashMap<>(immutableState.persistentGlobalState.getIndices());
    updatedIndexInfoMap.remove(name);
    PersistentGlobalState updatedState =
        immutableState.persistentGlobalState.asBuilder().withIndices(updatedIndexInfoMap).build();
    stateBackend.commitGlobalState(updatedState);

    immutableState = new ImmutableState(updatedState, new HashMap<>(immutableState.indexStateMap));
  }

  @Override
  public synchronized void indexClosed(String name) {
    Map<String, IndexState> updatedIndexStateMap = new HashMap<>(immutableState.indexStateMap);
    updatedIndexStateMap.remove(name);
    immutableState = new ImmutableState(immutableState.persistentGlobalState, updatedIndexStateMap);
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      logger.info("GlobalState.close: indices=" + getIndexNames());
      IOUtils.close(immutableState.indexStateMap.values());
    }
    super.close();
  }
}
