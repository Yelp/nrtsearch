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

import static com.yelp.nrtsearch.server.luceneserver.state.StateUtils.GLOBAL_STATE_FILE;
import static com.yelp.nrtsearch.server.luceneserver.state.StateUtils.GLOBAL_STATE_FOLDER;
import static com.yelp.nrtsearch.server.luceneserver.state.StateUtils.MAPPER;

import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.PersistentGlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** StateBackend implementation that stores state on the local filesystem. */
public class LocalStateBackend implements StateBackend {
  private static final Logger logger = LoggerFactory.getLogger(LocalStateBackend.class);

  private final Path globalStatePath;

  /**
   * Constructor.
   *
   * @param globalState global state
   */
  public LocalStateBackend(GlobalState globalState) {
    Objects.requireNonNull(globalState);
    this.globalStatePath = globalState.getStateDir().resolve(GLOBAL_STATE_FOLDER);
    StateUtils.ensureDirectory(globalStatePath);
  }

  @Override
  public PersistentGlobalState loadOrCreateGlobalState() throws IOException {
    logger.info("Loading local state");
    Path statePath = globalStatePath.resolve(GLOBAL_STATE_FILE);
    File stateFile = statePath.toFile();
    if (stateFile.isDirectory()) {
      throw new IllegalStateException("State file: " + stateFile + " is a directory");
    }
    if (!stateFile.exists()) {
      logger.info("Local state not present, initializing default");
      PersistentGlobalState state = new PersistentGlobalState();
      commitGlobalState(state);
      return state;
    } else {
      PersistentGlobalState persistentGlobalState = StateUtils.readStateFromFile(statePath);
      logger.info("Loaded local state: " + MAPPER.writeValueAsString(persistentGlobalState));
      return persistentGlobalState;
    }
  }

  @Override
  public void commitGlobalState(PersistentGlobalState persistentGlobalState) throws IOException {
    Objects.requireNonNull(persistentGlobalState);
    logger.info("Committing global state");
    StateUtils.writeStateToFile(persistentGlobalState, globalStatePath, GLOBAL_STATE_FILE);
    logger.info("Committed state: " + MAPPER.writeValueAsString(persistentGlobalState));
  }
}
