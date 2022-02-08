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

import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.config.StateConfig;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.PersistentGlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StateBackend implementation that persists state to a remote location using an {@link Archiver}.
 */
public class RemoteStateBackend implements StateBackend {
  public static final String GLOBAL_STATE_RESOURCE = "global_state";
  private static final Logger logger = LoggerFactory.getLogger(RemoteStateBackend.class);
  private final GlobalState globalState;
  private final Archiver archiver;
  private final Path localFilePath;
  private final RemoteBackendConfig config;

  /** Configuration class for state backend. */
  public static class RemoteBackendConfig {
    public static final String CONFIG_PREFIX = StateConfig.CONFIG_PREFIX + "remote.";
    private final boolean readOnly;

    /**
     * Read backend config from server config.
     *
     * @param luceneServerConfiguration server configuration
     * @return config for remote backend
     */
    public static RemoteBackendConfig fromConfig(
        LuceneServerConfiguration luceneServerConfiguration) {
      boolean readOnly =
          luceneServerConfiguration.getConfigReader().getBoolean(CONFIG_PREFIX + "readOnly", true);
      return new RemoteBackendConfig(readOnly);
    }

    /**
     * Constructor.
     *
     * @param readOnly is backend read only
     */
    public RemoteBackendConfig(boolean readOnly) {
      this.readOnly = readOnly;
    }

    /** Get if backend is read only. */
    public boolean getReadOnly() {
      return readOnly;
    }
  }

  /**
   * Constructor.
   *
   * @param globalState global state
   */
  public RemoteStateBackend(GlobalState globalState) {
    Objects.requireNonNull(globalState);
    this.globalState = globalState;
    this.config = RemoteBackendConfig.fromConfig(globalState.getConfiguration());
    this.archiver =
        globalState
            .getIncArchiver()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Archiver must be provided for remote state usage"));
    this.localFilePath = globalState.getStateDir().resolve(GLOBAL_STATE_FOLDER);
    StateUtils.ensureDirectory(localFilePath);
  }

  @Override
  public PersistentGlobalState loadOrCreateGlobalState() throws IOException {
    logger.info("Loading remote state");
    Path downloadedPath =
        archiver.download(globalState.getConfiguration().getServiceName(), GLOBAL_STATE_RESOURCE);
    if (downloadedPath == null) {
      PersistentGlobalState state = new PersistentGlobalState();
      logger.info("Remote state not present, initializing default");
      commitGlobalState(state);
      return state;
    } else {
      Path downloadedStateFilePath =
          downloadedPath.resolve(GLOBAL_STATE_FOLDER).resolve(GLOBAL_STATE_FILE);
      if (!downloadedStateFilePath.toFile().exists()) {
        throw new IllegalStateException("No state file present in downloaded directory");
      }
      // copy restored state to local state directory, not strictly required but ensures a
      // current copy of the state is always in the local directory
      Files.copy(
          downloadedStateFilePath,
          localFilePath.resolve(GLOBAL_STATE_FILE),
          StandardCopyOption.REPLACE_EXISTING);
      PersistentGlobalState persistentGlobalState =
          StateUtils.readStateFromFile(downloadedStateFilePath);
      logger.info("Loaded remote state: " + MAPPER.writeValueAsString(persistentGlobalState));
      return persistentGlobalState;
    }
  }

  @Override
  public void commitGlobalState(PersistentGlobalState persistentGlobalState) throws IOException {
    Objects.requireNonNull(persistentGlobalState);
    logger.info("Committing global state");
    if (config.getReadOnly()) {
      throw new IllegalStateException("Cannot update remote state when configured as read only");
    }
    StateUtils.writeStateToFile(persistentGlobalState, localFilePath, GLOBAL_STATE_FILE);
    String version =
        archiver.upload(
            globalState.getConfiguration().getServiceName(),
            GLOBAL_STATE_RESOURCE,
            localFilePath,
            Collections.singletonList(GLOBAL_STATE_FILE),
            Collections.emptyList(),
            true);
    archiver.blessVersion(
        globalState.getConfiguration().getServiceName(), GLOBAL_STATE_RESOURCE, version);
    logger.info("Committed state: " + MAPPER.writeValueAsString(persistentGlobalState));
  }
}
