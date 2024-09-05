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
import static com.yelp.nrtsearch.server.luceneserver.state.StateUtils.INDEX_STATE_FILE;
import static com.yelp.nrtsearch.server.luceneserver.state.StateUtils.ensureDirectory;

import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.config.StateConfig;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StateBackend implementation that persists state to a remote location using a {@link
 * RemoteBackend}.
 */
public class RemoteStateBackend implements StateBackend {
  public static final String GLOBAL_STATE_RESOURCE = "global_state";
  private static final Logger logger = LoggerFactory.getLogger(RemoteStateBackend.class);
  private final GlobalState globalState;
  private final RemoteBackend remoteBackend;
  private final String serviceName;
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
    this.remoteBackend = globalState.getRemoteBackend();
    this.serviceName = globalState.getConfiguration().getServiceName();
    this.config = RemoteBackendConfig.fromConfig(globalState.getConfiguration());
    this.localFilePath = globalState.getStateDir().resolve(GLOBAL_STATE_FOLDER);
    StateUtils.ensureDirectory(localFilePath);
  }

  @Override
  public GlobalStateInfo loadOrCreateGlobalState() throws IOException {
    logger.info("Loading remote state");
    boolean exists =
        remoteBackend.exists(serviceName, RemoteBackend.GlobalResourceType.GLOBAL_STATE);
    if (!exists) {
      GlobalStateInfo state = GlobalStateInfo.newBuilder().build();
      logger.info("Remote state not present, initializing default");
      commitGlobalState(state);
      return state;
    } else {
      InputStream stateStream = remoteBackend.downloadGlobalState(serviceName);
      byte[] stateBytes = stateStream.readAllBytes();
      // copy restored state to local state directory, not strictly required but ensures a
      // current copy of the state is always in the local directory
      StateUtils.writeToFile(stateBytes, localFilePath, GLOBAL_STATE_FILE);
      GlobalStateInfo globalStateInfo = StateUtils.globalStateFromUTF8(stateBytes);
      logger.info("Loaded remote state: " + JsonFormat.printer().print(globalStateInfo));
      return globalStateInfo;
    }
  }

  @Override
  public void commitGlobalState(GlobalStateInfo globalStateInfo) throws IOException {
    Objects.requireNonNull(globalStateInfo);
    logger.info("Committing global state");
    if (config.getReadOnly()) {
      throw new IllegalStateException("Cannot update remote state when configured as read only");
    }
    byte[] stateBytes = StateUtils.globalStateToUTF8(globalStateInfo);
    StateUtils.writeToFile(stateBytes, localFilePath, GLOBAL_STATE_FILE);
    remoteBackend.uploadGlobalState(serviceName, stateBytes);
    logger.info("Committed state: " + JsonFormat.printer().print(globalStateInfo));
  }

  @Override
  public IndexStateInfo loadIndexState(String indexIdentifier) throws IOException {
    Objects.requireNonNull(indexIdentifier);
    logger.info("Loading remote state for index: " + indexIdentifier);
    boolean exists =
        remoteBackend.exists(
            serviceName, indexIdentifier, RemoteBackend.IndexResourceType.INDEX_STATE);
    if (!exists) {
      logger.info("Remote state not present for index: " + indexIdentifier);
      return null;
    } else {
      InputStream stateStream = remoteBackend.downloadIndexState(serviceName, indexIdentifier);
      byte[] stateBytes = stateStream.readAllBytes();
      // copy restored state to local state directory, not strictly required but ensures a
      // current copy of the state is always in the local directory
      Path localIndexStateDirPath = globalState.getStateDir().resolve(indexIdentifier);
      StateUtils.ensureDirectory(localIndexStateDirPath);
      StateUtils.writeToFile(stateBytes, localIndexStateDirPath, INDEX_STATE_FILE);
      IndexStateInfo loadedState = StateUtils.indexStateFromUTF8(stateBytes);
      logger.info(
          "Loaded remote state for index: "
              + indexIdentifier
              + " : "
              + JsonFormat.printer().print(loadedState));
      return loadedState;
    }
  }

  @Override
  public void commitIndexState(String indexIdentifier, IndexStateInfo indexStateInfo)
      throws IOException {
    Objects.requireNonNull(indexIdentifier);
    Objects.requireNonNull(indexStateInfo);
    logger.info("Committing state for index: " + indexIdentifier);
    if (config.getReadOnly()) {
      throw new IllegalStateException("Cannot update remote state when configured as read only");
    }
    Path indexStatePath = globalState.getStateDir().resolve(indexIdentifier);
    ensureDirectory(indexStatePath);
    byte[] stateBytes = StateUtils.indexStateToUTF8(indexStateInfo);
    StateUtils.writeToFile(stateBytes, indexStatePath, INDEX_STATE_FILE);
    remoteBackend.uploadIndexState(serviceName, indexIdentifier, stateBytes);
    logger.info("Committed index state: " + JsonFormat.printer().print(indexStateInfo));
  }
}
