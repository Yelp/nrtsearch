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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.config.StateConfig;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.IndexBackupUtils;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
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
  public GlobalStateInfo loadOrCreateGlobalState() throws IOException {
    logger.info("Loading remote state");
    Path downloadedPath =
        archiver.download(globalState.getConfiguration().getServiceName(), GLOBAL_STATE_RESOURCE);
    if (downloadedPath == null) {
      GlobalStateInfo state = GlobalStateInfo.newBuilder().build();
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
      GlobalStateInfo globalStateInfo = StateUtils.readStateFromFile(downloadedStateFilePath);
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
    StateUtils.writeStateToFile(globalStateInfo, localFilePath, GLOBAL_STATE_FILE);
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
    logger.info("Committed state: " + JsonFormat.printer().print(globalStateInfo));
  }

  @Override
  public IndexStateInfo loadIndexState(String indexIdentifier) throws IOException {
    Objects.requireNonNull(indexIdentifier);
    logger.info("Loading remote state for index: " + indexIdentifier);
    String indexStateResourceName = indexIdentifier + IndexBackupUtils.INDEX_STATE_SUFFIX;
    Path downloadedPath =
        archiver.download(globalState.getConfiguration().getServiceName(), indexStateResourceName);
    if (downloadedPath == null) {
      logger.info("Remote state not present for index: " + indexIdentifier);
      return null;
    } else {
      Path downloadedStateFilePath = findIndexStateFile(downloadedPath);
      if (!downloadedStateFilePath.toFile().exists()) {
        throw new IllegalStateException(
            "No index state file present in downloaded directory: " + downloadedStateFilePath);
      }
      // copy restored state to local state directory, not strictly required but ensures a
      // current copy of the state is always in the local directory
      Path localIndexStateDirPath = globalState.getStateDir().resolve(indexIdentifier);
      StateUtils.ensureDirectory(localIndexStateDirPath);
      Path localIndexStateFilePath = localIndexStateDirPath.resolve(INDEX_STATE_FILE);
      Files.copy(
          downloadedStateFilePath, localIndexStateFilePath, StandardCopyOption.REPLACE_EXISTING);
      IndexStateInfo loadedState = StateUtils.readIndexStateFromFile(localIndexStateFilePath);
      logger.info(
          "Loaded remote state for index: "
              + indexIdentifier
              + " : "
              + JsonFormat.printer().print(loadedState));
      return loadedState;
    }
  }

  /**
   * Find the index state file in the downloaded path. Searches two levels deep for the file
   * index_state.json
   *
   * @param downloadedPath path to downloaded index state from {@link Archiver}
   * @return path to index state file
   * @throws IOException on filesystem error
   * @throws IllegalArgumentException if more or less than one state file is found
   */
  @VisibleForTesting
  static Path findIndexStateFile(Path downloadedPath) throws IOException {
    Objects.requireNonNull(downloadedPath);
    List<Path> stateFiles =
        Files.find(
                downloadedPath,
                2,
                (path, attrib) -> INDEX_STATE_FILE.equals(path.getFileName().toString()),
                FileVisitOption.FOLLOW_LINKS)
            .collect(Collectors.toList());
    if (stateFiles.isEmpty()) {
      throw new IllegalArgumentException(
          "No index state file found in downloadPath: " + downloadedPath);
    } else if (stateFiles.size() > 1) {
      throw new IllegalArgumentException(
          "Multiple index state files found in downloadedPath: "
              + downloadedPath
              + ", files: "
              + stateFiles);
    } else {
      return stateFiles.get(0);
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
    String indexStateResourceName = indexIdentifier + IndexBackupUtils.INDEX_STATE_SUFFIX;
    StateUtils.writeIndexStateToFile(indexStateInfo, indexStatePath, INDEX_STATE_FILE);
    String version =
        archiver.upload(
            globalState.getConfiguration().getServiceName(),
            indexStateResourceName,
            indexStatePath,
            Collections.singletonList(INDEX_STATE_FILE),
            Collections.emptyList(),
            true);
    archiver.blessVersion(
        globalState.getConfiguration().getServiceName(), indexStateResourceName, version);
    logger.info("Committed index state: " + JsonFormat.printer().print(indexStateInfo));
  }
}
