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
package com.yelp.nrtsearch.server.state;

import static com.yelp.nrtsearch.server.utils.TimeStringUtils.generateTimeStringMs;

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.concurrent.ExecutorFactory;
import com.yelp.nrtsearch.server.config.IndexStartConfig;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.config.IsolatedReplicaConfig;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.config.StateConfig.StateBackendType;
import com.yelp.nrtsearch.server.grpc.CreateIndexRequest;
import com.yelp.nrtsearch.server.grpc.DummyResponse;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexGlobalState;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.RestoreIndex;
import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import com.yelp.nrtsearch.server.grpc.StartIndexResponse;
import com.yelp.nrtsearch.server.grpc.StartIndexV2Request;
import com.yelp.nrtsearch.server.grpc.StopIndexRequest;
import com.yelp.nrtsearch.server.handler.Handler;
import com.yelp.nrtsearch.server.index.BackendStateManager;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.IndexStateManager;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.index.StartIndexProcessor;
import com.yelp.nrtsearch.server.index.StartIndexProcessor.StartIndexProcessorException;
import com.yelp.nrtsearch.server.leader.FencedException;
import com.yelp.nrtsearch.server.leader.LeaderElection;
import com.yelp.nrtsearch.server.leader.LeaderInfo;
import com.yelp.nrtsearch.server.leader.LeaderManager;
import com.yelp.nrtsearch.server.leader.PrimaryElector;
import com.yelp.nrtsearch.server.leader.PrimaryElectorContext;
import com.yelp.nrtsearch.server.leader.PrimaryElectorCreator;
import com.yelp.nrtsearch.server.nrt.NrtDataManager;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.state.backend.LocalStateBackend;
import com.yelp.nrtsearch.server.state.backend.RemoteStateBackend;
import com.yelp.nrtsearch.server.state.backend.StateBackend;
import io.grpc.Status;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
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
  private final LeaderManager leaderManager;

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
   * Return the base index name from the unique index identifier.
   *
   * @param uniqueIndexName index identifier
   * @return indexName index name
   * @throws NullPointerException if uniqueIndexName is null
   */
  public static String getBaseIndexName(String uniqueIndexName) {
    Objects.requireNonNull(uniqueIndexName);
    // unique identifier is in yyyyMMddHHmmssSSS time format, and it is safe to do a reverse split
    // to get the base index name
    return StringUtils.substringBeforeLast(uniqueIndexName, "-");
  }

  /**
   * Constructor.
   *
   * @param configuration server config
   * @param remoteBackend backend for persistent remote storage
   * @param executorFactory executor factory
   * @throws IOException on filesystem error
   */
  public BackendGlobalState(
      NrtsearchConfig configuration, RemoteBackend remoteBackend, ExecutorFactory executorFactory)
      throws IOException {
    this(configuration, remoteBackend, executorFactory, Optional.empty(), null);
  }

  /**
   * Constructor.
   *
   * @param configuration server config
   * @param remoteBackend backend for persistent remote storage, must have been constructed with the
   *     leader election fence when leader election is enabled
   * @param executorFactory executor factory
   * @param leaderElection leader election components, required if enabled in config
   * @param prometheusRegistry metrics registry, may be null
   * @throws IOException on filesystem error
   */
  public BackendGlobalState(
      NrtsearchConfig configuration,
      RemoteBackend remoteBackend,
      ExecutorFactory executorFactory,
      Optional<LeaderElection> leaderElection,
      PrometheusRegistry prometheusRegistry)
      throws IOException {
    super(configuration, remoteBackend, executorFactory);
    leaderManager = createLeaderManager(leaderElection, prometheusRegistry);
    stateBackend = createStateBackend();
    GlobalStateInfo globalStateInfo = loadGlobalState();
    // init index state managers
    Map<String, IndexStateManager> managerMap = new HashMap<>();
    for (Map.Entry<String, IndexGlobalState> entry : globalStateInfo.getIndicesMap().entrySet()) {
      IndexStateManager stateManager =
          createIndexStateManager(
              entry.getKey(),
              entry.getValue().getId(),
              configuration.getLiveSettingsOverride(entry.getKey()),
              stateBackend);
      stateManager.load();
      managerMap.put(entry.getKey(), stateManager);
    }
    immutableState = new ImmutableState(globalStateInfo, managerMap);
    // If any indices should be started, it will be done in the replicationStarted hook
  }

  /**
   * Create the {@link LeaderManager} if leader election is enabled. The pointer fence starts
   * inactive, so this node cannot write remote state until it is elected.
   */
  private LeaderManager createLeaderManager(
      Optional<LeaderElection> leaderElection, PrometheusRegistry prometheusRegistry) {
    NrtsearchConfig config = getConfiguration();
    if (!config.getLeaderElectionConfig().isEnabled()) {
      return null;
    }
    validateLeaderElectionConfig(config);
    LeaderElection election =
        leaderElection.orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Leader election is enabled, but leader election components were not provided"));
    LeaderInfo localInfo =
        new LeaderInfo(
            getEphemeralId(),
            getNodeName(),
            getHostName(),
            getPort(),
            config.getReplicationPort(),
            -1);
    PrimaryElector elector =
        PrimaryElectorCreator.getInstance()
            .createElector(
                new PrimaryElectorContext(
                    config, localInfo, election.conditionalStore(), prometheusRegistry))
            .orElseThrow();
    logger.info(
        "Leader election enabled, type: {}, starting as standby",
        config.getLeaderElectionConfig().getType());
    return new LeaderManager(
        elector,
        election.fence(),
        config.getLeaderElectionConfig().getClaimConcurrency(),
        new RoleController(),
        config.getLeaderElectionConfig().getRefreshIntervalMs());
  }

  /** Verify configuration is compatible with multi-primary leader election. */
  static void validateLeaderElectionConfig(NrtsearchConfig config) {
    IndexStartConfig startConfig = config.getIndexStartConfig();
    List<String> errors = new ArrayList<>();
    if (startConfig.getMode() != Mode.PRIMARY) {
      errors.add("indexStartConfig.mode must be PRIMARY");
    }
    if (startConfig.getDataLocationType() != IndexDataLocationType.REMOTE) {
      errors.add("indexStartConfig.dataLocationType must be REMOTE");
    }
    if (config.getStateConfig().getBackendType() != StateBackendType.REMOTE) {
      errors.add("stateConfig.backendType must be REMOTE");
    }
    // replicas must only advance to points that are durable in remote storage, so that they are
    // never ahead of the point a new primary restores from
    if (!config.getS3RefreshUpload()) {
      errors.add("s3RefreshUpload must be true");
    }
    if (!config.getS3RefreshNotifyAfterUpload()) {
      errors.add("s3RefreshNotifyAfterUpload must be true");
    }
    if (!config.getEnforcePrimaryGenOrdering()) {
      errors.add("enforcePrimaryGenOrdering must be true");
    }
    if (!errors.isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid configuration for leader election: " + String.join(", ", errors));
    }
  }

  /**
   * Load global state from the backend. When leader election is enabled and this node is not the
   * active primary, missing state is not created, since only the leader may write it; an empty
   * state is used until the leader creates it.
   */
  private GlobalStateInfo loadGlobalState() throws IOException {
    if (leaderManager != null && !leaderManager.getFence().isActive()) {
      if (!getRemoteBackend()
          .exists(
              getConfiguration().getServiceName(), RemoteBackend.GlobalResourceType.GLOBAL_STATE)) {
        logger.info("Global state does not exist yet, waiting for leader to create it");
        return GlobalStateInfo.newBuilder().build();
      }
    }
    return stateBackend.loadOrCreateGlobalState();
  }

  @Override
  public Optional<LeaderManager> getLeaderManager() {
    return Optional.ofNullable(leaderManager);
  }

  /**
   * Create {@link StateBackend} based on the current configuration. Protected to allow injection
   * for testing.
   */
  protected StateBackend createStateBackend() {
    return switch (getConfiguration().getStateConfig().getBackendType()) {
      case LOCAL -> new LocalStateBackend(this);
      case REMOTE -> new RemoteStateBackend(this);
      default ->
          throw new IllegalArgumentException(
              "Unsupported state backend type: "
                  + getConfiguration().getStateConfig().getBackendType());
    };
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
      String indexName,
      String indexId,
      IndexLiveSettings liveSettingsOverrides,
      StateBackend stateBackend) {
    return new BackendStateManager(indexName, indexId, liveSettingsOverrides, stateBackend, this);
  }

  /**
   * Generate a unique id to identify an index instance. Protected to allow injection for testing.
   */
  protected String getIndexId() {
    return generateTimeStringMs();
  }

  @VisibleForTesting
  StateBackend getStateBackend() {
    return stateBackend;
  }

  @Override
  public void reloadStateFromBackend() throws IOException {
    if (leaderManager != null) {
      // with leader election, the leader manager owns all changes to index managers; request a
      // refresh, which runs between role changes, and wait a bounded time for it
      long timeoutMs = getConfiguration().getLeaderElectionConfig().getReloadWaitMs();
      try {
        leaderManager.requestRefresh().get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        // the refresh is still queued or running and will complete in the background
        throw Status.DEADLINE_EXCEEDED
            .withDescription(
                "State refresh did not finish within "
                    + timeoutMs
                    + "ms, it continues in the"
                    + " background")
            .asRuntimeException();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted waiting for state refresh", e);
      } catch (ExecutionException e) {
        throw new IOException("State refresh failed", e.getCause());
      }
      return;
    }
    reloadStateFromBackendLocked();
  }

  /** Reload without leader election, same as before multi-primary support. */
  private synchronized void reloadStateFromBackendLocked() throws IOException {
    // replaced managers are not closed here, unchanged from before multi-primary support
    ImmutableState newImmutableState = buildReloadedState(new ArrayList<>());
    if (getConfiguration().getIndexStartConfig().getAutoStart()) {
      updateStartedIndices(newImmutableState, LeaderStartRole.FROM_ROLE);
    }
    this.immutableState = newImmutableState;
  }

  /**
   * Build state reloaded from the backend, reusing current index state managers when the index id
   * is unchanged. Managers that are no longer used are added to {@code replaced}. Must hold the
   * global state lock.
   */
  private ImmutableState buildReloadedState(List<IndexStateManager> replaced) throws IOException {
    GlobalStateInfo newGlobalStateInfo = loadGlobalState();
    Map<String, IndexStateManager> newManagerMap = new HashMap<>();
    for (Map.Entry<String, IndexGlobalState> entry :
        newGlobalStateInfo.getIndicesMap().entrySet()) {
      String indexName = entry.getKey();
      IndexStateManager stateManager = immutableState.indexStateManagerMap.get(indexName);
      if (stateManager == null || !entry.getValue().getId().equals(stateManager.getIndexId())) {
        if (stateManager != null) {
          replaced.add(stateManager);
        }
        stateManager =
            createIndexStateManager(
                indexName,
                entry.getValue().getId(),
                getConfiguration().getLiveSettingsOverride(indexName),
                stateBackend);
      }
      stateManager.load();
      newManagerMap.put(indexName, stateManager);
    }
    for (Map.Entry<String, IndexStateManager> entry :
        immutableState.indexStateManagerMap.entrySet()) {
      if (!newManagerMap.containsKey(entry.getKey())) {
        replaced.add(entry.getValue());
      }
    }
    return new ImmutableState(newGlobalStateInfo, newManagerMap);
  }

  /** Close index state managers no longer in use after a reload. */
  private void closeReplaced(List<IndexStateManager> replaced) {
    for (IndexStateManager manager : replaced) {
      try {
        manager.close();
      } catch (IOException e) {
        logger.warn("Error closing replaced index state manager", e);
      }
    }
  }

  @Override
  public int getReplicationPort() {
    return resolvedReplicationPort;
  }

  @Override
  public Path getIndexDir(String indexName) {
    try {
      return getIndexOrThrow(indexName).getRootDir();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public GlobalStateInfo getStateInfo() {
    return immutableState.globalStateInfo;
  }

  @Override
  public void replicationStarted(int replicationPort) throws IOException {
    this.resolvedReplicationPort = replicationPort;
    if (getConfiguration().getIndexStartConfig().getAutoStart()) {
      // with leader election every node boots as a standby; ignored without leader election
      updateStartedIndices(immutableState, LeaderStartRole.STANDBY);
    }
    if (leaderManager != null) {
      leaderManager.start();
    }
  }

  /** Config used by standby followers to poll remote storage for new index versions. */
  private IsolatedReplicaConfig getStandbyFollowerConfig() {
    IsolatedReplicaConfig serverConfig = getConfiguration().getIsolatedReplicaConfig();
    int pollingIntervalSeconds =
        serverConfig.isEnabled() ? serverConfig.getPollingIntervalSeconds() : 10;
    return new IsolatedReplicaConfig(true, pollingIntervalSeconds, 0, 0);
  }

  /**
   * Restart an index with a fresh {@link IndexStateManager}, so that it can be started in a
   * different role; a closed index state cannot be started again. Called only by role changes,
   * which the leader manager runs one at a time (restarting different indices in parallel on a
   * worker pool), so no refresh or other role change replaces this index concurrently. The global
   * state lock is only held to read and swap the manager.
   */
  private void restartIndex(String indexName, LeaderStartRole role) throws IOException {
    IndexStateManager current;
    IndexGlobalState indexGlobalState;
    synchronized (this) {
      current = getIndexStateManagerOrThrow(indexName);
      indexGlobalState = immutableState.globalStateInfo.getIndicesMap().get(indexName);
    }
    if (indexGlobalState == null) {
      throw new IllegalArgumentException("Index not in global state: " + indexName);
    }
    // load the replacement before touching the current manager; if this fails the current manager
    // is left as is
    IndexStateManager fresh =
        createIndexStateManager(
            indexName,
            indexGlobalState.getId(),
            getConfiguration().getLiveSettingsOverride(indexName),
            stateBackend);
    fresh.load();
    // swap in the replacement before closing the current manager, so that if the start below
    // fails the map holds an unstarted manager that a later reload or role change can start
    synchronized (this) {
      IndexGlobalState latest = immutableState.globalStateInfo.getIndicesMap().get(indexName);
      if (immutableState.indexStateManagerMap.get(indexName) != current
          || latest == null
          || !latest.getId().equals(indexGlobalState.getId())) {
        // not expected while transitions are serialized; do not overwrite newer state
        fresh.close();
        throw new IllegalStateException("Index changed during restart: " + indexName);
      }
      Map<String, IndexStateManager> managers = new HashMap<>(immutableState.indexStateManagerMap);
      managers.put(indexName, fresh);
      immutableState = new ImmutableState(immutableState.globalStateInfo, managers);
    }
    current.close();
    startIndexFromConfig(indexName, fresh, indexGlobalState, role);
  }

  /** {@link LeaderManager.IndexRoleController} operating on this global state. */
  private class RoleController implements LeaderManager.IndexRoleController {
    @Override
    public void claimAndReloadGlobalState() throws IOException {
      getRemoteBackend()
          .claimResource(
              getConfiguration().getServiceName(), RemoteBackend.GlobalResourceType.GLOBAL_STATE);
      // reload without starting indices, roles are switched by the leader manager
      List<IndexStateManager> replaced = new ArrayList<>();
      synchronized (BackendGlobalState.this) {
        immutableState = buildReloadedState(replaced);
      }
      closeReplaced(replaced);
    }

    @Override
    public Set<String> getIndicesToStart() {
      return BackendGlobalState.this.getIndicesToStart();
    }

    @Override
    public void claimIndex(String indexName) throws IOException {
      String uniqueName = getDataResourceForIndex(indexName);
      String service = getConfiguration().getServiceName();
      getRemoteBackend()
          .claimResource(service, uniqueName, RemoteBackend.IndexResourceType.INDEX_STATE);
      getRemoteBackend()
          .claimResource(service, uniqueName, RemoteBackend.IndexResourceType.POINT_STATE);
      // claim every existing pointer before any index restarts, so an older leader's later writes
      // to any of them fail; pointers this node writes without a claim are claimed on first write
      getRemoteBackend()
          .claimResource(service, uniqueName, RemoteBackend.IndexResourceType.WARMING_QUERIES);
    }

    @Override
    public void restartAsPrimary(String indexName, long primaryGen) throws IOException {
      restartIndex(indexName, LeaderStartRole.PRIMARY);
      ShardState shard = getIndexOrThrow(indexName).getShard(0);
      if (!shard.isPrimary() || shard.nrtPrimaryNode.getPrimaryGen() != primaryGen) {
        throw new IllegalStateException(
            "Index did not start as primary with primaryGen " + primaryGen + ": " + indexName);
      }
    }

    @Override
    public void restartAsStandby(String indexName) throws IOException {
      restartIndex(indexName, LeaderStartRole.STANDBY);
    }

    @Override
    public void refreshStandby(java.util.function.BooleanSupplier abort) throws IOException {
      // load remote state without holding the global state lock, checking for a pending role
      // change between index loads; only the swap is done under the lock
      ImmutableState current = immutableState;
      GlobalStateInfo newGlobalStateInfo = loadGlobalState();
      Map<String, IndexStateManager> newManagerMap = new HashMap<>();
      List<IndexStateManager> created = new ArrayList<>();
      try {
        for (Map.Entry<String, IndexGlobalState> entry :
            newGlobalStateInfo.getIndicesMap().entrySet()) {
          if (abort.getAsBoolean()) {
            logger.info("Refresh stopped early for a pending role change");
            closeReplaced(created);
            return;
          }
          String indexName = entry.getKey();
          IndexStateManager manager = current.indexStateManagerMap.get(indexName);
          if (manager == null || !entry.getValue().getId().equals(manager.getIndexId())) {
            manager =
                createIndexStateManager(
                    indexName,
                    entry.getValue().getId(),
                    getConfiguration().getLiveSettingsOverride(indexName),
                    stateBackend);
            created.add(manager);
          }
          // a standby follows field and settings changes by reloading index state
          manager.load();
          newManagerMap.put(indexName, manager);
        }
      } catch (IOException | RuntimeException e) {
        closeReplaced(created);
        throw e;
      }
      List<IndexStateManager> replaced = new ArrayList<>();
      synchronized (BackendGlobalState.this) {
        if (immutableState != current) {
          // a role change replaced managers while loading; drop this refresh, the next one retries
          closeReplaced(created);
          return;
        }
        for (Map.Entry<String, IndexStateManager> entry : current.indexStateManagerMap.entrySet()) {
          if (newManagerMap.get(entry.getKey()) != entry.getValue()) {
            replaced.add(entry.getValue());
          }
        }
        immutableState = new ImmutableState(newGlobalStateInfo, newManagerMap);
      }
      closeReplaced(replaced);
      if (!getConfiguration().getIndexStartConfig().getAutoStart()) {
        return;
      }
      for (Map.Entry<String, IndexGlobalState> entry :
          newGlobalStateInfo.getIndicesMap().entrySet()) {
        IndexStateManager manager = newManagerMap.get(entry.getKey());
        if (!entry.getValue().getStarted() || manager.getCurrent().isStarted()) {
          continue;
        }
        if (abort.getAsBoolean()) {
          logger.info("Refresh stopped early for a pending role change");
          return;
        }
        startIndexFromConfig(entry.getKey(), manager, entry.getValue(), LeaderStartRole.STANDBY);
      }
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
      stateManager =
          createIndexStateManager(
              indexName,
              indexId,
              getConfiguration().getLiveSettingsOverride(indexName),
              stateBackend);
      stateManager.create();
    } else {
      indexId = createIndexRequest.getExistsWithId();
      stateManager =
          createIndexStateManager(
              indexName,
              indexId,
              getConfiguration().getLiveSettingsOverride(indexName),
              stateBackend);
      stateManager.load();
    }

    if (createIndexRequest.hasSettings()) {
      stateManager.updateSettings(createIndexRequest.getSettings());
    }
    if (createIndexRequest.hasLiveSettings()) {
      stateManager.updateLiveSettings(createIndexRequest.getLiveSettings(), false);
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
        immutableState.globalStateInfo.toBuilder()
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
  public IndexState getIndex(String name) throws IOException {
    IndexStateManager indexStateManager = getIndexStateManager(name);
    return indexStateManager != null ? indexStateManager.getCurrent() : null;
  }

  @Override
  public IndexStateManager getIndexStateManager(String name) throws IOException {
    return immutableState.indexStateManagerMap.get(name);
  }

  @Override
  public synchronized void deleteIndex(String name) throws IOException {
    GlobalStateInfo updatedState =
        immutableState.globalStateInfo.toBuilder()
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
    if (leaderManager != null) {
      // with leader election, the node role decides how indices start: standby followers are
      // started by reload, and primaries only by promotion or by the active primary from config
      if (startIndexRequest.getMode() != Mode.PRIMARY
          || startIndexRequest.hasRestore()
          || startIndexRequest.getPrimaryGen() != 0
          || !startIndexRequest.getPrimaryAddress().isEmpty()
          || !startIndexRequest.getPrimaryDiscoveryFile().isEmpty()
          || startIndexRequest.getPort() != 0) {
        // these would otherwise be ignored: the leader decides restore and primaryGen
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "With leader election, startIndex only accepts indexName and mode=PRIMARY;"
                    + " restore, primaryGen and primary discovery are managed by the leader."
                    + " Use startIndexV2")
            .asRuntimeException();
      }
      Handler.verifyActivePrimary(this, "startIndex");
      return startIndexV2(
          StartIndexV2Request.newBuilder().setIndexName(startIndexRequest.getIndexName()).build());
    }
    IndexStateManager indexStateManager =
        getIndexStateManagerOrThrow(startIndexRequest.getIndexName());
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
          startIndexRequest.toBuilder()
              .setRestore(
                  startIndexRequest.getRestore().toBuilder()
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
          immutableState.globalStateInfo.toBuilder()
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
    // starting an index records it as started in global state, which only the leader may write
    Handler.verifyActivePrimary(this, "startIndexV2");
    IndexStateManager stateManager = getIndexStateManagerOrThrow(startIndexRequest.getIndexName());
    IndexGlobalState indexGlobalState =
        immutableState.globalStateInfo.getIndicesOrThrow(startIndexRequest.getIndexName());
    IndexGlobalState updatedIndexGlobalState =
        indexGlobalState.toBuilder().setStarted(true).build();
    StartIndexResponse response =
        startIndexFromConfig(
            startIndexRequest.getIndexName(), stateManager, updatedIndexGlobalState);

    if (getConfiguration().getIndexStartConfig().getMode() != Mode.REPLICA) {
      GlobalStateInfo updatedGlobalState =
          immutableState.globalStateInfo.toBuilder()
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
    return startIndex(indexStateManager, startIndexRequest, NrtDataManager.StartOptions.DEFAULT);
  }

  private StartIndexResponse startIndex(
      IndexStateManager indexStateManager,
      StartIndexRequest startIndexRequest,
      NrtDataManager.StartOptions startOptions)
      throws IOException {
    StartIndexProcessor startIndexHandler =
        new StartIndexProcessor(
            getConfiguration().getServiceName(),
            getEphemeralId(),
            getRemoteBackend(),
            indexStateManager,
            getConfiguration()
                .getIndexStartConfig()
                .getDataLocationType()
                .equals(IndexDataLocationType.REMOTE),
            getConfiguration().getS3RefreshUpload(),
            getConfiguration().getS3RefreshNotifyAfterUpload(),
            getConfiguration().getDiscoveryFileUpdateIntervalMs(),
            getConfiguration().getRequireIdField());
    try {
      return startIndexHandler.process(
          indexStateManager.getCurrent(), startIndexRequest, startOptions);
    } catch (StartIndexProcessorException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized DummyResponse stopIndex(StopIndexRequest stopIndexRequest)
      throws IOException {
    Handler.verifyActivePrimary(this, "stopIndex");
    IndexStateManager indexStateManager =
        getIndexStateManagerOrThrow(stopIndexRequest.getIndexName());
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
          immutableState.globalStateInfo.toBuilder()
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
    // release leadership first, so another primary can take over without waiting for the lease
    // ttl, and so no role transitions run while indices close
    releaseLeadership();
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
  private void updateStartedIndices(ImmutableState newState, LeaderStartRole role)
      throws IOException {
    for (Map.Entry<String, IndexGlobalState> entry :
        newState.globalStateInfo.getIndicesMap().entrySet()) {
      IndexStateManager indexStateManager = newState.indexStateManagerMap.get(entry.getKey());
      if (entry.getValue().getStarted() && !indexStateManager.getCurrent().isStarted()) {
        startIndexFromConfig(entry.getKey(), indexStateManager, entry.getValue(), role);
      }
    }
  }

  /** How an index is started when leader election is enabled. Ignored without leader election. */
  private enum LeaderStartRole {
    /** Started as primary by the leader, after its pointers are claimed. */
    PRIMARY,
    /** Started as a standby follower of remote data. */
    STANDBY,
    /** Decided by the current role; only for client requests, gated on being the active primary. */
    FROM_ROLE
  }

  /**
   * Start an index based on the {@link IndexStartConfig}, with the role decided by the current
   * leader role.
   *
   * @see #startIndexFromConfig(String, IndexStateManager, IndexGlobalState, LeaderStartRole)
   */
  private StartIndexResponse startIndexFromConfig(
      String indexName, IndexStateManager indexStateManager, IndexGlobalState indexGlobalState)
      throws IOException {
    return startIndexFromConfig(
        indexName, indexStateManager, indexGlobalState, LeaderStartRole.FROM_ROLE);
  }

  /**
   * Start an index based on the {@link IndexStartConfig}.
   *
   * @param indexName index name
   * @param indexStateManager index state manager
   * @param indexGlobalState index global state
   * @param leaderStartRole how to start the index when leader election is enabled
   * @return start response
   * @throws IOException on error starting the index
   */
  private StartIndexResponse startIndexFromConfig(
      String indexName,
      IndexStateManager indexStateManager,
      IndexGlobalState indexGlobalState,
      LeaderStartRole leaderStartRole)
      throws IOException {
    IndexStartConfig indexStartConfig = getConfiguration().getIndexStartConfig();
    Mode mode = indexStartConfig.getMode();
    long primaryGen = -1;
    boolean standby = false;
    if (leaderManager != null) {
      boolean asPrimary =
          switch (leaderStartRole) {
            case PRIMARY -> true;
            case STANDBY -> false;
            case FROM_ROLE -> leaderManager.isActive();
          };
      // only the active primary starts indices as primary, all others follow remote data
      if (asPrimary) {
        primaryGen = leaderManager.getFence().getEpoch();
        if (primaryGen <= 0) {
          throw new FencedException("Cannot start index as primary, fence is not active");
        }
      } else {
        mode = Mode.REPLICA;
        standby = true;
      }
    }
    StartIndexRequest.Builder requestBuilder =
        StartIndexRequest.newBuilder()
            .setIndexName(indexName)
            .setPrimaryGen(primaryGen)
            .setMode(mode);

    // set primary discovery config
    NrtDataManager.StartOptions startOptions;
    if (leaderManager == null) {
      startOptions = NrtDataManager.StartOptions.DEFAULT;
    } else if (standby) {
      // standby followers poll remote storage, and do not connect to the active primary
      startOptions = new NrtDataManager.StartOptions(false, getStandbyFollowerConfig());
    } else {
      // promoting a standby that was following remote data, only download changed files
      startOptions = new NrtDataManager.StartOptions(true, null);
    }
    if (!standby && mode.equals(Mode.REPLICA)) {
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
    StartIndexResponse response =
        startIndex(indexStateManager, requestBuilder.build(), startOptions);
    logger.info("Index started: " + response);
    return response;
  }
}
