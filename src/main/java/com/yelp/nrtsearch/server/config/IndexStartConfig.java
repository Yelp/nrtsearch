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
package com.yelp.nrtsearch.server.config;

import com.yelp.nrtsearch.server.grpc.Mode;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Configuration class for starting indices from global state. */
public class IndexStartConfig {
  public static final String CONFIG_PREFIX = "indexStartConfig.";
  public static final String PRIMARY_DISCOVERY_PREFIX = "primaryDiscovery.";

  /** How a replica finds the primary to replicate from. */
  public enum PrimaryResolverType {
    /** The primary in the discovery file, or the static host/port. */
    FILE,
    /**
     * With multi-primary leader election: ask the nodes in the discovery file (or the static
     * host/port) for the active primary with resolveLeader, and follow the confirmed leader.
     */
    LEADER_RPC
  }

  public static final int DEFAULT_LEADER_RESOLVE_POLL_MS = 2000;

  public enum IndexDataLocationType {
    LOCAL,
    REMOTE
  }

  private final boolean autoStart;
  private final Mode mode;
  private final String discoveryHost;
  private final Integer discoveryPort;
  private final String discoveryFile;
  private final IndexDataLocationType dataLocationType;
  private final PrimaryResolverType primaryResolver;
  private final int leaderResolvePollMs;

  /**
   * Create instance from provided configuration reader.
   *
   * @param configReader config reader
   * @return class instance
   */
  public static IndexStartConfig fromConfig(YamlConfigReader configReader) {
    Objects.requireNonNull(configReader);
    boolean autoStart = configReader.getBoolean(CONFIG_PREFIX + "autoStart", true);
    Mode mode = Mode.valueOf(configReader.getString(CONFIG_PREFIX + "mode", "STANDALONE"));
    String discoveryHost =
        configReader.getString(CONFIG_PREFIX + PRIMARY_DISCOVERY_PREFIX + "host", "");
    Integer discoveryPort =
        configReader.getInteger(CONFIG_PREFIX + PRIMARY_DISCOVERY_PREFIX + "port", 0);
    String discoveryFile =
        configReader.getString(CONFIG_PREFIX + PRIMARY_DISCOVERY_PREFIX + "file", "");
    PrimaryResolverType primaryResolver =
        PrimaryResolverType.valueOf(
            configReader.getString(CONFIG_PREFIX + PRIMARY_DISCOVERY_PREFIX + "resolver", "FILE"));
    int leaderResolvePollMs =
        configReader.getInteger(
            CONFIG_PREFIX + PRIMARY_DISCOVERY_PREFIX + "leaderResolvePollMs",
            DEFAULT_LEADER_RESOLVE_POLL_MS);
    boolean allowSingleLeaderSeed =
        configReader.getBoolean(
            CONFIG_PREFIX + PRIMARY_DISCOVERY_PREFIX + "allowSingleLeaderSeed", false);
    if (primaryResolver == PrimaryResolverType.LEADER_RPC
        && discoveryFile.isEmpty()
        && splitHosts(discoveryHost).size() < 2
        && !allowSingleLeaderSeed) {
      // if that single host is the primary that fails, the replica can never find the new leader
      throw new IllegalArgumentException(
          "primaryDiscovery.resolver LEADER_RPC needs a discovery file or a comma separated list of"
              + " all primary hosts; a single host can not find a new leader if it fails. Set"
              + " primaryDiscovery.allowSingleLeaderSeed: true if the host is a load balanced name"
              + " for all primaries");
    }
    IndexDataLocationType dataLocationType =
        IndexDataLocationType.valueOf(
            configReader.getString(CONFIG_PREFIX + "dataLocationType", "LOCAL"));
    return new IndexStartConfig(
        autoStart,
        mode,
        discoveryHost,
        discoveryPort,
        discoveryFile,
        dataLocationType,
        primaryResolver,
        leaderResolvePollMs);
  }

  /**
   * Constructor.
   *
   * @param autoStart if indices should be automatically started
   * @param mode mode to start indices in
   * @param discoveryHost primary host address
   * @param discoveryPort primary host replication port
   * @param discoveryFile file containing primary host/port
   * @param dataLocationType where index data is present
   */
  public IndexStartConfig(
      boolean autoStart,
      Mode mode,
      String discoveryHost,
      Integer discoveryPort,
      String discoveryFile,
      IndexDataLocationType dataLocationType) {
    this(
        autoStart,
        mode,
        discoveryHost,
        discoveryPort,
        discoveryFile,
        dataLocationType,
        PrimaryResolverType.FILE,
        DEFAULT_LEADER_RESOLVE_POLL_MS);
  }

  /**
   * Constructor.
   *
   * @param primaryResolver how replicas find the primary
   * @param leaderResolvePollMs how often LEADER_RPC replicas re-resolve the leader
   */
  public IndexStartConfig(
      boolean autoStart,
      Mode mode,
      String discoveryHost,
      Integer discoveryPort,
      String discoveryFile,
      IndexDataLocationType dataLocationType,
      PrimaryResolverType primaryResolver,
      int leaderResolvePollMs) {
    if (leaderResolvePollMs <= 0) {
      throw new IllegalArgumentException("leaderResolvePollMs must be > 0");
    }
    this.primaryResolver = primaryResolver;
    this.leaderResolvePollMs = leaderResolvePollMs;
    this.autoStart = autoStart;
    this.mode = mode;
    this.discoveryHost = discoveryHost;
    this.discoveryPort = discoveryPort;
    this.discoveryFile = discoveryFile;
    this.dataLocationType = dataLocationType;
    // this limitation exists because we do not handle backup/restore of the taxonomy index
    // properly, which is only used in STANDALONE mode
    if (Mode.STANDALONE.equals(mode) && IndexDataLocationType.REMOTE.equals(dataLocationType)) {
      throw new IllegalArgumentException("Cannot use REMOTE data location in STANDALONE mode");
    }
  }

  /** Get if indices should be started on server startup. */
  public boolean getAutoStart() {
    return autoStart;
  }

  /** Get mode to start indices in. */
  public Mode getMode() {
    return mode;
  }

  /** Get primary host address, or empty string if using discovery file. */
  public String getDiscoveryHost() {
    return discoveryHost;
  }

  /** Get primary replication port. */
  public Integer getDiscoveryPort() {
    return discoveryPort;
  }

  /** Split a comma separated host list, trimming and dropping empty entries. */
  public static List<String> splitHosts(String hosts) {
    return Arrays.stream(hosts.split(","))
        .map(String::trim)
        .filter(h -> !h.isEmpty())
        .collect(Collectors.toList());
  }

  /** How replicas find the primary to replicate from. */
  public PrimaryResolverType getPrimaryResolver() {
    return primaryResolver;
  }

  /** How often LEADER_RPC replicas re-resolve the leader, in ms. */
  public int getLeaderResolvePollMs() {
    return leaderResolvePollMs;
  }

  /** Get path to discovery file containing primary host/port. */
  public String getDiscoveryFile() {
    return discoveryFile;
  }

  /** Get the location of index data. */
  public IndexDataLocationType getDataLocationType() {
    return dataLocationType;
  }
}
