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
import java.util.Objects;

/** Configuration class for starting indices from global state. */
public class IndexStartConfig {
  public static final String CONFIG_PREFIX = "indexStartConfig.";
  public static final String PRIMARY_DISCOVERY_PREFIX = "primaryDiscovery.";

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

  /**
   * Create instance from provided configuration reader.
   *
   * @param configReader config reader
   * @return class instance
   */
  public static IndexStartConfig fromConfig(YamlConfigReader configReader) {
    Objects.requireNonNull(configReader);
    boolean autoStart = configReader.getBoolean(CONFIG_PREFIX + "autoStart", false);
    Mode mode = Mode.valueOf(configReader.getString(CONFIG_PREFIX + "mode", "STANDALONE"));
    String discoveryHost =
        configReader.getString(CONFIG_PREFIX + PRIMARY_DISCOVERY_PREFIX + "host", "");
    Integer discoveryPort =
        configReader.getInteger(CONFIG_PREFIX + PRIMARY_DISCOVERY_PREFIX + "port", 0);
    String discoveryFile =
        configReader.getString(CONFIG_PREFIX + PRIMARY_DISCOVERY_PREFIX + "file", "");
    IndexDataLocationType dataLocationType =
        IndexDataLocationType.valueOf(
            configReader.getString(CONFIG_PREFIX + "dataLocationType", "LOCAL"));
    return new IndexStartConfig(
        autoStart, mode, discoveryHost, discoveryPort, discoveryFile, dataLocationType);
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

  /** Get path to discovery file containing primary host/port. */
  public String getDiscoveryFile() {
    return discoveryFile;
  }

  /** Get the location of index data. */
  public IndexDataLocationType getDataLocationType() {
    return dataLocationType;
  }
}
