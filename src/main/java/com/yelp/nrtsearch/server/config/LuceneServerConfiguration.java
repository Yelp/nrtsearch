/*
 * Copyright 2020 Yelp Inc.
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

import com.google.inject.Inject;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

public class LuceneServerConfiguration {
  public static final Path DEFAULT_USER_DIR =
      Paths.get(System.getProperty("user.home"), "lucene", "server");
  public static final Path DEFAULT_ARCHIVER_DIR =
      Paths.get(DEFAULT_USER_DIR.toString(), "archiver");
  public static final Path DEFAULT_BOTO_CFG_PATH =
      Paths.get(DEFAULT_USER_DIR.toString(), "boto.cfg");
  public static final Path DEFAULT_STATE_DIR =
      Paths.get(DEFAULT_USER_DIR.toString(), "default_state");
  public static final Path DEFAULT_INDEX_DIR =
      Paths.get(DEFAULT_USER_DIR.toString(), "default_index");
  private static final String DEFAULT_BUCKET_NAME = "DEFAULT_ARCHIVE_BUCKET";
  private static final String DEFAULT_HOSTNAME = "localhost";
  private static final int DEFAULT_PORT = 50051;
  private static final int DEFAULT_REPLICATION_PORT = 50052;
  private static final String DEFAULT_NODE_NAME = "main";
  // buckets represent number of requests completed in "less than" seconds
  private static final double[] DEFAULT_METRICS_BUCKETS =
      new double[] {.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10};
  private static final int DEFAULT_INTERVAL_MS = 1000 * 10;
  private static final List<String> DEFAULT_PLUGINS = Collections.emptyList();
  private static final Path DEFAULT_PLUGIN_SEARCH_PATH =
      Paths.get(DEFAULT_USER_DIR.toString(), "plugins");
  private static final String DEFAULT_SERVICE_NAME = "nrtsearch-generic";

  private final int port;
  private final int replicationPort;
  private final int replicaReplicationPortPingInterval;
  private final String nodeName;
  private final String hostName;
  private final String stateDir;
  private final String indexDir;
  private final String archiveDirectory;
  private final String botoCfgPath;
  private final String bucketName;
  private final double[] metricsBuckets;
  private final String[] plugins;
  private final String pluginSearchPath;
  private final String serviceName;
  private final boolean restoreState;
  private final ThreadPoolConfiguration threadPoolConfiguration;

  private final YamlConfigReader configReader;

  @Inject
  public LuceneServerConfiguration(InputStream yamlStream) {
    configReader = new YamlConfigReader(yamlStream);

    port = configReader.getInteger("port", DEFAULT_PORT);
    replicationPort = configReader.getInteger("replicationPort", DEFAULT_REPLICATION_PORT);
    replicaReplicationPortPingInterval =
        configReader.getInteger("replicaReplicationPortPingInterval", DEFAULT_INTERVAL_MS);
    nodeName = configReader.getString("nodeName", DEFAULT_NODE_NAME);
    hostName = configReader.getString("hostName", DEFAULT_HOSTNAME);
    stateDir = configReader.getString("stateDir", DEFAULT_STATE_DIR.toString());
    indexDir = configReader.getString("indexDir", DEFAULT_INDEX_DIR.toString());
    archiveDirectory = configReader.getString("archiveDirectory", DEFAULT_ARCHIVER_DIR.toString());
    botoCfgPath = configReader.getString("botoCfgPath", DEFAULT_BOTO_CFG_PATH.toString());
    bucketName = configReader.getString("bucketName", DEFAULT_BUCKET_NAME);
    double[] metricsBuckets;
    try {
      List<Double> bucketList = configReader.getDoubleList("metricsBuckets");
      metricsBuckets = new double[bucketList.size()];
      for (int i = 0; i < bucketList.size(); ++i) {
        metricsBuckets[i] = bucketList.get(i);
      }
    } catch (ConfigKeyNotFoundException e) {
      metricsBuckets = DEFAULT_METRICS_BUCKETS;
    }
    this.metricsBuckets = metricsBuckets;
    plugins = configReader.getStringList("plugins", DEFAULT_PLUGINS).toArray(new String[0]);
    pluginSearchPath =
        configReader.getString("pluginSearchPath", DEFAULT_PLUGIN_SEARCH_PATH.toString());
    serviceName = configReader.getString("serviceName", DEFAULT_SERVICE_NAME);
    restoreState = configReader.getBoolean("restoreState", false);
    threadPoolConfiguration = new ThreadPoolConfiguration(configReader);
  }

  public ThreadPoolConfiguration getThreadPoolConfiguration() {
    return threadPoolConfiguration;
  }

  public int getPort() {
    return port;
  }

  public String getServiceName() {
    return serviceName;
  }

  public int getReplicationPort() {
    return replicationPort;
  }

  public String getNodeName() {
    return nodeName;
  }

  public String getStateDir() {
    return stateDir;
  }

  public String getIndexDir() {
    return indexDir;
  }

  public String getHostName() {
    return hostName;
  }

  public String getBotoCfgPath() {
    return botoCfgPath;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getArchiveDirectory() {
    return archiveDirectory;
  }

  public double[] getMetricsBuckets() {
    return metricsBuckets;
  }

  public int getReplicaReplicationPortPingInterval() {
    return replicaReplicationPortPingInterval;
  }

  public String[] getPlugins() {
    return this.plugins;
  }

  public String getPluginSearchPath() {
    return this.pluginSearchPath;
  }

  public boolean getRestoreState() {
    return restoreState;
  }

  public YamlConfigReader getConfigReader() {
    return configReader;
  }
}
