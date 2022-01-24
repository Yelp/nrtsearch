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
import com.yelp.nrtsearch.server.luceneserver.warming.WarmerConfig;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LuceneServerConfiguration {
  private static final Pattern ENV_VAR_PATTERN = Pattern.compile("\\$\\{([A-Za-z0-9_]+)}");

  private static final long AS_LARGE_AS_INFINITE = TimeUnit.DAYS.toSeconds(1000L);
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
  private static final int DEFAULT_MAX_CONCURRENT_CALLS_REPLICATION = -1;
  private static final List<String> DEFAULT_PLUGINS = Collections.emptyList();
  private static final Path DEFAULT_PLUGIN_SEARCH_PATH =
      Paths.get(DEFAULT_USER_DIR.toString(), "plugins");
  private static final String DEFAULT_SERVICE_NAME = "nrtsearch-generic";
  private final int port;
  private final int replicationPort;
  private final int replicaReplicationPortPingInterval;
  private final int maxConcurrentCallsPerConnectionForReplication;
  private final String nodeName;
  private final String hostName;
  private final String stateDir;
  private final String indexDir;
  private final String archiveDirectory;
  private final String botoCfgPath;
  private final String bucketName;
  private final double[] metricsBuckets;
  private final boolean publishJvmMetrics;
  private final String[] plugins;
  private final String pluginSearchPath;
  private final String serviceName;
  private final boolean restoreState;
  private final boolean restoreFromIncArchiver;
  private final boolean backupWithIncArchiver;
  private final ThreadPoolConfiguration threadPoolConfiguration;
  private final IndexPreloadConfig preloadConfig;
  private final QueryCacheConfig queryCacheConfig;
  private final WarmerConfig warmerConfig;
  private final boolean downloadAsStream;
  private final boolean fileSendDelay;
  private final boolean virtualSharding;
  private final boolean syncInitialNrtPoint;
  private final boolean indexVerbose;
  private final FileCopyConfig fileCopyConfig;
  private final ScriptCacheConfig scriptCacheConfig;
  private final boolean deadlineCancellation;

  private final YamlConfigReader configReader;
  private final long maxConnectionAgeForReplication;
  private final long maxConnectionAgeGraceForReplication;

  @Inject
  public LuceneServerConfiguration(InputStream yamlStream) {
    configReader = new YamlConfigReader(yamlStream);

    port = configReader.getInteger("port", DEFAULT_PORT);
    replicationPort = configReader.getInteger("replicationPort", DEFAULT_REPLICATION_PORT);
    replicaReplicationPortPingInterval =
        configReader.getInteger("replicaReplicationPortPingInterval", DEFAULT_INTERVAL_MS);
    maxConcurrentCallsPerConnectionForReplication =
        configReader.getInteger(
            "maxConcurrentCallsPerConnectionForReplication",
            DEFAULT_MAX_CONCURRENT_CALLS_REPLICATION);
    maxConnectionAgeForReplication =
        configReader.getLong("maxConnectionAgeForReplication", AS_LARGE_AS_INFINITE);
    maxConnectionAgeGraceForReplication =
        configReader.getLong("maxConnectionAgeGraceForReplication", AS_LARGE_AS_INFINITE);
    nodeName = configReader.getString("nodeName", DEFAULT_NODE_NAME);
    hostName = substituteEnvVariables(configReader.getString("hostName", DEFAULT_HOSTNAME));
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
    publishJvmMetrics = configReader.getBoolean("publishJvmMetrics", false);
    plugins = configReader.getStringList("plugins", DEFAULT_PLUGINS).toArray(new String[0]);
    pluginSearchPath =
        configReader.getString("pluginSearchPath", DEFAULT_PLUGIN_SEARCH_PATH.toString());
    serviceName = configReader.getString("serviceName", DEFAULT_SERVICE_NAME);
    restoreState = configReader.getBoolean("restoreState", false);
    restoreFromIncArchiver = configReader.getBoolean("restoreFromIncArchiver", false);
    backupWithIncArchiver = configReader.getBoolean("backupWithIncArchiver", false);
    preloadConfig = IndexPreloadConfig.fromConfig(configReader);
    queryCacheConfig = QueryCacheConfig.fromConfig(configReader);
    warmerConfig = WarmerConfig.fromConfig(configReader);
    downloadAsStream = configReader.getBoolean("downloadAsStream", false);
    fileSendDelay = configReader.getBoolean("fileSendDelay", true);
    virtualSharding = configReader.getBoolean("virtualSharding", false);
    syncInitialNrtPoint = configReader.getBoolean("syncInitialNrtPoint", false);
    indexVerbose = configReader.getBoolean("indexVerbose", false);
    fileCopyConfig = FileCopyConfig.fromConfig(configReader);
    threadPoolConfiguration = new ThreadPoolConfiguration(configReader);
    scriptCacheConfig = ScriptCacheConfig.fromConfig(configReader);
    deadlineCancellation = configReader.getBoolean("deadlineCancellation", false);
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

  public int getMaxConcurrentCallsPerConnectionForReplication() {
    return maxConcurrentCallsPerConnectionForReplication;
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

  public boolean getPublishJvmMetrics() {
    return publishJvmMetrics;
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

  public boolean getRestoreFromIncArchiver() {
    return restoreFromIncArchiver;
  }

  public boolean getBackupWithInArchiver() {
    return backupWithIncArchiver;
  }

  public IndexPreloadConfig getPreloadConfig() {
    return preloadConfig;
  }

  public QueryCacheConfig getQueryCacheConfig() {
    return queryCacheConfig;
  }

  public WarmerConfig getWarmerConfig() {
    return warmerConfig;
  }

  public boolean getDownloadAsStream() {
    return downloadAsStream;
  }

  public boolean getFileSendDelay() {
    return fileSendDelay;
  }

  public boolean getVirtualSharding() {
    return virtualSharding;
  }

  public boolean getSyncInitialNrtPoint() {
    return syncInitialNrtPoint;
  }

  public boolean getIndexVerbose() {
    return indexVerbose;
  }

  public FileCopyConfig getFileCopyConfig() {
    return fileCopyConfig;
  }

  public YamlConfigReader getConfigReader() {
    return configReader;
  }

  public ScriptCacheConfig getScriptCacheConfig() {
    return scriptCacheConfig;
  }

  public boolean getDeadlineCancellation() {
    return deadlineCancellation;
  }

  /**
   * Substitute all sub strings of the form ${FOO} with the environment variable value env[FOO].
   * Variable names may only contain letters, numbers, and underscores. If a variable is not present
   * in the environment, it is substituted with an empty string.
   *
   * @param s string to make substitutions
   */
  private String substituteEnvVariables(String s) {
    String result = s;
    Matcher matcher = ENV_VAR_PATTERN.matcher(s);
    Set<String> foundVars = null;
    while (matcher.find()) {
      if (foundVars == null) {
        foundVars = new HashSet<>();
      }
      foundVars.add(matcher.group(1));
    }

    if (foundVars == null) {
      return result;
    }

    for (String envVar : foundVars) {
      String envStr = System.getenv(envVar);
      if (envStr == null) {
        envStr = "";
      }
      result = result.replaceAll("\\$\\{" + envVar + "}", envStr);
    }
    return result;
  }

  public long getMaxConnectionAgeForReplication() {
    return maxConnectionAgeForReplication;
  }

  public long getMaxConnectionAgeGraceForReplication() {
    return maxConnectionAgeGraceForReplication;
  }
}
