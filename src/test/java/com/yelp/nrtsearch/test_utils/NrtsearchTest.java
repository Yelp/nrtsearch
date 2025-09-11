/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.test_utils;

import static com.yelp.nrtsearch.test_utils.DefaultTestProperties.PORT;
import static com.yelp.nrtsearch.test_utils.DefaultTestProperties.REPLICATION_PORT;
import static com.yelp.nrtsearch.test_utils.DefaultTestProperties.S3_BUCKET_NAME;

import com.amazonaws.services.s3.AmazonS3;
import com.yelp.nrtsearch.module.TestNrtsearchModule;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.NrtsearchClient;
import com.yelp.nrtsearch.server.grpc.NrtsearchServer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/**
 * Base class for tests which need to initialize an Nrtsearch instance. Unlike {@link
 * ServerTestCase} which directly creates a gRPC server using {@link
 * NrtsearchServer.LuceneServerImpl}, this class creates and starts {@link NrtsearchServer} using a
 * custom guice module ({@link TestNrtsearchModule}. This class is useful for tests which require
 * testing the initialization path of {@link NrtsearchServer}.
 */
public class NrtsearchTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @ClassRule
  public static final AmazonS3Provider S3_PROVIDER = new AmazonS3Provider(getS3BucketName());

  private final Path pluginSearchPath = getOrCreatePluginSearchPath();

  @Rule
  public final TestNrtsearchServer testServer =
      new TestNrtsearchServer(getConfig(), S3_PROVIDER.getAmazonS3());

  public NrtsearchTest() throws IOException {}

  private static Path getOrCreatePluginSearchPath() {
    try {
      // Try to create the folder, but handle the case where it already exists
      return TEMPORARY_FOLDER.newFolder("plugin_search_path").toPath();
    } catch (IOException e) {
      if (e.getMessage() != null && e.getMessage().contains("already exists")) {
        // Folder already exists, just return the path to it
        return TEMPORARY_FOLDER.getRoot().toPath().resolve("plugin_search_path");
      }
      throw new RuntimeException("Failed to create plugin search path", e);
    }
  }

  private static Path getOrCreateStateDir() throws IOException {
    try {
      return TEMPORARY_FOLDER.newFolder("tmp_state_dir").toPath();
    } catch (IOException e) {
      if (e.getMessage() != null && e.getMessage().contains("already exists")) {
        return TEMPORARY_FOLDER.getRoot().toPath().resolve("tmp_state_dir");
      }
      throw e;
    }
  }

  private static Path getOrCreateIndexDir() throws IOException {
    try {
      return TEMPORARY_FOLDER.newFolder("tmp_index_dir").toPath();
    } catch (IOException e) {
      if (e.getMessage() != null && e.getMessage().contains("already exists")) {
        return TEMPORARY_FOLDER.getRoot().toPath().resolve("tmp_index_dir");
      }
      throw e;
    }
  }

  /**
   * Override this method to provide the plugins to install on server.
   *
   * @return List of plugins - either names of directories in the {@link #getPluginSearchPath()} or
   *     s3 paths.
   */
  protected List<String> getPlugins() {
    return List.of();
  }

  /**
   * This method provides the local path where Nrtsearch will search for plugins or store downloaded
   * plugins. This will be automatically created from a {@link TemporaryFolder} so any added
   * contents will be deleted after the test. Either add any plugin files to this path or override
   * the method and provide a path containing plugin files.
   *
   * @return Plugin search path for Nrtsearch
   */
  protected Path getPluginSearchPath() {
    return pluginSearchPath;
  }

  /**
   * Override this method to add any additional configuration which will be used to build {@link
   * NrtsearchConfig} for the test Nrtsearch instance. When overriding this method remember to call
   * {@code super.addNrtsearchConfigs(config)} so that the default configs get added as well, unless
   * you don't need the defaults.
   *
   * @param config A {@link Map} which will be used to built {@link NrtsearchConfig}
   */
  protected void addNrtsearchConfigs(Map<String, Object> config) {
    config.put("port", PORT);
    config.put("replicationPort", REPLICATION_PORT);
    config.put("bucketName", getS3BucketName());
    config.put("plugins", getPlugins());
    config.put("pluginSearchPath", getPluginSearchPath().toAbsolutePath().toString());
    try {
      config.put("stateDir", getOrCreateStateDir().toString());
      config.put("indexDir", getOrCreateIndexDir().toString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private NrtsearchConfig getConfig() {
    Map<String, Object> config = new HashMap<>();
    addNrtsearchConfigs(config);

    Yaml yaml = new Yaml();
    DumperOptions options = new DumperOptions();
    options.setPrettyFlow(false);

    String yamlConfig = yaml.dump(config);
    return new NrtsearchConfig(new ByteArrayInputStream(yamlConfig.getBytes()));
  }

  /**
   * Get the {@link NrtsearchClient} for the test Nrtsearch instance
   *
   * @return {@link NrtsearchClient}
   */
  protected NrtsearchClient getClient() {
    return testServer.getClient();
  }

  /**
   * Get a mock {@link AmazonS3} client for tests.
   *
   * @return mocked {@link AmazonS3} client
   */
  protected static AmazonS3 getS3Client() {
    return S3_PROVIDER.getAmazonS3();
  }

  /**
   * Get the bucket name in S3 which is used in the test. Override to provide a different name.
   *
   * @return Name of the bucket
   */
  protected static String getS3BucketName() {
    return S3_BUCKET_NAME;
  }
}
