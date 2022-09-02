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
package com.yelp.nrtsearch.server.plugins;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import io.prometheus.client.CollectorRegistry;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PluginsServiceTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private LuceneServerConfiguration getEmptyConfig() {
    String config = "nodeName: \"lucene_server_foo\"";
    return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
  }

  private LuceneServerConfiguration getConfigWithSearchPath(String path) {
    String config = "pluginSearchPath: \"" + path + "\"";
    return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
  }

  private CollectorRegistry getCollectorRegistry() {
    return new CollectorRegistry();
  }

  @Test
  public void testGetSinglePluginSearchPath() {
    LuceneServerConfiguration config = getConfigWithSearchPath("some/plugin/path");
    PluginsService pluginsService = new PluginsService(config, getCollectorRegistry());
    List<File> expectedPaths = new ArrayList<>();
    expectedPaths.add(new File("some/plugin/path"));
    assertEquals(expectedPaths, pluginsService.getPluginSearchPath());
  }

  @Test
  public void testGetMultiPluginSearchPath() {
    String searchPath =
        "some1/plugin1/path1"
            + File.pathSeparator
            + "some2/plugin2/path2"
            + File.pathSeparator
            + "some3/plugin3/path3"
            + File.pathSeparator;
    LuceneServerConfiguration config = getConfigWithSearchPath(searchPath);
    PluginsService pluginsService = new PluginsService(config, getCollectorRegistry());
    List<File> expectedPaths = new ArrayList<>();
    expectedPaths.add(new File("some1/plugin1/path1"));
    expectedPaths.add(new File("some2/plugin2/path2"));
    expectedPaths.add(new File("some3/plugin3/path3"));
    assertEquals(expectedPaths, pluginsService.getPluginSearchPath());
  }

  @Test
  public void testFindPluginInstallDir() throws IOException {
    File plugin1 = folder.newFolder("plugin1");
    File plugin2 = folder.newFolder("plugin2");
    File plugin3 = folder.newFolder("plugin3");
    PluginsService pluginsService = new PluginsService(getEmptyConfig(), getCollectorRegistry());
    assertEquals(
        plugin1,
        pluginsService.findPluginInstallDir(
            "plugin1", Collections.singletonList(folder.getRoot())));
    assertEquals(
        plugin2,
        pluginsService.findPluginInstallDir(
            "plugin2", Collections.singletonList(folder.getRoot())));
    assertEquals(
        plugin3,
        pluginsService.findPluginInstallDir(
            "plugin3", Collections.singletonList(folder.getRoot())));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFindPluginInstallDirNotFound() throws IOException {
    folder.newFolder("plugin1");
    PluginsService pluginsService = new PluginsService(getEmptyConfig(), getCollectorRegistry());
    pluginsService.findPluginInstallDir("invalid", Collections.singletonList(folder.getRoot()));
  }

  @Test
  public void testFindFirstPluginInstallDir() throws IOException {
    List<File> searchPath = new ArrayList<>();
    searchPath.add(folder.newFolder("dir1"));
    searchPath.add(folder.newFolder("dir2"));
    searchPath.add(folder.newFolder("dir3"));
    folder.newFolder("dir1", "plugin1");
    folder.newFolder("dir1", "plugin2");
    File installDir = folder.newFolder("dir2", "plugin3");
    folder.newFolder("dir3", "plugin3");
    folder.newFolder("dir3", "plugin4");
    PluginsService pluginsService = new PluginsService(getEmptyConfig(), getCollectorRegistry());
    assertEquals(installDir, pluginsService.findPluginInstallDir("plugin3", searchPath));
  }

  @Test
  public void testGetPluginJars() throws IOException {
    List<File> jars = new ArrayList<>();
    jars.add(folder.newFile("some1.jar"));
    jars.add(folder.newFile("some2.jar"));
    jars.add(folder.newFile("some3.jar"));
    folder.newFile("not_jar");
    folder.newFile("some_file.txt");
    folder.newFolder("some_folder.jar");
    PluginsService pluginsService = new PluginsService(getEmptyConfig(), getCollectorRegistry());
    assertEquals(
        new HashSet<>(jars), new HashSet<>(pluginsService.getPluginJars(folder.getRoot())));
  }

  public static class LoadTestPlugin extends Plugin {
    public LuceneServerConfiguration config;
    public CollectorRegistry collectorRegistry;

    public LoadTestPlugin(LuceneServerConfiguration config) {
      this.config = config;
    }

    /**
     * This method shouldn't be called as this class doesn't implement MetricsPlugin
     *
     * @param collectorRegistry Prometheus collector registry.
     */
    public void registerMetrics(CollectorRegistry collectorRegistry) {
      this.collectorRegistry = collectorRegistry;
    }
  }

  public static class LoadTestPluginWithMetrics extends Plugin implements MetricsPlugin {
    public LuceneServerConfiguration config;
    public CollectorRegistry collectorRegistry;

    public LoadTestPluginWithMetrics(LuceneServerConfiguration config) {
      this.config = config;
    }

    @Override
    public void registerMetrics(CollectorRegistry collectorRegistry) {
      this.collectorRegistry = collectorRegistry;
    }
  }

  @Test
  public void testGetPluginClass() {
    PluginsService pluginsService = new PluginsService(getEmptyConfig(), getCollectorRegistry());
    Class<? extends Plugin> clazz =
        pluginsService.getPluginClass(
            Collections.emptyList(),
            "com.yelp.nrtsearch.server.plugins.PluginsServiceTest$LoadTestPlugin");
    assertEquals(LoadTestPlugin.class, clazz);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPluginClassNotFound() {
    PluginsService pluginsService = new PluginsService(getEmptyConfig(), getCollectorRegistry());
    pluginsService.getPluginClass(
        Collections.emptyList(), "com.yelp.nrtsearch.server.plugins.NotClass");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPluginClassNotPlugin() {
    PluginsService pluginsService = new PluginsService(getEmptyConfig(), getCollectorRegistry());
    pluginsService.getPluginClass(
        Collections.emptyList(), "com.yelp.nrtsearch.server.plugins.PluginServiceTest");
  }

  @Test
  public void testGetPluginInstance() {
    CollectorRegistry collectorRegistry = getCollectorRegistry();
    PluginsService pluginsService = new PluginsService(getEmptyConfig(), collectorRegistry);
    Plugin loadedPlugin = pluginsService.getPluginInstance(LoadTestPlugin.class);
    assertEquals(null, ((LoadTestPlugin) loadedPlugin).collectorRegistry);
  }

  @Test
  public void testGetPluginInstanceWithMetrics() {
    CollectorRegistry collectorRegistry = getCollectorRegistry();
    PluginsService pluginsService = new PluginsService(getEmptyConfig(), collectorRegistry);
    Plugin loadedPlugin = pluginsService.getPluginInstance(LoadTestPluginWithMetrics.class);
    assertEquals(collectorRegistry, ((LoadTestPluginWithMetrics) loadedPlugin).collectorRegistry);
  }

  @Test
  public void testGetPluginInstanceHasConfig() {
    LuceneServerConfiguration config = getEmptyConfig();
    PluginsService pluginsService = new PluginsService(config, getCollectorRegistry());
    Plugin loadedPlugin = pluginsService.getPluginInstance(LoadTestPlugin.class);
    LoadTestPlugin loadTestPlugin = (LoadTestPlugin) loadedPlugin;
    assertSame(config, loadTestPlugin.config);
  }
}
