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

import com.amazonaws.services.s3.AmazonS3;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import io.prometheus.client.CollectorRegistry;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to handle the loading and registration of nrtsearch plugins.
 *
 * <p>Loads the plugins specified by the {@link LuceneServerConfiguration}. Plugins are located by
 * searching the config provided plugin search path for a folder matching the plugin name. A
 * classloader is created with the jars provided in the plugin directory. The plugin provides a
 * config file containing the {@link Plugin} classname that should be loaded. Reflection is used to
 * give the loaded plugin access to the lucene server config.
 */
public class PluginsService {
  private static final Logger logger = LoggerFactory.getLogger(PluginsService.class);

  private final LuceneServerConfiguration config;
  private final CollectorRegistry collectorRegistry;
  private final List<PluginDescriptor> loadedPluginDescriptors = new ArrayList<>();

  private final AmazonS3 amazonS3;

  public PluginsService(
      LuceneServerConfiguration config, AmazonS3 amazonS3, CollectorRegistry collectorRegistry) {
    this.config = config;
    this.collectorRegistry = collectorRegistry;
    this.amazonS3 = amazonS3;
  }

  /**
   * Load the list of plugins specified in the {@link LuceneServerConfiguration}. This handles both
   * the loading of the plugin class from provided jars and creation of a new instance.
   *
   * @return list of loaded plugin instances
   */
  public List<Plugin> loadPlugins() {
    logger.info("Loading plugins: " + Arrays.toString(config.getPlugins()));
    List<File> pluginSearchPath = getPluginSearchPath();
    logger.debug("Plugin search path: " + pluginSearchPath);
    List<Plugin> loadedPlugins = new ArrayList<>();
    PluginDownloader pluginDownloader = new PluginDownloader(amazonS3, config);
    for (String plugin : config.getPlugins()) {
      logger.info("Loading plugin: " + plugin);
      PluginDescriptor descriptor = loadPlugin(plugin, pluginSearchPath, pluginDownloader);
      loadedPluginDescriptors.add(descriptor);
      loadedPlugins.add(descriptor.getPlugin());
    }
    pluginDownloader.close();
    return loadedPlugins;
  }

  /**
   * Cleanup function called during server shutdown. Closes all loaded plugins using the {@link
   * java.io.Closeable} interface.
   */
  public void shutdown() {
    loadedPluginDescriptors.forEach(
        plugin -> {
          try {
            plugin.getPlugin().close();
          } catch (Exception e) {
            logger.info("Exception closing plugin: " + plugin.getPluginMetadata().getName(), e);
          }
        });
  }

  /** Get the information for all loaded plugins. */
  public List<PluginDescriptor> getLoadedPluginDescriptors() {
    return loadedPluginDescriptors;
  }

  /**
   * Convert the search path into a list of directory. The search path separator is the same as the
   * OS path separator.
   */
  List<File> getPluginSearchPath() {
    return Stream.of(config.getPluginSearchPath().split(File.pathSeparator))
        .map(File::new)
        .collect(Collectors.toList());
  }

  /**
   * Load a plugin using the given search path. The path is checked for a directory matching the
   * plugin name. The config file in that directory provides the name of the {@link Plugin} class to
   * load out of the provided jars.
   *
   * @param pluginName name of plugin to load
   * @param searchPath list of directories to search
   * @return a descriptor representing the loaded plugin
   */
  PluginDescriptor loadPlugin(
      String pluginName, List<File> searchPath, PluginDownloader pluginDownloader) {
    Path defaultSearchPath = searchPath.get(0).toPath();
    pluginName = pluginDownloader.downloadPluginIfNeeded(pluginName, defaultSearchPath);

    File pluginInstallDir = findPluginInstallDir(pluginName, searchPath);
    logger.debug("Plugin install dir: " + pluginInstallDir);
    PluginMetadata metadata = PluginMetadata.fromInstallDir(pluginInstallDir);
    logger.debug("Metadata: " + metadata);
    List<File> jarList = getPluginJars(pluginInstallDir);
    logger.debug("Plugin jars: " + jarList);

    Class<? extends Plugin> pluginClass = getPluginClass(jarList, metadata.getClassname());
    Plugin pluginInstance = getPluginInstance(pluginClass);
    return new PluginDescriptor(pluginInstance, metadata);
  }

  /** Find the first directory in the search path that matches the plugin name. */
  File findPluginInstallDir(String pluginName, List<File> searchPath) {
    for (File path : searchPath) {
      File[] pluginFolders =
          path.listFiles(file -> file.getName().equals(pluginName) && file.isDirectory());
      if (pluginFolders != null && pluginFolders.length > 0) {
        return pluginFolders[0];
      }
    }
    throw new IllegalArgumentException(
        "Could not locate install for plugin: "
            + pluginName
            + " in search path: "
            + searchPath.stream()
                .map(File::toString)
                .collect(Collectors.joining(File.pathSeparator)));
  }

  /** Get the list of all jar files in the plugin folder. */
  List<File> getPluginJars(File pluginFolder) {
    File[] jarFiles =
        pluginFolder.listFiles(file -> file.getName().endsWith(".jar") && file.isFile());
    if (jarFiles == null) {
      return Collections.emptyList();
    }
    return Arrays.asList(jarFiles);
  }

  /**
   * Load the plugin class out of the provided jars by creating a new class loader.
   *
   * @return loaded plugin Class
   */
  Class<? extends Plugin> getPluginClass(List<File> jarList, String pluginClassName) {
    PluginClassLoader pluginClassLoader =
        new PluginClassLoader(
            jarList.stream()
                .map(
                    jar -> {
                      try {
                        return jar.getAbsoluteFile().toURI().toURL();
                      } catch (MalformedURLException e) {
                        throw new IllegalArgumentException("Invalid jar file name: " + jar, e);
                      }
                    })
                .toArray(URL[]::new));

    try {
      return Class.forName(pluginClassName, true, pluginClassLoader).asSubclass(Plugin.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot load plugin class: " + pluginClassName, e);
    }
  }

  /** Get an instance of the loaded plugin class using reflection. */
  Plugin getPluginInstance(Class<? extends Plugin> pluginClass) {
    try {
      Plugin plugin =
          pluginClass
              .getDeclaredConstructor(new Class[] {LuceneServerConfiguration.class})
              .newInstance(config);
      if (plugin instanceof MetricsPlugin) {
        ((MetricsPlugin) plugin).registerMetrics(collectorRegistry);
      }
      return plugin;
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to load plugin class instance via reflection", e);
    }
  }
}
