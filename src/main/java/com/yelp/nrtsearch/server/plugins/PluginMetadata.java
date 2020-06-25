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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;

/**
 * Contains the metadata present in the plugin-metadata.yaml file provided with the plugin
 * installation. This is used by the {@link PluginsService} to load the plugin.
 *
 * <p>The config file is expected to be a valid yaml file containing the following keys: name:
 * plugin name version: plugin version description: plugin description classname: full classname of
 * plugin provided class that extends {@link Plugin}
 */
public class PluginMetadata {
  static final String METADATA_FILE = "plugin-metadata.yaml";
  static final String NAME = "name";
  static final String VERSION = "version";
  static final String DESCRIPTION = "description";
  static final String CLASSNAME = "classname";

  private final String name;
  private final String version;
  private final String description;
  private final String classname;

  /**
   * Loads the plugin metadata from a plugin installation directory. This directory is expected to
   * contain a plugin-metadata.yaml config file.
   *
   * @param pluginDir base plugin installation directory containing metadata config file and jars
   * @return plugin metadata loaded from config file
   * @throws IllegalArgumentException if config file does not exist in plugin directory, the yaml
   *     file is not a hash, or a required key is missing
   */
  static PluginMetadata fromInstallDir(File pluginDir) {
    try {
      File metadataFile = getMetadataFile(pluginDir);
      return fromInputStream(new FileInputStream(metadataFile));
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(
          "File: " + METADATA_FILE + " not found in: " + pluginDir, e);
    }
  }

  /**
   * Load plugin metadata from a yaml {@link InputStream}.
   *
   * @param fileStream stream containing yaml file contents
   * @return plugin metadata loaded from input stream
   * @throws IllegalArgumentException if the yaml is not a hash, or a required key is missing
   */
  static PluginMetadata fromInputStream(InputStream fileStream) {
    Yaml yaml = new Yaml();
    Object configObject = yaml.load(fileStream);
    if (!(configObject instanceof Map)) {
      throw new IllegalArgumentException("Plugin metadata must be a yaml hash");
    }
    Map<Object, Object> configMap = (Map<Object, Object>) configObject;
    String name = getConfigString(configMap, NAME);
    String version = getConfigString(configMap, VERSION);
    String description = getConfigString(configMap, DESCRIPTION);
    String classname = getConfigString(configMap, CLASSNAME);
    return new PluginMetadata(classname, name, version, description);
  }

  /**
   * Find the plugin metadata yaml file given the plugin install directory.
   *
   * @param pluginDir plugin directory containing metadata file and jars
   * @return metadata yaml {@link File}
   * @throws FileNotFoundException if metadata file is not present in directory
   */
  static File getMetadataFile(File pluginDir) throws FileNotFoundException {
    File[] files =
        pluginDir.listFiles(file -> file.getName().equals(METADATA_FILE) && file.isFile());
    if (files != null && files.length > 0) {
      return files[0];
    }
    throw new FileNotFoundException("No metadata file for plugin");
  }

  private static String getConfigString(Map<Object, Object> configMap, String key) {
    if (!configMap.containsKey(key) || configMap.get(key) == null) {
      throw new IllegalArgumentException("Plugin config missing key: " + key);
    }
    return configMap.get(key).toString();
  }

  PluginMetadata(String classname, String name, String version, String description) {
    this.classname = classname;
    this.name = name;
    this.version = version;
    this.description = description;
  }

  /** Get the full class name of the provided {@link Plugin}. Used to load class from jars. */
  public String getClassname() {
    return classname;
  }

  /** Get plugin name. */
  public String getName() {
    return name;
  }

  /** Get plugin version. */
  public String getVersion() {
    return version;
  }

  /** Get a description of the plugin. */
  public String getDescription() {
    return description;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PluginMetadata{");
    sb.append("name='").append(name).append("'");
    sb.append(", classname='").append(classname).append("'");
    sb.append(", version='").append(version).append("'");
    sb.append(", description='").append(description).append("'");
    sb.append('}');
    return sb.toString();
  }
}
