/*
 * Copyright 2020 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.yelp.nrtsearch.server.plugins;

import com.google.inject.Inject;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator;

import com.google.inject.Injector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class to handle the loading and registration of nrtsearch plugins.
 *
 * Loads the plugins specified by the {@link LuceneServerConfiguration}. Plugins are located
 * by searching the config provided plugin search path for a folder matching the plugin name.
 * A classloader is created with the jars provided in the plugin directory. The plugin provides a
 * config file containing the {@link Plugin} classname that should be loaded. Injection is used to give
 * the loaded plugin access to lucene server resources (such as config).
 *
 * After the plugin is loaded, the provided functionality is registered with the appropriate server components.
 */
public class PluginsService {
    private static final Logger logger = LoggerFactory.getLogger(Plugin.class.getName());

    @Inject
    private LuceneServerConfiguration config;
    @Inject
    private Injector injector;

    private final List<PluginDescriptor> loadedPluginDescriptors = new ArrayList<>();

    /**
     * Load the list of plugins specified in the {@link LuceneServerConfiguration}.
     * This handles both the loading of the plugin class from provided jars and the registration
     * of plugin functionality with other lucene server components.
     */
    public void loadPlugins() {
        logger.info("Loading plugins: " + Arrays.toString(config.getPlugins()));
        List<File> pluginSearchPath = getPluginSearchPath();
        logger.debug("Plugin search path: " + pluginSearchPath);
        for (String plugin : config.getPlugins()) {
            logger.info("Loading plugin: " + plugin);
            PluginDescriptor descriptor = loadPlugin(plugin, pluginSearchPath);
            registerPlugin(descriptor.getPlugin());
            loadedPluginDescriptors.add(descriptor);
        }
    }

    /**
     * Cleanup function called during server shutdown. Closes all loaded plugins using the {@link java.io.Closeable}
     * interface.
     */
    public void shutdown() {
        loadedPluginDescriptors.forEach(plugin -> {
            try {
                plugin.getPlugin().close();
            } catch (Exception e) {
                logger.info("Exception closing plugin: " + plugin.getPluginMetadata().getName(), e);
            }
        });
    }

    /**
     * Get the information for all loaded plugins.
     */
    public List<PluginDescriptor> getLoadedPluginDescriptors() {
        return loadedPluginDescriptors;
    }


    /**
     * Convert the search path into a list of directory. The search path separator is the same as the OS
     * path separator.
     */
    List<File> getPluginSearchPath() {
        return Stream.of(config.getPluginSearchPath().split(File.pathSeparator))
                .map(File::new)
                .collect(Collectors.toList());
    }

    /**
     * Load a plugin using the given search path. The path is checked for a directory matching the plugin name.
     * The config file in that directory provides the name of the {@link Plugin} class to load out of the provided jars.
     *
     * @param pluginName name of plugin to load
     * @param searchPath list of directories to search
     * @return a descriptor representing the loaded plugin
     */
    PluginDescriptor loadPlugin(String pluginName, List<File> searchPath) {
        File pluginInstallDir = findPluginInstallDir(pluginName, searchPath);
        logger.debug("Plugin install dir: " + pluginInstallDir);
        PluginMetadata metadata = PluginMetadata.fromInstallDir(pluginInstallDir);
        logger.debug("Metadata: " + metadata);
        List<File> jarList = getPluginJars(pluginInstallDir);
        logger.debug("Plugin jars: " + jarList);

        Class<?> pluginClass = getPluginClass(jarList, metadata.getClassname());
        Plugin pluginInstance = getPluginInstance(pluginClass);
        return new PluginDescriptor(pluginInstance, metadata);
    }

    /**
     * Find the first directory in the search path that matches the plugin name.
     */
    File findPluginInstallDir(String pluginName, List<File> searchPath) {
        for (File path : searchPath) {
            File[] pluginFolders = path.listFiles(file -> file.getName().equals(pluginName) && file.isDirectory());
            if (pluginFolders != null && pluginFolders.length > 0) {
                return pluginFolders[0];
            }
        }
        throw new IllegalArgumentException("Could not locate install for plugin: " + pluginName +
                " in search path: " + searchPath.stream().map(File::toString).collect(Collectors.joining(File.pathSeparator)));
    }

    /**
     * Get the list of all jar files in the plugin folder.
     */
    List<File> getPluginJars(File pluginFolder) {
        File[] jarFiles = pluginFolder.listFiles(file -> file.getName().endsWith(".jar") && file.isFile());
        if (jarFiles == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(jarFiles);
    }

    /**
     * Load the plugin class out of the provided jars by creating a new class loader.
     * @return loaded plugin Class
     */
    Class<?> getPluginClass(List<File> jarList, String pluginClassName) {
        URLClassLoader pluginClassLoader = new URLClassLoader(
                jarList.stream().map(jar -> {
                    try {
                        return jar.getAbsoluteFile().toURI().toURL();
                    } catch (MalformedURLException e) {
                        throw new IllegalArgumentException("Invalid jar file name: " + jar, e);
                    }
                }).toArray(URL[]::new)
        );

        try {
            return pluginClassLoader.loadClass(pluginClassName);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Cannot find plugin class: " + pluginClassName, e);
        }
    }

    /**
     * Get an instance of the loaded plugin class using guice injection.
     */
    Plugin getPluginInstance(Class<?> pluginClass) {
        Object pluginObject = injector.getInstance(pluginClass);
        if (!(pluginObject instanceof Plugin)) {
            throw new IllegalArgumentException("Class: " + pluginClass.getName() + " is not a Plugin");
        }
        return (Plugin) pluginObject;
    }

    /**
     * Determine what resources this plugin class provides and register them for use with lucene server components.
     * @param plugin loaded plugin instance
     */
    private void registerPlugin(Plugin plugin) {
        if (plugin instanceof AnalysisPlugin) {
            AnalysisPlugin analysisPlugin = (AnalysisPlugin) plugin;
            AnalyzerCreator.register(analysisPlugin.getAnalyzers());
        }
    }
}
