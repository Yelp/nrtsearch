/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.remote;

import com.yelp.nrtsearch.server.plugins.FileCompressorPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry for {@link FileCompressor} implementations. Built-in compressors are registered at
 * construction. Plugins implementing {@link FileCompressorPlugin} may register additional
 * compressors by name.
 */
public class FileCompressorCreator {

  private static FileCompressorCreator instance;

  private final Map<String, FileCompressor> compressors = new HashMap<>();

  private FileCompressorCreator() {
    compressors.put("LZ4", new LZ4FileCompressor());
  }

  /**
   * Get a registered {@link FileCompressor} by name.
   *
   * @param name compressor name, or null/"NONE"/"" to indicate no compression
   * @return the registered compressor, or null if name indicates no compression
   * @throws IllegalArgumentException if name is not registered and is not a no-compression value
   */
  public FileCompressor getCompressor(String name) {
    if (name == null || name.isEmpty() || "NONE".equals(name)) {
      return null;
    }
    FileCompressor compressor = compressors.get(name);
    if (compressor == null) {
      throw new IllegalArgumentException(
          "Unknown compressor: " + name + ", must be one of: " + compressors.keySet());
    }
    return compressor;
  }

  private void register(String name, FileCompressor compressor) {
    if (compressors.containsKey(name)) {
      throw new IllegalArgumentException("FileCompressor " + name + " already exists");
    }
    compressors.put(name, compressor);
  }

  /**
   * Initialize the singleton instance. Registers any additional {@link FileCompressor}
   * implementations provided by {@link FileCompressorPlugin}s.
   *
   * @param plugins list of loaded plugins
   */
  public static void initialize(Iterable<Plugin> plugins) {
    instance = new FileCompressorCreator();
    for (Plugin plugin : plugins) {
      if (plugin instanceof FileCompressorPlugin compressorPlugin) {
        compressorPlugin.getFileCompressors().forEach(instance::register);
      }
    }
  }

  /** Get the singleton instance. */
  public static FileCompressorCreator getInstance() {
    return instance;
  }
}
