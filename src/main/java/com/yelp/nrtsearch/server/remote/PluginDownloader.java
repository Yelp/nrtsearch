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
package com.yelp.nrtsearch.server.remote;

import java.io.Closeable;
import java.nio.file.Path;

/** Interface for downloading plugins from an external source. */
public interface PluginDownloader extends Closeable {

  /**
   * If the plugin name is a path supported by this downloader, download the plugin to the specified
   * directory.
   *
   * @param pluginNameOrPath potential plugin path to download
   * @param destPath directory to store downloaded plugin folder
   * @return resolved plugin name
   */
  String downloadPluginIfNeeded(String pluginNameOrPath, Path destPath);
}
