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
package com.yelp.nrtsearch.server.plugins;

import com.amazonaws.services.s3.AmazonS3;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.utils.S3Downloader;
import com.yelp.nrtsearch.server.utils.S3Util;
import com.yelp.nrtsearch.server.utils.ZipUtil;
import java.io.Closeable;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Download plugins from external sources. */
public class PluginDownloader implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(PluginDownloader.class);
  private static final String ZIP_EXTENSION = ".zip";

  private final S3Downloader s3Downloader;
  private final boolean saveBeforeUnzip;

  public PluginDownloader(AmazonS3 amazonS3, LuceneServerConfiguration configuration) {
    this.s3Downloader = new S3Downloader(amazonS3);
    this.saveBeforeUnzip = configuration.getSavePluginBeforeUnzip();
  }

  /**
   * If the plugin name is an external URI, download the plugin from the URI. Currently only S3 URI
   * containing a zip file is supported.
   *
   * @param pluginName Plugin name from configuration
   * @param searchPath Path of local storage where plugin files must be located to be loaded
   * @return Updated file name
   */
  public String downloadPluginIfNeeded(String pluginName, Path searchPath) {
    if (S3Util.isValidS3FilePath(pluginName) && pluginName.endsWith(ZIP_EXTENSION)) {
      logger.info("Downloading plugin: {}", pluginName);
      ZipUtil.extractZip(s3Downloader.downloadFromS3Path(pluginName), searchPath, saveBeforeUnzip);
      // Assuming that the plugin directory is same as the name of the file
      return S3Util.getS3FileName(pluginName).split(".zip")[0];
    }
    return pluginName;
  }

  @Override
  public void close() {
    s3Downloader.close();
  }
}
