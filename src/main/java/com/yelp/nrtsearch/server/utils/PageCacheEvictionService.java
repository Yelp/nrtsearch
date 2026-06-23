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
package com.yelp.nrtsearch.server.utils;

import com.yelp.nrtsearch.server.config.PageCacheEvictionConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import org.apache.lucene.store.FileSwitchDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evicts cold index file data from the OS page cache after writing.
 *
 * <p>For standalone files, the entire file is evicted if its extension is in the cold set. For
 * compound files (.cfs), the corresponding entries file (.cfe) is parsed to identify cold sub-file
 * byte ranges, and only those ranges are evicted.
 */
public class PageCacheEvictionService {
  private static final Logger logger = LoggerFactory.getLogger(PageCacheEvictionService.class);

  private static final String CFS_EXTENSION = "cfs";
  private static final String CFE_EXTENSION = "cfe";

  private final PageCacheEvictionConfig config;
  private final PageCacheEvictor evictor;

  public PageCacheEvictionService(PageCacheEvictionConfig config, PageCacheEvictor evictor) {
    this.config = config;
    this.evictor = evictor;
  }

  /**
   * Evicts cold data for a collection of index files. For each standalone file whose extension is
   * in the cold set, the entire file is evicted. For compound files (.cfs), cold sub-file ranges
   * are evicted using offset/length data from the companion .cfe file.
   *
   * @param indexDir directory containing the files
   * @param fileNames names of files to consider for eviction
   */
  public void evictColdData(Path indexDir, Collection<String> fileNames) {
    if (!config.isEnabled() || !evictor.isSupported()) {
      return;
    }
    for (String fileName : fileNames) {
      String ext = FileSwitchDirectory.getExtension(fileName);
      if (CFS_EXTENSION.equals(ext)) {
        evictColdDataForCfs(indexDir, fileName);
      } else if (!CFE_EXTENSION.equals(ext) && config.shouldEvict(ext)) {
        try {
          evictor.evictFile(indexDir.resolve(fileName));
        } catch (IOException e) {
          logger.warn("Failed to evict page cache for file {}: {}", fileName, e.getMessage());
        }
      }
    }
  }

  /**
   * Evicts cold sub-file ranges within a compound file (.cfs). Parses the corresponding .cfe file
   * to find the byte offsets of cold logical files within the .cfs, then evicts those ranges.
   *
   * @param indexDir directory containing the files
   * @param cfsFileName name of the .cfs file
   */
  public void evictColdDataForCfs(Path indexDir, String cfsFileName) {
    if (!config.isEnabled() || !evictor.isSupported()) {
      return;
    }
    String cfeFileName =
        cfsFileName.substring(0, cfsFileName.length() - CFS_EXTENSION.length()) + CFE_EXTENSION;
    Path cfePath = indexDir.resolve(cfeFileName);
    Path cfsPath = indexDir.resolve(cfsFileName);
    try {
      Map<String, CompoundFileEntriesReader.CfeEntry> entries =
          CompoundFileEntriesReader.readEntries(cfePath);
      for (CompoundFileEntriesReader.CfeEntry entry : entries.values()) {
        String ext = FileSwitchDirectory.getExtension(entry.name());
        if (config.shouldEvict(ext)) {
          evictor.evictFileRange(cfsPath, entry.offset(), entry.length());
        }
      }
    } catch (IOException e) {
      logger.warn(
          "Failed to evict page cache for compound file {}: {}", cfsFileName, e.getMessage());
    }
  }
}
