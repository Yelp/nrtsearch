/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.nrt.pagecache;

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.config.PageCachePolicyConfig.Action;
import com.yelp.nrtsearch.server.config.PageCachePolicyConfig.IndexPageCachePolicy;
import com.yelp.nrtsearch.server.index.DirectoryFactory;
import com.yelp.nrtsearch.server.monitoring.NrtMetrics;
import com.yelp.nrtsearch.server.utils.PageCacheUtils;
import com.yelp.nrtsearch.server.utils.PageCacheUtils.DropStats;
import com.yelp.nrtsearch.server.utils.PageCacheUtils.FileRange;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies an index page cache policy to index files on a replica. Ranges with the {@link
 * Action#EVICT} action are dropped from the OS page cache. For compound files, the action of each
 * sub-file is used, and only the ranges of matching sub-files are dropped.
 *
 * <p>The policy should be applied before an index reader opens the files, since pages that are
 * mapped into the process address space are not dropped. Errors are logged and counted, but never
 * thrown, so they cannot fail a copy.
 */
public class PageCachePolicyApplier {
  private static final Logger logger = LoggerFactory.getLogger(PageCachePolicyApplier.class);

  /** Applier that does nothing. */
  public static final PageCachePolicyApplier NOOP = new PageCachePolicyApplier();

  /** What caused the policy to be applied, used to label metrics. */
  public enum Trigger {
    /** Files copied for a new nrt point. */
    NRT_POINT,
    /** Files pre-copied for a merged segment. */
    MERGE_PRECOPY,
    /** Existing or restored index files on replica startup. */
    STARTUP;

    final String label = name().toLowerCase(Locale.ROOT);
  }

  /**
   * Result of applying the policy to a set of files.
   *
   * @param files number of files with ranges to evict
   * @param evictedBytes total length of evicted ranges
   * @param failures number of files that failed
   * @param syncNanos time spent writing back dirty pages
   * @param dropNanos time spent dropping pages
   */
  public record ApplyResult(
      int files, long evictedBytes, int failures, long syncNanos, long dropNanos) {}

  private final String indexName;
  private final Path indexPath;
  private final Directory directory;
  private final IndexPageCachePolicy policy;

  /**
   * Create an applier for an index. If the policy has no {@link Action#EVICT} rules, or page cache
   * operations are not supported on this platform, this returns {@link #NOOP}.
   *
   * @param indexName index name, used to label metrics
   * @param indexPath index data directory path
   * @param directory index directory, used to read file lengths and compound file entries
   * @param policy index page cache policy
   * @param readAdvice mmap read advice setting
   * @return applier
   */
  public static PageCachePolicyApplier create(
      String indexName,
      Path indexPath,
      Directory directory,
      IndexPageCachePolicy policy,
      DirectoryFactory.MMapReadAdvice readAdvice) {
    if (!policy.hasAction(Action.EVICT)) {
      return NOOP;
    }
    if (!PageCacheUtils.isSupported()) {
      logger.warn(
          "Page cache EVICT rules are set for index {}, but are not supported on this platform",
          indexName);
      return NOOP;
    }
    if (readAdvice != DirectoryFactory.MMapReadAdvice.BY_CONTEXT) {
      logger.warn(
          "Page cache EVICT rules are set for index {}, but mmapReadAdvice is {}. Evicted data may"
              + " be read back into the page cache by OS readahead, set mmapReadAdvice to"
              + " BY_CONTEXT",
          indexName,
          readAdvice);
    }
    return new PageCachePolicyApplier(indexName, indexPath, directory, policy);
  }

  private PageCachePolicyApplier() {
    this.indexName = null;
    this.indexPath = null;
    this.directory = null;
    this.policy = null;
  }

  @VisibleForTesting
  PageCachePolicyApplier(
      String indexName, Path indexPath, Directory directory, IndexPageCachePolicy policy) {
    this.indexName = indexName;
    this.indexPath = indexPath;
    this.directory = directory;
    this.policy = policy;
  }

  /**
   * Apply the policy to the given index files.
   *
   * @param fileNames names of index files in the directory
   * @param trigger what caused the policy to be applied
   * @return apply result
   */
  public ApplyResult apply(Collection<String> fileNames, Trigger trigger) {
    if (this == NOOP) {
      return new ApplyResult(0, 0, 0, 0, 0);
    }
    long startNanos = System.nanoTime();
    int files = 0;
    int failures = 0;
    long evictedBytes = 0;
    long syncNanos = 0;
    long dropNanos = 0;
    for (String fileName : fileNames) {
      try {
        List<FileRange> ranges = getEvictRanges(fileName);
        if (ranges.isEmpty()) {
          continue;
        }
        Path filePath = indexPath.resolve(fileName);
        // Small files may only be in memory, if using an NRTCachingDirectory
        if (!Files.exists(filePath)) {
          continue;
        }
        DropStats stats = PageCacheUtils.dropFromPageCache(filePath, ranges);
        long fileBytes = 0;
        for (FileRange range : ranges) {
          fileBytes += range.length();
        }
        files++;
        evictedBytes += fileBytes;
        syncNanos += stats.syncNanos();
        dropNanos += stats.dropNanos();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Dropped {} bytes in {} ranges of {} from page cache, sync: {} ms, drop: {} ms",
              fileBytes,
              ranges.size(),
              fileName,
              toMillis(stats.syncNanos()),
              toMillis(stats.dropNanos()));
        }
      } catch (IOException | RuntimeException e) {
        failures++;
        logger.warn(
            "Unable to drop index {} file {} from page cache, trigger: {}",
            indexName,
            fileName,
            trigger.label,
            e);
      }
    }

    ApplyResult result = new ApplyResult(files, evictedBytes, failures, syncNanos, dropNanos);
    if (files > 0 || failures > 0) {
      NrtMetrics.pageCacheSyncTime
          .labelValues(indexName, trigger.label)
          .observe(toMillis(syncNanos));
      NrtMetrics.pageCacheDropTime
          .labelValues(indexName, trigger.label)
          .observe(toMillis(dropNanos));
      NrtMetrics.pageCacheEvictedBytes.labelValues(indexName, trigger.label).inc(evictedBytes);
      if (failures > 0) {
        NrtMetrics.pageCacheEvictionFailures.labelValues(indexName, trigger.label).inc(failures);
      }
      logger.info(
          "Applied page cache policy for index {}, trigger: {}, files: {}, evicted bytes: {},"
              + " failures: {}, sync: {} ms, drop: {} ms, total: {} ms",
          indexName,
          trigger.label,
          files,
          evictedBytes,
          failures,
          toMillis(syncNanos),
          toMillis(dropNanos),
          toMillis(System.nanoTime() - startNanos));
    }
    return result;
  }

  /**
   * Get the byte ranges of an index file with the {@link Action#EVICT} action.
   *
   * @param fileName index file name
   * @return ranges to evict, or an empty list
   * @throws IOException on error reading the file length or compound file entries
   */
  @VisibleForTesting
  List<FileRange> getEvictRanges(String fileName) throws IOException {
    if (CompoundFileRanges.isCompoundDataFile(fileName)) {
      Map<String, FileRange> subFiles;
      try {
        subFiles = CompoundFileRanges.read(directory, fileName);
      } catch (NoSuchFileException e) {
        // Entries file may be copied after the data file in a later job
        logger.warn("Compound entries file not found for index {} file {}", indexName, fileName);
        return List.of();
      }
      List<FileRange> ranges = new ArrayList<>();
      for (Map.Entry<String, FileRange> entry : subFiles.entrySet()) {
        if (policy.fileAction(entry.getKey()) == Action.EVICT) {
          ranges.add(entry.getValue());
        }
      }
      return ranges;
    }
    if (policy.indexFileAction(fileName) == Action.EVICT) {
      return List.of(new FileRange(0, directory.fileLength(fileName)));
    }
    return List.of();
  }

  private static double toMillis(long nanos) {
    return nanos / 1_000_000.0;
  }
}
