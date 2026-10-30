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
import com.yelp.nrtsearch.server.monitoring.NrtMetrics;
import com.yelp.nrtsearch.server.utils.PageCacheUtils.FileRange;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Preloads compound file sub-files with the {@link Action#PRELOAD} action into the OS page cache.
 * {@link org.apache.lucene.store.MMapDirectory} can only preload whole files, so compound files are
 * excluded from its preload predicate (see {@link
 * com.yelp.nrtsearch.server.config.IndexPreloadConfig}), and their {@code PRELOAD} sub-file ranges
 * are read here instead. Reading a range makes its pages resident in the page cache, but unlike
 * {@code MMapDirectory} preloading, does not map them into the process address space.
 *
 * <p>Errors are logged, but never thrown.
 */
public class CompoundFilePreloader {
  private static final Logger logger = LoggerFactory.getLogger(CompoundFilePreloader.class);
  static final int READ_BUFFER_SIZE = 1024 * 1024;
  // Sub-files are aligned in compound files (Lucene90CompoundFormat), so gaps up to this size are
  // padding, not data from another sub-file
  static final int MAX_ALIGNMENT_GAP = 64;

  /** Preloader that does nothing. */
  public static final CompoundFilePreloader NOOP = new CompoundFilePreloader();

  /**
   * Result of preloading a set of files.
   *
   * @param files number of compound files with ranges preloaded
   * @param preloadedBytes total length of preloaded ranges
   * @param failures number of files that failed
   * @param nanos time spent preloading
   */
  public record PreloadResult(int files, long preloadedBytes, int failures, long nanos) {}

  private final String indexName;
  private final Path indexPath;
  private final Directory directory;
  private final IndexPageCachePolicy policy;

  /**
   * Create a preloader for an index. If the policy has no {@link Action#PRELOAD} rules, this
   * returns {@link #NOOP}.
   *
   * @param indexName index name, used to label metrics
   * @param indexPath index data directory path
   * @param directory index directory, used to read compound file entries
   * @param policy index page cache policy
   * @return preloader
   */
  public static CompoundFilePreloader create(
      String indexName, Path indexPath, Directory directory, IndexPageCachePolicy policy) {
    if (!policy.hasAction(Action.PRELOAD)) {
      return NOOP;
    }
    return new CompoundFilePreloader(indexName, indexPath, directory, policy);
  }

  private CompoundFilePreloader() {
    this.indexName = null;
    this.indexPath = null;
    this.directory = null;
    this.policy = null;
  }

  @VisibleForTesting
  CompoundFilePreloader(
      String indexName, Path indexPath, Directory directory, IndexPageCachePolicy policy) {
    this.indexName = indexName;
    this.indexPath = indexPath;
    this.directory = directory;
    this.policy = policy;
  }

  /**
   * Preload the {@link Action#PRELOAD} sub-file ranges of the compound files in the given index
   * files. Other files are ignored.
   *
   * @param fileNames names of index files in the directory
   * @param trigger what caused the preload
   * @return preload result
   */
  public PreloadResult preload(Collection<String> fileNames, PageCacheTrigger trigger) {
    if (this == NOOP) {
      return new PreloadResult(0, 0, 0, 0);
    }
    long startNanos = System.nanoTime();
    int files = 0;
    int failures = 0;
    long preloadedBytes = 0;
    for (String fileName : fileNames) {
      if (!CompoundFileRanges.isCompoundDataFile(fileName)) {
        continue;
      }
      try {
        List<FileRange> ranges = getPreloadRanges(fileName);
        if (ranges.isEmpty()) {
          continue;
        }
        Path filePath = indexPath.resolve(fileName);
        // Small files may only be in memory, if using an NRTCachingDirectory
        if (!Files.exists(filePath)) {
          continue;
        }
        preloadedBytes += readRanges(filePath, ranges);
        files++;
      } catch (IOException | RuntimeException e) {
        failures++;
        logger.warn(
            "Unable to preload index {} compound file {}, trigger: {}",
            indexName,
            fileName,
            trigger.label(),
            e);
      }
    }

    long nanos = System.nanoTime() - startNanos;
    PreloadResult result = new PreloadResult(files, preloadedBytes, failures, nanos);
    if (files > 0 || failures > 0) {
      NrtMetrics.pageCachePreloadTime
          .labelValues(indexName, trigger.label())
          .observe(nanos / 1_000_000.0);
      NrtMetrics.pageCachePreloadedBytes
          .labelValues(indexName, trigger.label())
          .inc(preloadedBytes);
      if (failures > 0) {
        NrtMetrics.pageCachePreloadFailures.labelValues(indexName, trigger.label()).inc(failures);
      }
      logger.info(
          "Preloaded compound files for index {}, trigger: {}, files: {}, preloaded bytes: {},"
              + " failures: {}, time: {} ms",
          indexName,
          trigger.label(),
          files,
          preloadedBytes,
          failures,
          nanos / 1_000_000.0);
    }
    return result;
  }

  /**
   * Get the byte ranges of a compound file's sub-files with the {@link Action#PRELOAD} action.
   * Ranges separated only by alignment padding are merged.
   *
   * @param compoundFileName compound data file name
   * @return ranges to preload, in file order, or an empty list
   * @throws IOException on error reading compound file entries
   */
  @VisibleForTesting
  List<FileRange> getPreloadRanges(String compoundFileName) throws IOException {
    Map<String, FileRange> subFiles;
    try {
      subFiles = CompoundFileRanges.read(directory, compoundFileName);
    } catch (NoSuchFileException e) {
      logger.warn(
          "Compound entries file not found for index {} file {}", indexName, compoundFileName);
      return List.of();
    }
    List<FileRange> ranges = new ArrayList<>();
    for (Map.Entry<String, FileRange> entry : subFiles.entrySet()) {
      if (policy.fileAction(entry.getKey()) == Action.PRELOAD) {
        ranges.add(entry.getValue());
      }
    }
    ranges.sort((a, b) -> Long.compare(a.offset(), b.offset()));
    return mergeAdjacent(ranges);
  }

  /**
   * Merge ranges separated only by alignment padding, so they are read as one range. Ranges are
   * never merged across a larger gap, since the gap may be another sub-file that should not be
   * preloaded.
   */
  @VisibleForTesting
  static List<FileRange> mergeAdjacent(List<FileRange> sortedRanges) {
    List<FileRange> merged = new ArrayList<>();
    FileRange current = null;
    for (FileRange range : sortedRanges) {
      if (range.length() == 0) {
        continue;
      }
      if (current == null) {
        current = range;
      } else if (range.offset() - (current.offset() + current.length()) < MAX_ALIGNMENT_GAP) {
        long end = Math.max(current.offset() + current.length(), range.offset() + range.length());
        current = new FileRange(current.offset(), end - current.offset());
      } else {
        merged.add(current);
        current = range;
      }
    }
    if (current != null) {
      merged.add(current);
    }
    return merged;
  }

  private static long readRanges(Path filePath, List<FileRange> ranges) throws IOException {
    long bytesRead = 0;
    ByteBuffer buffer = ByteBuffer.allocateDirect(READ_BUFFER_SIZE);
    try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
      for (FileRange range : ranges) {
        long position = range.offset();
        long end = range.offset() + range.length();
        while (position < end) {
          buffer.clear();
          buffer.limit((int) Math.min(buffer.capacity(), end - position));
          int read = channel.read(buffer, position);
          if (read < 0) {
            throw new IOException(
                "Unexpected end of file " + filePath + " at " + position + ", expected " + end);
          }
          position += read;
        }
        bytesRead += range.length();
      }
    }
    return bytesRead;
  }
}
