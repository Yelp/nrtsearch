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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.yelp.nrtsearch.server.config.PageCachePolicyConfig;
import com.yelp.nrtsearch.server.config.PageCachePolicyConfig.Action;
import com.yelp.nrtsearch.server.config.PageCachePolicyConfig.IndexPageCachePolicy;
import com.yelp.nrtsearch.server.config.PageCachePolicyConfig.Rule;
import com.yelp.nrtsearch.server.index.DirectoryFactory;
import com.yelp.nrtsearch.server.nrt.pagecache.CompoundFilePreloader.PreloadResult;
import com.yelp.nrtsearch.server.utils.PageCacheUtils;
import com.yelp.nrtsearch.server.utils.PageCacheUtils.FileRange;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CompoundFilePreloaderTest {
  private static final String INDEX = "test_index";
  private static final Rule EVICT_QUANTIZED_VEC =
      new Rule(List.of("*QuantizedVectorsFormat_*.vec"), Action.EVICT);
  private static final Rule PRELOAD_ALL = new Rule(List.of("*"), Action.PRELOAD);

  @org.junit.Rule public TemporaryFolder folder = new TemporaryFolder();

  private static IndexPageCachePolicy policy(Rule... rules) {
    return new PageCachePolicyConfig(List.of(rules), Map.of()).getGlobalPolicy();
  }

  private static String findFile(Directory directory, String suffix) throws IOException {
    return Arrays.stream(directory.listAll())
        .filter(f -> f.endsWith(suffix))
        .findFirst()
        .orElseThrow();
  }

  private static long totalLength(List<FileRange> ranges) {
    return ranges.stream().mapToLong(FileRange::length).sum();
  }

  private static boolean overlaps(FileRange a, FileRange b) {
    return a.offset() < b.offset() + b.length() && b.offset() < a.offset() + a.length();
  }

  @Test
  public void testCreate() throws IOException {
    try (Directory directory = new MMapDirectory(folder.getRoot().toPath())) {
      assertSame(
          CompoundFilePreloader.NOOP,
          CompoundFilePreloader.create(
              INDEX, folder.getRoot().toPath(), directory, policy(EVICT_QUANTIZED_VEC)));
      assertTrue(
          CompoundFilePreloader.create(
                  INDEX, folder.getRoot().toPath(), directory, policy(PRELOAD_ALL))
              != CompoundFilePreloader.NOOP);
    }
  }

  @Test
  public void testNoopPreload() {
    assertEquals(
        new PreloadResult(0, 0, 0, 0),
        CompoundFilePreloader.NOOP.preload(List.of("_0.cfs"), PageCacheTrigger.STARTUP));
  }

  @Test
  public void testMergeAdjacent() {
    int gap = CompoundFilePreloader.MAX_ALIGNMENT_GAP;
    assertEquals(List.of(), CompoundFilePreloader.mergeAdjacent(List.of()));
    // adjacent ranges, and alignment padding, are merged
    assertEquals(
        List.of(new FileRange(0, 300)),
        CompoundFilePreloader.mergeAdjacent(
            List.of(new FileRange(0, 100), new FileRange(100, 100), new FileRange(263, 37))));
    // larger gaps may be another sub-file, and are not merged
    assertEquals(
        List.of(new FileRange(0, 100), new FileRange(100 + gap, 100)),
        CompoundFilePreloader.mergeAdjacent(
            List.of(new FileRange(0, 100), new FileRange(100 + gap, 100))));
    // contained ranges, and empty ranges
    assertEquals(
        List.of(new FileRange(10, 100)),
        CompoundFilePreloader.mergeAdjacent(
            List.of(new FileRange(10, 100), new FileRange(20, 10), new FileRange(500, 0))));
  }

  @Test
  public void testPreloadRanges_preloadAll() throws IOException {
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, true);
    try (Directory directory = new MMapDirectory(indexPath)) {
      String cfsName = findFile(directory, ".cfs");
      CompoundFilePreloader preloader =
          new CompoundFilePreloader(INDEX, indexPath, directory, policy(PRELOAD_ALL));
      List<FileRange> ranges = preloader.getPreloadRanges(cfsName);
      // all sub-files are within one read buffer of each other
      assertEquals(1, ranges.size());
      Map<String, FileRange> subFiles = CompoundFileRanges.read(directory, cfsName);
      long first = subFiles.values().stream().mapToLong(FileRange::offset).min().orElseThrow();
      long last =
          subFiles.values().stream().mapToLong(r -> r.offset() + r.length()).max().orElseThrow();
      assertEquals(new FileRange(first, last - first), ranges.getFirst());
    }
  }

  @Test
  public void testPreloadRanges_disjointFromEvict() throws IOException {
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, true);
    try (Directory directory = new MMapDirectory(indexPath)) {
      String cfsName = findFile(directory, ".cfs");
      IndexPageCachePolicy policy = policy(EVICT_QUANTIZED_VEC, PRELOAD_ALL);
      FileRange evictRange =
          CompoundFileRanges.read(directory, cfsName).get(PageCacheTestIndex.QUANTIZED_VEC);

      // without merging gaps, preload ranges must not overlap the evict range
      List<FileRange> preloadSubFiles = new ArrayList<>();
      for (Map.Entry<String, FileRange> entry :
          CompoundFileRanges.read(directory, cfsName).entrySet()) {
        if (policy.fileAction(entry.getKey()) == Action.PRELOAD) {
          preloadSubFiles.add(entry.getValue());
        }
      }
      assertFalse(preloadSubFiles.isEmpty());
      assertFalse(preloadSubFiles.contains(evictRange));

      // merged ranges only span alignment padding, never the evicted sub-file
      CompoundFilePreloader preloader =
          new CompoundFilePreloader(INDEX, indexPath, directory, policy);
      List<FileRange> ranges = preloader.getPreloadRanges(cfsName);
      assertTrue(totalLength(preloadSubFiles) <= totalLength(ranges));
      assertTrue(
          totalLength(ranges)
              < totalLength(preloadSubFiles)
                  + (long) CompoundFilePreloader.MAX_ALIGNMENT_GAP * preloadSubFiles.size());
      for (FileRange range : ranges) {
        assertFalse(overlaps(range, evictRange));
      }
    }
  }

  @Test
  public void testPreloadRanges_missingEntries() throws IOException {
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, true);
    try (Directory directory = new MMapDirectory(indexPath)) {
      directory.deleteFile(findFile(directory, ".cfe"));
      CompoundFilePreloader preloader =
          new CompoundFilePreloader(INDEX, indexPath, directory, policy(PRELOAD_ALL));
      assertEquals(List.of(), preloader.getPreloadRanges(findFile(directory, ".cfs")));
    }
  }

  @Test
  public void testPreload_ignoresNonCompoundFiles() throws IOException {
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, false);
    try (Directory directory = new MMapDirectory(indexPath)) {
      CompoundFilePreloader preloader =
          new CompoundFilePreloader(INDEX, indexPath, directory, policy(PRELOAD_ALL));
      PreloadResult result =
          preloader.preload(List.of(directory.listAll()), PageCacheTrigger.STARTUP);
      assertEquals(0, result.files());
      assertEquals(0, result.failures());
    }
  }

  @Test
  public void testPreload_residency() throws IOException {
    assumeTrue(PageCacheUtils.isSupported());
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, true);
    try (Directory directory = new MMapDirectory(indexPath)) {
      String cfsName = findFile(directory, ".cfs");
      Path cfsPath = indexPath.resolve(cfsName);
      Map<String, FileRange> subFiles = CompoundFileRanges.read(directory, cfsName);
      FileRange vecRange = subFiles.get(PageCacheTestIndex.QUANTIZED_VEC);
      FileRange veqRange = subFiles.get(PageCacheTestIndex.QUANTIZED_VEQ);
      long fileLength = directory.fileLength(cfsName);
      PageCacheUtils.dropFromPageCache(cfsPath, List.of(new FileRange(0, fileLength)));
      assertEquals(0, PageCacheUtils.residentPages(cfsPath, veqRange));

      IndexPageCachePolicy policy = policy(EVICT_QUANTIZED_VEC, PRELOAD_ALL);
      CompoundFilePreloader preloader =
          CompoundFilePreloader.create(INDEX, indexPath, directory, policy);
      PreloadResult result =
          preloader.preload(List.of(directory.listAll()), PageCacheTrigger.STARTUP);
      assertEquals(1, result.files());
      assertEquals(0, result.failures());

      // preloaded sub-files are resident
      long pageSize = PageCacheUtils.getPageSize();
      assertEquals(
          (veqRange.offset() + veqRange.length() - 1) / pageSize - veqRange.offset() / pageSize + 1,
          PageCacheUtils.residentPages(cfsPath, veqRange));
      // the evicted sub-file is not part of the preload ranges, though readahead while reading
      // its neighbors may load some of it
      for (FileRange range : preloader.getPreloadRanges(cfsName)) {
        assertFalse(overlaps(range, vecRange));
      }
    }
  }

  @Test
  public void testPreloadThenApply_compoundFile() throws IOException {
    assumeTrue(PageCacheUtils.isSupported());
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, true);
    try (Directory directory = new MMapDirectory(indexPath)) {
      String cfsName = findFile(directory, ".cfs");
      Path cfsPath = indexPath.resolve(cfsName);
      Map<String, FileRange> subFiles = CompoundFileRanges.read(directory, cfsName);
      FileRange vecRange = subFiles.get(PageCacheTestIndex.QUANTIZED_VEC);
      FileRange veqRange = subFiles.get(PageCacheTestIndex.QUANTIZED_VEQ);

      IndexPageCachePolicy policy = policy(EVICT_QUANTIZED_VEC, PRELOAD_ALL);
      long fileLength = directory.fileLength(cfsName);
      PageCacheUtils.dropFromPageCache(cfsPath, List.of(new FileRange(0, fileLength)));

      // same order as NRTReplicaNode.applyPageCachePolicy
      List<String> files = List.of(directory.listAll());
      CompoundFilePreloader.create(INDEX, indexPath, directory, policy)
          .preload(files, PageCacheTrigger.NRT_POINT);
      PageCachePolicyApplier.create(
              INDEX, indexPath, directory, policy, DirectoryFactory.MMapReadAdvice.BY_CONTEXT)
          .apply(files, PageCacheTrigger.NRT_POINT);

      long pageSize = PageCacheUtils.getPageSize();
      assertEquals(
          (veqRange.offset() + veqRange.length() - 1) / pageSize - veqRange.offset() / pageSize + 1,
          PageCacheUtils.residentPages(cfsPath, veqRange));
      // the evicted sub-file is not preloaded, except pages shared with neighboring sub-files
      assertTrue(PageCacheUtils.residentPages(cfsPath, vecRange) <= 2);
    }
  }
}
