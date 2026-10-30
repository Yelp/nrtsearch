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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.yelp.nrtsearch.server.config.PageCachePolicyConfig;
import com.yelp.nrtsearch.server.config.PageCachePolicyConfig.Action;
import com.yelp.nrtsearch.server.config.PageCachePolicyConfig.IndexPageCachePolicy;
import com.yelp.nrtsearch.server.config.PageCachePolicyConfig.Rule;
import com.yelp.nrtsearch.server.index.DirectoryFactory;
import com.yelp.nrtsearch.server.monitoring.NrtMetrics;
import com.yelp.nrtsearch.server.nrt.pagecache.PageCachePolicyApplier.ApplyResult;
import com.yelp.nrtsearch.server.utils.PageCacheUtils;
import com.yelp.nrtsearch.server.utils.PageCacheUtils.FileRange;
import io.prometheus.metrics.core.metrics.Summary;
import io.prometheus.metrics.model.snapshots.SummarySnapshot.SummaryDataPointSnapshot;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PageCachePolicyApplierTest {
  private static final String INDEX = "test_index";
  private static final IndexPageCachePolicy QUANTIZED_EVICT_POLICY =
      policy(new Rule(List.of("*QuantizedVectorsFormat_*.vec"), Action.EVICT));

  @org.junit.Rule public TemporaryFolder folder = new TemporaryFolder();

  private static IndexPageCachePolicy policy(Rule... rules) {
    return new PageCachePolicyConfig(List.of(rules), Map.of()).getGlobalPolicy();
  }

  private static long summaryCount(Summary summary, String index, String trigger) {
    return summary.collect().getDataPoints().stream()
        .filter(
            dataPoint ->
                index.equals(dataPoint.getLabels().get("index"))
                    && trigger.equals(dataPoint.getLabels().get("trigger")))
        .mapToLong(SummaryDataPointSnapshot::getCount)
        .sum();
  }

  private static String findFile(Directory directory, String suffix) throws IOException {
    return Arrays.stream(directory.listAll())
        .filter(f -> f.endsWith(suffix))
        .findFirst()
        .orElseThrow();
  }

  @Test
  public void testCreate_noEvictRules() throws IOException {
    try (Directory directory = new MMapDirectory(folder.getRoot().toPath())) {
      assertSame(
          PageCachePolicyApplier.NOOP,
          PageCachePolicyApplier.create(
              INDEX,
              folder.getRoot().toPath(),
              directory,
              policy(new Rule(List.of("*"), Action.PRELOAD)),
              DirectoryFactory.MMapReadAdvice.BY_CONTEXT));
    }
  }

  @Test
  public void testCreate_evictRules() throws IOException {
    try (Directory directory = new MMapDirectory(folder.getRoot().toPath())) {
      PageCachePolicyApplier applier =
          PageCachePolicyApplier.create(
              INDEX,
              folder.getRoot().toPath(),
              directory,
              QUANTIZED_EVICT_POLICY,
              DirectoryFactory.MMapReadAdvice.BY_CONTEXT);
      assertEquals(PageCacheUtils.isSupported(), applier != PageCachePolicyApplier.NOOP);
    }
  }

  @Test
  public void testNoopApply() {
    assertEquals(
        new ApplyResult(0, 0, 0, 0, 0),
        PageCachePolicyApplier.NOOP.apply(List.of("_0.cfs"), PageCacheTrigger.NRT_POINT));
  }

  @Test
  public void testEvictRanges_noCompoundFile() throws IOException {
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, false);
    try (Directory directory = new MMapDirectory(indexPath)) {
      PageCachePolicyApplier applier =
          new PageCachePolicyApplier(INDEX, indexPath, directory, QUANTIZED_EVICT_POLICY);
      int evictFiles = 0;
      for (String file : directory.listAll()) {
        List<FileRange> ranges = applier.getEvictRanges(file);
        if (file.endsWith(PageCacheTestIndex.QUANTIZED_VEC)) {
          assertEquals(List.of(new FileRange(0, directory.fileLength(file))), ranges);
          evictFiles++;
        } else {
          assertEquals("file: " + file, List.of(), ranges);
        }
      }
      assertEquals(1, evictFiles);
    }
  }

  @Test
  public void testEvictRanges_compoundFile() throws IOException {
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, true);
    try (Directory directory = new MMapDirectory(indexPath)) {
      PageCachePolicyApplier applier =
          new PageCachePolicyApplier(INDEX, indexPath, directory, QUANTIZED_EVICT_POLICY);
      String cfsName = findFile(directory, ".cfs");
      FileRange expected =
          CompoundFileRanges.read(directory, cfsName).get(PageCacheTestIndex.QUANTIZED_VEC);
      assertEquals(List.of(expected), applier.getEvictRanges(cfsName));
      for (String file : directory.listAll()) {
        if (!file.equals(cfsName)) {
          assertEquals("file: " + file, List.of(), applier.getEvictRanges(file));
        }
      }
    }
  }

  @Test
  public void testEvictRanges_compoundFileFirstMatchWins() throws IOException {
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, true);
    try (Directory directory = new MMapDirectory(indexPath)) {
      String cfsName = findFile(directory, ".cfs");
      Map<String, FileRange> subFiles = CompoundFileRanges.read(directory, cfsName);
      // evict all vector files, except the quantized vectors
      PageCachePolicyApplier applier =
          new PageCachePolicyApplier(
              INDEX,
              indexPath,
              directory,
              policy(
                  new Rule(List.of("*.veq"), Action.DEFAULT),
                  new Rule(List.of("*.vec", "*.veq", "*.vex"), Action.EVICT)));
      List<FileRange> ranges = applier.getEvictRanges(cfsName);
      List<FileRange> expected =
          subFiles.entrySet().stream()
              .filter(e -> e.getKey().endsWith(".vec") || e.getKey().endsWith(".vex"))
              .map(Map.Entry::getValue)
              .toList();
      assertEquals(expected, ranges);
      assertTrue(ranges.size() >= 4);
    }
  }

  @Test
  public void testEvictRanges_compoundFileWithoutEntries() throws IOException {
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, true);
    try (Directory directory = new MMapDirectory(indexPath)) {
      directory.deleteFile(findFile(directory, ".cfe"));
      PageCachePolicyApplier applier =
          new PageCachePolicyApplier(INDEX, indexPath, directory, QUANTIZED_EVICT_POLICY);
      assertEquals(List.of(), applier.getEvictRanges(findFile(directory, ".cfs")));
    }
  }

  @Test
  public void testApply_countsFailures() throws IOException {
    assumeTrue(PageCacheUtils.isSupported());
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, true);
    try (Directory directory = new MMapDirectory(indexPath)) {
      PageCachePolicyApplier applier =
          new PageCachePolicyApplier("failure_index", indexPath, directory, QUANTIZED_EVICT_POLICY);
      // missing standalone file fails to read its length
      ApplyResult result =
          applier.apply(
              List.of("_9_Lucene104HnswBinaryQuantizedVectorsFormat_0.vec"),
              PageCacheTrigger.STARTUP);
      assertEquals(new ApplyResult(0, 0, 1, 0, 0), result);
      assertEquals(
          1.0,
          NrtMetrics.pageCacheEvictionFailures.labelValues("failure_index", "startup").get(),
          0);
    }
  }

  @Test
  public void testApply_compoundFile() throws IOException {
    assumeTrue(PageCacheUtils.isSupported());
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, true);
    try (Directory directory = new MMapDirectory(indexPath)) {
      String cfsName = findFile(directory, ".cfs");
      Path cfsPath = indexPath.resolve(cfsName);
      Map<String, FileRange> subFiles = CompoundFileRanges.read(directory, cfsName);
      FileRange vecRange = subFiles.get(PageCacheTestIndex.QUANTIZED_VEC);
      FileRange veqRange = subFiles.get(PageCacheTestIndex.QUANTIZED_VEQ);
      long pageSize = PageCacheUtils.getPageSize();
      // the index was just written, so it is cached
      long vecPages = PageCacheUtils.residentPages(cfsPath, vecRange);
      long veqPages = PageCacheUtils.residentPages(cfsPath, veqRange);
      assertTrue(vecPages >= vecRange.length() / pageSize);

      PageCachePolicyApplier applier =
          PageCachePolicyApplier.create(
              "apply_index",
              indexPath,
              directory,
              QUANTIZED_EVICT_POLICY,
              DirectoryFactory.MMapReadAdvice.BY_CONTEXT);
      ApplyResult result = applier.apply(List.of(directory.listAll()), PageCacheTrigger.NRT_POINT);
      assertEquals(1, result.files());
      assertEquals(vecRange.length(), result.evictedBytes());
      assertEquals(0, result.failures());

      // at most the partial pages at the edges of the range remain
      assertTrue(PageCacheUtils.residentPages(cfsPath, vecRange) <= 2);
      // other sub-files are not dropped
      assertEquals(veqPages, PageCacheUtils.residentPages(cfsPath, veqRange));
      assertEquals(
          vecRange.length(),
          NrtMetrics.pageCacheEvictedBytes.labelValues("apply_index", "nrt_point").get(),
          0);
      assertEquals(1, summaryCount(NrtMetrics.pageCacheSyncTime, "apply_index", "nrt_point"));
      assertEquals(1, summaryCount(NrtMetrics.pageCacheDropTime, "apply_index", "nrt_point"));
    }
  }

  @Test
  public void testApply_noCompoundFile() throws IOException {
    assumeTrue(PageCacheUtils.isSupported());
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, false);
    try (Directory directory = new MMapDirectory(indexPath)) {
      String vecName = findFile(directory, PageCacheTestIndex.QUANTIZED_VEC);
      String veqName = findFile(directory, PageCacheTestIndex.QUANTIZED_VEQ);
      FileRange vecRange = new FileRange(0, directory.fileLength(vecName));
      FileRange veqRange = new FileRange(0, directory.fileLength(veqName));
      long veqPages = PageCacheUtils.residentPages(indexPath.resolve(veqName), veqRange);

      PageCachePolicyApplier applier =
          PageCachePolicyApplier.create(
              INDEX,
              indexPath,
              directory,
              QUANTIZED_EVICT_POLICY,
              DirectoryFactory.MMapReadAdvice.BY_CONTEXT);
      ApplyResult result =
          applier.apply(List.of(directory.listAll()), PageCacheTrigger.MERGE_PRECOPY);
      assertEquals(1, result.files());
      assertEquals(vecRange.length(), result.evictedBytes());

      assertEquals(0, PageCacheUtils.residentPages(indexPath.resolve(vecName), vecRange));
      assertEquals(veqPages, PageCacheUtils.residentPages(indexPath.resolve(veqName), veqRange));
    }
  }

  @Test
  public void testApply_skipsMissingCompoundFile() throws IOException {
    assumeTrue(PageCacheUtils.isSupported());
    Path indexPath = folder.newFolder().toPath();
    try (Directory directory = new MMapDirectory(indexPath)) {
      PageCachePolicyApplier applier =
          PageCachePolicyApplier.create(
              INDEX,
              indexPath,
              directory,
              QUANTIZED_EVICT_POLICY,
              DirectoryFactory.MMapReadAdvice.BY_CONTEXT);
      // missing entries are logged, not counted as a failure
      assertEquals(
          new ApplyResult(0, 0, 0, 0, 0),
          applier.apply(List.of("_5.cfs"), PageCacheTrigger.NRT_POINT));
    }
  }
}
