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
package com.yelp.nrtsearch.vectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BucketRegistryTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static IndexConfig config(String bucket, String index) {
    return new IndexConfig(bucket, index, 4, IndexConfig.DistanceMetric.COSINE, List.of(), 0L);
  }

  /** Make saves fail: a directory where the temp file is written can't be replaced by a file. */
  private static void breakSaves(Path file) throws Exception {
    Files.createDirectory(file.resolveSibling(file.getFileName() + ".tmp"));
  }

  private static void fixSaves(Path file) throws Exception {
    Files.delete(file.resolveSibling(file.getFileName() + ".tmp"));
  }

  @Test
  public void testSavedAndReloaded() throws Exception {
    Path file = folder.getRoot().toPath().resolve("registry.json");
    BucketRegistry registry = new BucketRegistry(file);
    registry.createBucket("bucket-a");
    registry.reserveIndex("bucket-a", "index-a");
    registry.addIndex(config("bucket-a", "index-a"));

    BucketRegistry reloaded = new BucketRegistry(file);
    assertEquals(1, reloaded.listBuckets().size());
    assertEquals("index-a", reloaded.getIndex("bucket-a", "index-a").getIndexName());
  }

  @Test
  public void testFailedAddIndexSaveRollsBack() throws Exception {
    Path file = folder.getRoot().toPath().resolve("registry.json");
    BucketRegistry registry = new BucketRegistry(file);
    registry.createBucket("bucket-a");
    registry.reserveIndex("bucket-a", "index-a");
    breakSaves(file);

    assertThrows(
        UncheckedIOException.class, () -> registry.addIndex(config("bucket-a", "index-a")));
    // the index is not in memory either
    assertTrue(registry.listIndexes("bucket-a").isEmpty());
    assertThrows(VectorsException.class, () -> registry.getIndex("bucket-a", "index-a"));

    // and the name can be used once saves work again
    fixSaves(file);
    registry.reserveIndex("bucket-a", "index-a");
    registry.addIndex(config("bucket-a", "index-a"));
    assertEquals(1, new BucketRegistry(file).listIndexes("bucket-a").size());
  }

  @Test
  public void testFailedCreateBucketSaveRollsBack() throws Exception {
    Path file = folder.getRoot().toPath().resolve("registry.json");
    BucketRegistry registry = new BucketRegistry(file);
    breakSaves(file);
    assertThrows(UncheckedIOException.class, () -> registry.createBucket("bucket-a"));
    assertTrue(registry.listBuckets().isEmpty());
    fixSaves(file);
    registry.createBucket("bucket-a");
    assertEquals(1, registry.listBuckets().size());
  }

  @Test
  public void testReservations() {
    BucketRegistry registry = new BucketRegistry(null);
    registry.createBucket("bucket-a");
    registry.reserveIndex("bucket-a", "index-a");
    // a reserved name can't be reserved again, and blocks bucket deletion
    VectorsException duplicate =
        assertThrows(VectorsException.class, () -> registry.reserveIndex("bucket-a", "index-a"));
    assertEquals(409, duplicate.getHttpStatus());
    assertThrows(VectorsException.class, () -> registry.deleteBucket("bucket-a"));
    registry.releaseIndex("bucket-a", "index-a");
    registry.reserveIndex("bucket-a", "index-a");
    registry.releaseIndex("bucket-a", "index-a");
    registry.deleteBucket("bucket-a");
    // adding an index that wasn't reserved is a bug
    registry.createBucket("bucket-b");
    assertThrows(IllegalStateException.class, () -> registry.addIndex(config("bucket-b", "x-y")));
  }
}
