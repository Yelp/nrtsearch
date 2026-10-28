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
package com.yelp.nrtsearch.server.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import com.yelp.nrtsearch.server.utils.PageCacheUtils.FileRange;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.apache.lucene.util.Constants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PageCacheUtilsTest {
  private static final int NUM_PAGES = 64;

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private Path writeFile(long size) throws IOException {
    byte[] data = new byte[(int) size];
    new Random(0).nextBytes(data);
    Path path = folder.newFile().toPath();
    Files.write(path, data);
    return path;
  }

  @Test
  public void testSupported() {
    assertEquals(Constants.LINUX && Constants.JRE_IS_64BIT, PageCacheUtils.isSupported());
  }

  @Test
  public void testInvalidRange() {
    assertThrows(IllegalArgumentException.class, () -> new FileRange(-1, 10));
    assertThrows(IllegalArgumentException.class, () -> new FileRange(0, -1));
  }

  @Test
  public void testNotSupported() throws IOException {
    assumeFalse(PageCacheUtils.isSupported());
    Path path = writeFile(1024);
    assertThrows(
        IOException.class,
        () -> PageCacheUtils.dropFromPageCache(path, List.of(new FileRange(0, 1024))));
    assertThrows(
        IOException.class, () -> PageCacheUtils.residentPages(path, new FileRange(0, 1024)));
    assertThrows(IOException.class, PageCacheUtils::getPageSize);
  }

  @Test
  public void testDropFile() throws IOException {
    assumeTrue(PageCacheUtils.isSupported());
    long pageSize = PageCacheUtils.getPageSize();
    long size = NUM_PAGES * pageSize;
    Path path = writeFile(size);
    FileRange fullRange = new FileRange(0, size);
    // data was just written, so is cached
    assertEquals(NUM_PAGES, PageCacheUtils.residentPages(path, fullRange));

    PageCacheUtils.dropFromPageCache(path, List.of(fullRange));
    assertEquals(0, PageCacheUtils.residentPages(path, fullRange));
  }

  @Test
  public void testDropRanges() throws IOException {
    assumeTrue(PageCacheUtils.isSupported());
    long pageSize = PageCacheUtils.getPageSize();
    long size = NUM_PAGES * pageSize;
    Path path = writeFile(size);
    FileRange fullRange = new FileRange(0, size);
    assertEquals(NUM_PAGES, PageCacheUtils.residentPages(path, fullRange));

    // ranges are not page aligned, only fully contained pages are dropped
    FileRange range1 = new FileRange(pageSize + 100, 10 * pageSize);
    FileRange range2 = new FileRange(20 * pageSize, 5 * pageSize);
    PageCacheUtils.dropFromPageCache(path, List.of(range1, range2));

    assertEquals(0, PageCacheUtils.residentPages(path, new FileRange(2 * pageSize, 9 * pageSize)));
    assertEquals(0, PageCacheUtils.residentPages(path, range2));
    assertEquals(2, PageCacheUtils.residentPages(path, new FileRange(0, 2 * pageSize)));
    assertEquals(1, PageCacheUtils.residentPages(path, new FileRange(11 * pageSize, pageSize)));
    assertEquals(
        NUM_PAGES - 25,
        PageCacheUtils.residentPages(path, new FileRange(25 * pageSize, size - 25 * pageSize)));
    assertTrue(PageCacheUtils.residentPages(path, fullRange) <= NUM_PAGES - 14);
  }

  @Test
  public void testResidentPages_unaligned() throws IOException {
    assumeTrue(PageCacheUtils.isSupported());
    long pageSize = PageCacheUtils.getPageSize();
    Path path = writeFile(NUM_PAGES * pageSize);
    // spans parts of 3 pages
    assertEquals(3, PageCacheUtils.residentPages(path, new FileRange(pageSize - 1, pageSize + 2)));
    assertEquals(0, PageCacheUtils.residentPages(path, new FileRange(10, 0)));
  }

  @Test
  public void testDropEmptyRange() throws IOException {
    assumeTrue(PageCacheUtils.isSupported());
    long pageSize = PageCacheUtils.getPageSize();
    long size = NUM_PAGES * pageSize;
    Path path = writeFile(size);
    FileRange fullRange = new FileRange(0, size);
    assertEquals(NUM_PAGES, PageCacheUtils.residentPages(path, fullRange));

    // a zero length would mean to the end of the file for the native calls
    PageCacheUtils.dropFromPageCache(path, List.of(new FileRange(pageSize, 0)));
    assertEquals(NUM_PAGES, PageCacheUtils.residentPages(path, fullRange));
  }

  @Test
  public void testDropMissingFile() {
    assumeTrue(PageCacheUtils.isSupported());
    Path path = folder.getRoot().toPath().resolve("missing");
    IOException e =
        assertThrows(
            IOException.class,
            () -> PageCacheUtils.dropFromPageCache(path, List.of(new FileRange(0, 10))));
    assertTrue(e.getMessage(), e.getMessage().startsWith("open failed for " + path));
  }
}
