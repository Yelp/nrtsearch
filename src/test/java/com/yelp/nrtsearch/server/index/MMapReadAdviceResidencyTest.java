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
package com.yelp.nrtsearch.server.index;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.yelp.nrtsearch.server.config.IndexPreloadConfig;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.utils.PageCacheUtils;
import com.yelp.nrtsearch.server.utils.PageCacheUtils.FileRange;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verify the effect of the mmapReadAdvice setting on page cache residency. With random access
 * advice, reading a few bytes of a file only faults in the pages read. Without it, the OS reads
 * ahead around each page fault.
 */
public class MMapReadAdviceResidencyTest {
  private static final int NUM_PAGES = 256;
  private static final String FILE_NAME = "_0_Lucene99HnswVectorsFormat_0.vec";
  // Context used by Lucene to open vector data files
  private static final IOContext VECTOR_CONTEXT =
      IOContext.DEFAULT.withHints(
          FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM);

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private long residentAfterSparseReads(String dirImpl, String config) throws IOException {
    Path dirPath = folder.newFolder().toPath();
    long pageSize = PageCacheUtils.getPageSize();
    long size = NUM_PAGES * pageSize;
    byte[] data = new byte[(int) size];
    new Random(0).nextBytes(data);
    Path filePath = dirPath.resolve(FILE_NAME);
    Files.write(filePath, data);
    FileRange fullRange = new FileRange(0, size);
    PageCacheUtils.dropFromPageCache(filePath, List.of(fullRange));

    DirectoryFactory factory =
        DirectoryFactory.get(
            dirImpl, new NrtsearchConfig(new ByteArrayInputStream(config.getBytes())));
    try (Directory directory = factory.open(dirPath, new IndexPreloadConfig(false, Set.of()));
        IndexInput in = directory.openInput(FILE_NAME, VECTOR_CONTEXT)) {
      assertTrue(directory instanceof MMapDirectory);
      // read one byte from 4 pages, spread across the file
      for (int i = 0; i < 4; ++i) {
        in.seek(i * (size / 4));
        in.readByte();
      }
      return PageCacheUtils.residentPages(filePath, fullRange);
    }
  }

  private void assertByContextLimitsReadahead(String dirImpl) throws IOException {
    assumeTrue(PageCacheUtils.isSupported() && MMapDirectory.supportsMadvise());
    long byContext = residentAfterSparseReads(dirImpl, "mmapReadAdvice: BY_CONTEXT");
    long none = residentAfterSparseReads(dirImpl, "mmapReadAdvice: NONE");
    // random advice disables readahead, so only the pages read are resident
    assertTrue(dirImpl + " BY_CONTEXT resident pages: " + byContext, byContext <= 4);
    // default OS readahead brings in more pages around each fault
    assertTrue(dirImpl + " NONE resident pages: " + none, none > byContext);
  }

  @Test
  public void testByContextLimitsReadahead_mmapDirectory() throws IOException {
    assertByContextLimitsReadahead("MMapDirectory");
  }

  @Test
  public void testByContextLimitsReadahead_fsDirectory() throws IOException {
    // FSDirectory opens an MMapDirectory on 64-bit platforms
    assertByContextLimitsReadahead("FSDirectory");
  }
}
