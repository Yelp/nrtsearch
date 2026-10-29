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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.utils.PageCacheUtils.FileRange;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CompoundFileRangesTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testFileNames() {
    assertTrue(CompoundFileRanges.isCompoundDataFile("_0.cfs"));
    assertTrue(CompoundFileRanges.isCompoundDataFile("_1a.cfs"));
    assertFalse(CompoundFileRanges.isCompoundDataFile("_0.cfe"));
    assertFalse(CompoundFileRanges.isCompoundDataFile("_0_Lucene104_0.tip"));
    assertFalse(CompoundFileRanges.isCompoundDataFile("segments_1"));
    assertEquals("_0.cfe", CompoundFileRanges.entriesFileName("_0.cfs"));
    assertEquals("_1a.cfe", CompoundFileRanges.entriesFileName("_1a.cfs"));
  }

  @Test
  public void testReadMatchesCompoundReader() throws IOException {
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, true);
    try (Directory directory = new MMapDirectory(indexPath)) {
      SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
      assertEquals(1, segmentInfos.size());
      SegmentCommitInfo commitInfo = segmentInfos.info(0);
      assertTrue(commitInfo.info.getUseCompoundFile());
      String cfsName = IndexFileNames.segmentFileName(commitInfo.info.name, "", "cfs");
      long cfsLength = directory.fileLength(cfsName);

      Map<String, FileRange> ranges = CompoundFileRanges.read(directory, cfsName);
      assertTrue(ranges.containsKey(PageCacheTestIndex.QUANTIZED_VEC));
      assertTrue(ranges.containsKey(PageCacheTestIndex.QUANTIZED_VEQ));

      try (CompoundDirectory compoundDirectory =
          commitInfo
              .info
              .getCodec()
              .compoundFormat()
              .getCompoundReader(directory, commitInfo.info)) {
        List<String> subFiles = new ArrayList<>();
        for (String subFile : compoundDirectory.listAll()) {
          subFiles.add(IndexFileNames.stripSegmentName(subFile));
        }
        assertEquals(subFiles.size(), ranges.size());
        for (String subFile : compoundDirectory.listAll()) {
          FileRange range = ranges.get(IndexFileNames.stripSegmentName(subFile));
          assertEquals(compoundDirectory.fileLength(subFile), range.length());
          assertTrue(range.offset() > 0);
          assertTrue(range.offset() + range.length() <= cfsLength);
        }
      }

      // the full precision vectors hold at least dimensions * float bytes * docs
      assertTrue(
          ranges.get(PageCacheTestIndex.QUANTIZED_VEC).length()
              >= (long) PageCacheTestIndex.DIMENSIONS * Float.BYTES * PageCacheTestIndex.NUM_DOCS);
    }
  }

  @Test
  public void testRangesDoNotOverlap() throws IOException {
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, true);
    try (Directory directory = new MMapDirectory(indexPath)) {
      List<FileRange> ranges =
          new ArrayList<>(CompoundFileRanges.read(directory, "_0.cfs").values());
      ranges.sort((a, b) -> Long.compare(a.offset(), b.offset()));
      for (int i = 1; i < ranges.size(); ++i) {
        FileRange prev = ranges.get(i - 1);
        assertTrue(prev.offset() + prev.length() <= ranges.get(i).offset());
      }
    }
  }

  @Test
  public void testMissingEntries() throws IOException {
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, true);
    try (Directory directory = new MMapDirectory(indexPath)) {
      directory.deleteFile("_0.cfe");
      assertThrows(NoSuchFileException.class, () -> CompoundFileRanges.read(directory, "_0.cfs"));
    }
  }

  @Test
  public void testCorruptEntries() throws IOException {
    Path indexPath = folder.newFolder().toPath();
    PageCacheTestIndex.write(indexPath, true);
    try (Directory directory = new MMapDirectory(indexPath)) {
      byte[] bytes = new byte[(int) directory.fileLength("_0.cfe")];
      try (IndexInput in = directory.openInput("_0.cfe", IOContext.READONCE)) {
        in.readBytes(bytes, 0, bytes.length);
      }
      // flip a byte in the entries, before the footer
      bytes[bytes.length - 20] ^= 0x1;
      directory.deleteFile("_0.cfe");
      try (IndexOutput out = directory.createOutput("_0.cfe", IOContext.DEFAULT)) {
        out.writeBytes(bytes, bytes.length);
      }
      assertThrows(CorruptIndexException.class, () -> CompoundFileRanges.read(directory, "_0.cfs"));
    }
  }

  @Test
  public void testNotCompoundFile() throws IOException {
    try (Directory directory = new MMapDirectory(folder.getRoot().toPath())) {
      assertThrows(
          IllegalArgumentException.class, () -> CompoundFileRanges.read(directory, "_0.cfe"));
    }
  }
}
