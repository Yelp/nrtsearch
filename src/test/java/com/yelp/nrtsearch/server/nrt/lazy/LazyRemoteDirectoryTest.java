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
package com.yelp.nrtsearch.server.nrt.lazy;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.config.LazyRemoteDataConfig;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LazyRemoteDirectoryTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private Path path;
  private Directory base;
  private RemoteBackend backend;
  private LazyRemoteDirectory dir;
  private byte[] fileBytes;
  private NrtFileMetaData meta;
  private final LazyRemoteDataConfig config =
      new LazyRemoteDataConfig(
          true, List.of("vec"), LazyRemoteDataConfig.CacheType.MEMORY, "", 1, 4);

  @Before
  public void setUp() throws Exception {
    path = folder.newFolder("index").toPath();
    base = new NIOFSDirectory(path);
    backend = mock(RemoteBackend.class);
    RemoteBlockCache cache = new RemoteBlockCache.Memory(1024 * 1024, 4096);
    dir = new LazyRemoteDirectory(base, path, "service", backend, cache, config);

    // Build a real Lucene file (header + body + footer) in a scratch directory
    Path scratch = folder.newFolder("scratch").toPath();
    try (Directory sd = new NIOFSDirectory(scratch)) {
      try (IndexOutput out = sd.createOutput("_0.vec", IOContext.DEFAULT)) {
        CodecUtil.writeIndexHeader(out, "Test", 0, new byte[16], "");
        for (int i = 0; i < 10_000; i++) {
          out.writeInt(i);
        }
        CodecUtil.writeFooter(out);
      }
      fileBytes = Files.readAllBytes(scratch.resolve("_0.vec"));
      try (IndexInput in = sd.openInput("_0.vec", IOContext.DEFAULT)) {
        byte[] header = CodecUtil.readIndexHeader(in);
        byte[] footer = CodecUtil.readFooter(in);
        long checksum = CodecUtil.retrieveChecksum(in);
        meta = new NrtFileMetaData(header, footer, fileBytes.length, checksum, "p1", "t1");
      }
    }
    when(backend.readIndexFileRange(
            eq("service"), eq("index-id"), eq("_0.vec"), any(), anyLong(), anyInt()))
        .thenAnswer(
            inv -> {
              long offset = inv.getArgument(4);
              int length = inv.getArgument(5);
              return Arrays.copyOfRange(fileBytes, (int) offset, (int) offset + length);
            });
  }

  /** Write a stub the way a copy job does: to a temp file, then rename into place. */
  private void addStub(String name) throws Exception {
    String tmp = dir.createStubTemp(name, "index-id", meta);
    dir.rename(tmp, name);
  }

  @Test
  public void testStubLooksLikeFile() throws Exception {
    assertTrue(dir.isLazyFile("_0.vec"));
    assertFalse(dir.isLazyFile("_0.vex"));
    addStub("_0.vec");
    try (IndexOutput out = base.createOutput("_0.vex", IOContext.DEFAULT)) {
      out.writeByte((byte) 1);
    }

    assertEquals(List.of("_0.vec", "_0.vex"), Arrays.asList(dir.listAll()));
    assertEquals(fileBytes.length, dir.fileLength("_0.vec"));
    assertEquals(1, dir.fileLength("_0.vex"));

    try (IndexInput in = dir.openInput("_0.vec", IOContext.DEFAULT)) {
      byte[] all = new byte[fileBytes.length];
      in.readBytes(all, 0, all.length);
      assertArrayEquals(fileBytes, all);
    }
    // full checksum verification works through range reads
    try (IndexInput in = dir.openInput("_0.vec", IOContext.DEFAULT)) {
      assertEquals(meta.checksum, CodecUtil.checksumEntireFile(in));
    }
  }

  @Test
  public void testPendingStubInvisibleUntilRenamed() throws Exception {
    String tmp = dir.createStubTemp("_0.vec", "index-id", meta);
    // the temp file is listed like any copy temp file, the stub isn't visible yet
    assertEquals(List.of(tmp), Arrays.asList(dir.listAll()));
    assertFalse(dir.hasStub("_0.vec"));
    assertThrows(java.nio.file.NoSuchFileException.class, () -> dir.fileLength("_0.vec"));

    dir.rename(tmp, "_0.vec");
    assertTrue(dir.hasStub("_0.vec"));
    assertEquals(List.of("_0.vec"), Arrays.asList(dir.listAll()));
    assertEquals(fileBytes.length, dir.fileLength("_0.vec"));
  }

  @Test
  public void testCancelledStubLeavesExistingFileUnchanged() throws Exception {
    // the replica already has _0.vec as a real file
    try (IndexOutput out = base.createOutput("_0.vec", IOContext.DEFAULT)) {
      out.writeByte((byte) 7);
    }
    // a copy job writes a stub for it, then is cancelled and deletes its temp files
    String tmp = dir.createStubTemp("_0.vec", "index-id", meta);
    dir.deleteFile(tmp);
    assertEquals(List.of("_0.vec"), Arrays.asList(dir.listAll()));
    assertFalse(dir.hasStub("_0.vec"));
    assertEquals(1, dir.fileLength("_0.vec"));
    // and a later rename of the deleted temp name is not treated as a stub
    try (IndexOutput out = base.createOutput("other_tmp", IOContext.DEFAULT)) {
      out.writeByte((byte) 8);
    }
    dir.rename("other_tmp", "_1.vec");
    assertFalse(dir.hasStub("_1.vec"));
  }

  @Test
  public void testStubsLoadedOnOpen() throws Exception {
    addStub("_0.vec");
    LazyRemoteDirectory reopened =
        new LazyRemoteDirectory(
            base, path, "service", backend, new RemoteBlockCache.Memory(1024, 1024), config);
    assertTrue(reopened.hasStub("_0.vec"));
    assertEquals(fileBytes.length, reopened.fileLength("_0.vec"));
  }

  @Test
  public void testLookupsDontReadStubFiles() throws Exception {
    addStub("_0.vec");
    // stubs are served from memory: replacing the stub file on disk is not seen
    Files.writeString(path.resolve(LazyFileStub.stubName("_0.vec")), "not json");
    assertEquals(fileBytes.length, dir.fileLength("_0.vec"));
    try (IndexInput in = dir.openInput("_0.vec", IOContext.DEFAULT)) {
      assertEquals(fileBytes.length, in.length());
    }
  }

  @Test
  public void testDeleteAndRename() throws Exception {
    addStub("_0.vec");
    dir.deleteFile("_0.vec");
    assertEquals(0, dir.listAll().length);
    assertNull(LazyFileStub.read(path, "_0.vec"));
    assertFalse(dir.hasStub("_0.vec"));

    // a real file renamed onto a stub's name replaces the stub
    addStub("_1.vec");
    try (IndexOutput out = base.createOutput("tmp_1", IOContext.DEFAULT)) {
      out.writeByte((byte) 7);
    }
    dir.rename("tmp_1", "_1.vec");
    assertFalse(dir.hasStub("_1.vec"));
    assertNull(LazyFileStub.read(path, "_1.vec"));
    assertEquals(1, dir.fileLength("_1.vec"));

    // a stub replaces a real file of the same name, after the deleter removed the real file
    String tmp = dir.createStubTemp("_1.vec", "index-id", meta);
    base.deleteFile("_1.vec");
    dir.rename(tmp, "_1.vec");
    assertTrue(dir.hasStub("_1.vec"));
    assertEquals(fileBytes.length, dir.fileLength("_1.vec"));
  }

  @Test
  public void testSync() throws Exception {
    addStub("_0.vec");
    dir.sync(List.of("_0.vec"));
  }

  @Test
  public void testUnwrap() throws Exception {
    assertSame(dir, LazyRemoteDirectory.unwrap(dir));
    assertSame(dir, LazyRemoteDirectory.unwrap(new NRTCachingDirectory(dir, 1, 1)));
    assertNull(LazyRemoteDirectory.unwrap(base));
  }

  @Test
  public void testWriteStubsForRestore() throws Exception {
    NrtFileMetaData compressed = new NrtFileMetaData(meta.header, meta.footer, 10, 1, "p", "t");
    compressed.compressionType = "LZ4";
    Set<String> stubbed =
        LazyRemoteDirectory.writeStubsForRestore(
            path, config, "index-id", Map.of("_0.vec", meta, "_0.vex", meta, "_1.vec", compressed));
    assertEquals(Set.of("_0.vec"), stubbed);
    // restore writes stubs before the directory is opened
    LazyRemoteDirectory opened =
        new LazyRemoteDirectory(
            base, path, "service", backend, new RemoteBlockCache.Memory(1024, 1024), config);
    assertTrue(opened.hasStub("_0.vec"));
    assertFalse(opened.hasStub("_1.vec"));
  }

  @Test
  public void testCompressedStubRejected() {
    NrtFileMetaData compressed = new NrtFileMetaData(meta.header, meta.footer, 10, 1, "p", "t");
    compressed.compressionType = "ZSTD";
    assertThrows(
        IllegalArgumentException.class, () -> dir.createStubTemp("_0.vec", "index-id", compressed));
  }

  @Test
  public void testConfig() {
    LazyRemoteDataConfig c =
        new LazyRemoteDataConfig(
            true, List.of(".VEC", "veq"), LazyRemoteDataConfig.CacheType.DISK, "", 10, 64);
    assertTrue(c.isLazyFile("_0_Lucene99HnswVectorsFormat_0.vec"));
    assertTrue(c.isLazyFile("_0.veq"));
    assertFalse(c.isLazyFile("_0.vex"));
    assertFalse(c.isLazyFile("segments_1"));
    assertFalse(LazyRemoteDataConfig.disabled().isLazyFile("_0.vec"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LazyRemoteDataConfig(
                true, List.of(), LazyRemoteDataConfig.CacheType.MEMORY, "", 0, 64));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LazyRemoteDataConfig(
                true, List.of(), LazyRemoteDataConfig.CacheType.MEMORY, "", 1, 1));
  }
}
