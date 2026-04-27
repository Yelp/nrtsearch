/*
 * Copyright 2024 Yelp Inc.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BootstrapCleanupDirectoryTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testDeleteFile_removesSymlinkAndBootstrapTarget() throws IOException {
    Path bsDir = folder.newFolder("bootstrap").toPath();
    Path idxDir = folder.newFolder("index").toPath();

    // Write a real file to bootstrapDir
    Path bootstrapFile = bsDir.resolve("seg_1.fdt");
    Files.writeString(bootstrapFile, "segment data");

    // Create a symlink in indexDir pointing to bootstrapDir file
    Path symlink = idxDir.resolve("seg_1.fdt");
    Files.createSymbolicLink(symlink, bootstrapFile);

    Set<String> bootstrapFiles = ConcurrentHashMap.newKeySet();
    bootstrapFiles.add("seg_1.fdt");

    MMapDirectory mmap = new MMapDirectory(idxDir);
    BootstrapCleanupDirectory dir = new BootstrapCleanupDirectory(mmap, bsDir, bootstrapFiles);

    assertTrue(Files.exists(symlink));
    assertTrue(Files.exists(bootstrapFile));

    dir.deleteFile("seg_1.fdt");

    assertFalse(
        "symlink should be deleted",
        Files.exists(symlink, java.nio.file.LinkOption.NOFOLLOW_LINKS));
    assertFalse("bootstrap target should be deleted", Files.exists(bootstrapFile));
    assertFalse("file removed from bootstrap set", bootstrapFiles.contains("seg_1.fdt"));
    dir.close();
  }

  @Test
  public void testDeleteFile_nonBootstrapFile_doesNotTouchBootstrapDir() throws IOException {
    Set<String> bootstrapFiles = ConcurrentHashMap.newKeySet();
    bootstrapFiles.add("seg_1.fdt");

    Path idxDir = folder.newFolder("index2").toPath();
    Path bsDir = folder.newFolder("bootstrap2").toPath();
    MMapDirectory mmap = new MMapDirectory(idxDir);
    BootstrapCleanupDirectory dir = new BootstrapCleanupDirectory(mmap, bsDir, bootstrapFiles);

    // Write a real file directly to indexDir (not a symlink, not a bootstrap file)
    try (IndexOutput out = dir.createOutput("seg_2.fdt", IOContext.DEFAULT)) {
      out.writeBytes(new byte[] {1, 2, 3}, 3);
    }

    dir.deleteFile("seg_2.fdt");

    // bootstrap set unchanged
    assertTrue(bootstrapFiles.contains("seg_1.fdt"));
    dir.close();
  }

  @Test
  public void testDeleteFile_bootstrapTargetAlreadyGone_doesNotThrow() throws IOException {
    Path bsDir = folder.newFolder("bootstrap3").toPath();
    Path idxDir = folder.newFolder("index3").toPath();
    // Create a symlink pointing to a bootstrap file that doesn't exist
    Path missingTarget = bsDir.resolve("seg_missing.fdt");
    Path symlink = idxDir.resolve("seg_missing.fdt");
    Files.createSymbolicLink(symlink, missingTarget);

    Set<String> bootstrapFiles = ConcurrentHashMap.newKeySet();
    bootstrapFiles.add("seg_missing.fdt");

    MMapDirectory mmap = new MMapDirectory(idxDir);
    BootstrapCleanupDirectory dir = new BootstrapCleanupDirectory(mmap, bsDir, bootstrapFiles);

    // Should not throw even though target doesn't exist (Files.deleteIfExists)
    dir.deleteFile("seg_missing.fdt");

    assertFalse(bootstrapFiles.contains("seg_missing.fdt"));
    dir.close();
  }

  @Test
  public void testListAll_includesSymlinks() throws IOException {
    Path bsDir = folder.newFolder("bootstrap4").toPath();
    Path idxDir = folder.newFolder("index4").toPath();

    // Create a bootstrap file and symlink
    Path bootstrapFile = bsDir.resolve("seg_1.fdt");
    Files.writeString(bootstrapFile, "data");
    Files.createSymbolicLink(idxDir.resolve("seg_1.fdt"), bootstrapFile);

    MMapDirectory mmap = new MMapDirectory(idxDir);
    BootstrapCleanupDirectory dir =
        new BootstrapCleanupDirectory(mmap, bsDir, ConcurrentHashMap.newKeySet());

    String[] files = dir.listAll();
    assertArrayEquals(new String[] {"seg_1.fdt"}, files);
    dir.close();
  }

  @Test
  public void testOpenInput_readsSymlinkedBootstrapFile() throws IOException {
    Path bsDir = folder.newFolder("bootstrap5").toPath();
    Path idxDir = folder.newFolder("index5").toPath();

    // Write real data to bootstrap file
    Path bootstrapFile = bsDir.resolve("data.bin");
    byte[] content = new byte[] {10, 20, 30, 40};
    Files.write(bootstrapFile, content);

    // Symlink from indexDir to bootstrapDir
    Files.createSymbolicLink(idxDir.resolve("data.bin"), bootstrapFile);

    MMapDirectory mmap = new MMapDirectory(idxDir);
    BootstrapCleanupDirectory dir =
        new BootstrapCleanupDirectory(mmap, bsDir, ConcurrentHashMap.newKeySet());

    try (var input = dir.openInput("data.bin", IOContext.DEFAULT)) {
      byte[] read = new byte[4];
      input.readBytes(read, 0, 4);
      assertArrayEquals(content, read);
    }
    dir.close();
  }
}
