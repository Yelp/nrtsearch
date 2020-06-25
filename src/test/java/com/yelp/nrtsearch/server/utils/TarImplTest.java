/*
 * Copyright 2020 Yelp Inc.
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

import com.amazonaws.util.IOUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TarImplTest {

  private TarEntry tarEntry1;
  private TarEntry tarEntry2;

  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private Path tarTestBaseDirectory;

  @Before
  public void setup() throws IOException {
    tarEntry1 = new TarEntry("/foo", "testcontentoffoo");
    tarEntry2 = new TarEntry("/bar/baz", "testcontentofbaz");
    tarTestBaseDirectory = folder.newFolder("tar").toPath();
  }

  @Test
  public void extractTar() throws IOException {
    Path sourceTarFile = tarTestBaseDirectory.resolve("test_tar.tar.lz4");
    Path destDir = tarTestBaseDirectory.resolve("extractedDir");
    try (final FileOutputStream fileOutputStream = new FileOutputStream(sourceTarFile.toFile()); ) {
      byte[] tarContent = getTarFile(Arrays.asList(tarEntry1, tarEntry2));
      fileOutputStream.write(tarContent);
    }
    new TarImpl(TarImpl.CompressionMode.LZ4).extractTar(sourceTarFile, destDir);

    assertEquals(Files.readAllLines(destDir.resolve("foo")).get(0), tarEntry1.content);
    assertEquals(
        Files.readAllLines(destDir.resolve("bar").resolve("baz")).get(0), tarEntry2.content);
  }

  @Test
  public void buildTar() throws IOException {
    Path sourceDir = Files.createDirectory(tarTestBaseDirectory.resolve("dirToTar"));
    Path subDir = Files.createDirectory(sourceDir.resolve("subDir"));
    try (ByteArrayInputStream test1content = new ByteArrayInputStream("test1content".getBytes());
        ByteArrayInputStream test2content = new ByteArrayInputStream("test2content".getBytes());
        FileOutputStream fileOutputStream1 =
            new FileOutputStream(sourceDir.resolve("test1").toFile());
        FileOutputStream fileOutputStream2 =
            new FileOutputStream(subDir.resolve("test2").toFile()); ) {
      IOUtils.copy(test1content, fileOutputStream1);
      IOUtils.copy(test2content, fileOutputStream2);
    }
    Path destTarFile = tarTestBaseDirectory.resolve("result.tar.gz");
    new TarImpl(TarImpl.CompressionMode.LZ4).buildTar(sourceDir, destTarFile);

    try (final FileInputStream fileInputStream = new FileInputStream(destTarFile.toFile());
        final LZ4FrameInputStream compressorInputStream = new LZ4FrameInputStream(fileInputStream);
        final TarArchiveInputStream tarArchiveInputStream =
            new TarArchiveInputStream(compressorInputStream); ) {
      Path destDir = tarTestBaseDirectory.resolve("test_extract");
      new TarImpl(TarImpl.CompressionMode.LZ4).extractTar(tarArchiveInputStream, destDir);
      assertEquals(true, dirsMatch(sourceDir.toFile(), destDir.resolve("dirToTar").toFile()));
    }
  }

  static boolean dirsMatch(File file1, File file2) throws IOException {
    if (file1.isDirectory() && file2.isDirectory()) {
      File[] files1 = file1.listFiles();
      File[] files2 = file2.listFiles();
      if (files1.length != files2.length) {
        return false;
      }
      for (int i = 0; i < files1.length; i++) {
        boolean isMatch = dirsMatch(files1[i], files2[i]);
        if (!isMatch) {
          return false;
        }
      }
      return true;
    } else if (file1.isFile() && file2.isFile()) {
      return Files.readAllLines(file1.toPath())
          .get(0)
          .equals(Files.readAllLines(file2.toPath()).get(0));
    }
    return false;
  }

  byte[] getTarFile(List<TarEntry> tarEntries) throws IOException {
    try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final LZ4FrameOutputStream compressorOutputStream =
            new LZ4FrameOutputStream(byteArrayOutputStream);
        final TarArchiveOutputStream tarArchiveOutputStream =
            new TarArchiveOutputStream(compressorOutputStream); ) {
      for (final TarEntry tarEntry : tarEntries) {
        final byte[] data = tarEntry.content.getBytes(StandardCharsets.UTF_8);
        final TarArchiveEntry archiveEntry = new TarArchiveEntry(tarEntry.path);
        archiveEntry.setSize(data.length);
        tarArchiveOutputStream.putArchiveEntry(archiveEntry);
        tarArchiveOutputStream.write(data);
        tarArchiveOutputStream.closeArchiveEntry();
      }

      tarArchiveOutputStream.close();
      return byteArrayOutputStream.toByteArray();
    }
  }

  class TarEntry {
    public final String path;
    public final String content;

    public TarEntry(String path, String content) {
      this.path = path;
      this.content = content;
    }
  }

  public static void main(String[] args) throws IOException {
    // lz4
    TarImpl lz4Tar = new TarImpl(Tar.CompressionMode.LZ4);
    long t1 = System.nanoTime();
    lz4Tar.buildTar(Paths.get(args[0]), Paths.get(args[1] + ".lz4"));
    long t2 = System.nanoTime();
    System.out.println("buildTar with lz4 took " + (t2 - t1) / (1000 * 1000 * 1000) + " seconds");

    lz4Tar.extractTar(Paths.get(args[1] + ".lz4"), Paths.get(args[0], "lz4"));
    long t3 = System.nanoTime();
    System.out.println("extractTar with lz4 took " + (t3 - t2) / (1000 * 1000 * 1000) + " seconds");

    // gzip
    TarImpl gzipTar = new TarImpl(Tar.CompressionMode.GZIP);
    t1 = System.nanoTime();
    gzipTar.buildTar(Paths.get(args[0]), Paths.get(args[1] + ".gzip"));
    t2 = System.nanoTime();
    System.out.println(
        "buildTar with with gzip took " + (t2 - t1) / (1000 * 1000 * 1000) + " seconds");

    gzipTar.extractTar(Paths.get(args[1] + ".gzip"), Paths.get(args[0], "gzip"));
    t3 = System.nanoTime();
    System.out.println(
        "extractTar with gzip took " + (t3 - t2) / (1000 * 1000 * 1000) + " seconds");
  }
}
