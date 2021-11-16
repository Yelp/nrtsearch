/*
 * Copyright 2021 Yelp Inc.
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
package com.yelp.nrtsearch.server.backup;

import static org.junit.Assert.assertEquals;

import com.amazonaws.util.IOUtils;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NoTarImplTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void extractTar() throws IOException {
    Path sourceDir = folder.newFolder("sourceDir").toPath();
    Path file = sourceDir.resolve("file1");
    Files.createFile(file);
    try (ByteArrayInputStream test1content = new ByteArrayInputStream("test1content".getBytes())) {
      new NoTarImpl().extractTar(test1content, sourceDir, file.getFileName().toString());
      assertEquals("test1content", Files.readString(sourceDir.resolve("file1")));
    }
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testExtractTar() throws IOException {
    new NoTarImpl()
        .extractTar(
            new TarArchiveInputStream(new ByteArrayInputStream("test1content".getBytes())), null);
  }

  @Test
  public void buildTar() throws IOException {
    Path sourceDir = folder.newFolder("sourceDir").toPath();
    try (ByteArrayInputStream test1content = new ByteArrayInputStream("test1content".getBytes());
        FileOutputStream fileOutputStream1 =
            new FileOutputStream(sourceDir.resolve("test1").toFile())) {
      IOUtils.copy(test1content, fileOutputStream1);
    }
    try (FileOutputStream outputStream =
        new FileOutputStream(sourceDir.resolve("output").toFile())) {
      new NoTarImpl()
          .buildTar(
              sourceDir.resolve("test1"),
              outputStream,
              Collections.emptyList(),
              Collections.emptyList());
    }
    assertEquals("test1content", Files.readString(sourceDir.resolve("output")));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testBuildTar() throws IOException {
    new NoTarImpl()
        .buildTar(
            folder.newFolder("sourceDir").toPath(),
            folder.newFolder("destFile").toPath(),
            Collections.emptyList(),
            Collections.emptyList());
  }

  @Test
  public void getCompressionMode() {
    assertEquals(Tar.CompressionMode.NONE, new NoTarImpl().getCompressionMode());
  }
}
