/*
 * Copyright 2023 Yelp Inc.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ZipUtilTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private Path extractDirectory;

  @Before
  public void setup() throws IOException {
    extractDirectory = Paths.get(folder.newFolder("extracted").getPath());
  }

  /**
   * Test unzipping the test_zip file and verify that all the files are extracted in the correct
   * directory and have the correct contents.
   */
  @Test
  public void testUnzip() throws IOException {
    try (InputStream inputStream =
        getClass().getClassLoader().getResourceAsStream("util/test_zip.zip")) {
      List<String> actualExtractedFiles = ZipUtil.extractZip(inputStream, extractDirectory, true);

      assertFalse(Files.exists(extractDirectory.resolve("tmp.zip")));

      assertEquals(9, actualExtractedFiles.size());
      List<String> expectedExtractedFiles =
          List.of(
              "test_zip/",
              "test_zip/file1.txt",
              "test_zip/dir2/",
              "test_zip/dir1/",
              "test_zip/dir2/file3",
              "test_zip/dir2/dir3/",
              "test_zip/dir1/file2",
              "test_zip/dir2/dir3/file4",
              "test_zip/dir2/dir3/file5");
      assertEquals(expectedExtractedFiles, actualExtractedFiles);

      for (String fileName : expectedExtractedFiles) {
        Path filePath = extractDirectory.resolve(fileName);
        assertTrue(Files.exists(filePath));
        if (filePath.toFile().isFile()) {
          String actualContents = new String(Files.readAllBytes(filePath));
          Path expectedFilePath =
              Paths.get(getClass().getClassLoader().getResource("util/" + fileName).getPath());
          String expectedContents = new String(Files.readAllBytes(expectedFilePath));
          assertEquals(expectedContents, actualContents);
        }
      }
    }
  }

  @Test
  public void testUnzipPlugin() throws IOException {
    try (InputStream inputStream =
        getClass().getClassLoader().getResourceAsStream("util/example-plugin-0.0.1.zip")) {
      List<String> extractedFiles = ZipUtil.extractZip(inputStream, extractDirectory, false);
      assertEquals(extractedFiles.get(0), "example-plugin-0.0.1/");
    }
  }
}
