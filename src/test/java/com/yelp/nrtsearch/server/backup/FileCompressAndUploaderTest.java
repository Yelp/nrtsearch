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

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileCompressAndUploaderTest {
  private final String BUCKET_NAME = "filecompressanduploader-unittest";
  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private BackupTestHelper backupTestHelper;

  @Before
  public void setup() throws IOException {
    backupTestHelper = new BackupTestHelper(BUCKET_NAME, folder);
  }

  @After
  public void teardown() {
    backupTestHelper.shutdown();
  }

  @Test
  public void uploadWithTarImplSingleFile() throws IOException {
    Path indexDir =
        Files.createDirectory(backupTestHelper.getArchiverDirectory().resolve("testIndex"));
    Path indexFile = Paths.get(indexDir.toString(), "file1");
    Files.writeString(indexFile, "testcontent");
    backupTestHelper
        .getFileCompressAndUploaderWithTar()
        .upload("testservice", "testresource", "file1", indexDir, false);
    backupTestHelper.testUpload(
        "testservice", "testresource", "file1", Map.of("file1", "testcontent"), null);
  }

  public void createDirWithContents(Map<String, String> filesAndContents, Path dir)
      throws IOException {
    Path indexDir = Files.createDirectories(dir);
    for (Map.Entry<String, String> entry : filesAndContents.entrySet()) {
      Files.writeString(Paths.get(indexDir.toString(), entry.getKey()), entry.getValue());
    }
  }

  @Test
  public void createDirWithContents() throws IOException {
    createDirWithContents(
        Map.of("file1", "testcontent1", "file2", "testcontent2"),
        backupTestHelper.getArchiverDirectory().resolve("testIndex"));
    backupTestHelper
        .getFileCompressAndUploaderWithTar()
        .upload(
            "testservice",
            "testresource",
            "abcdef",
            backupTestHelper.getArchiverDirectory().resolve("testIndex"),
            true);
    backupTestHelper.testUpload(
        "testservice",
        "testresource",
        "abcdef",
        Map.of("file1", "testcontent1", "file2", "testcontent2"),
        null);
  }

  @Test(expected = IllegalStateException.class)
  public void uploadEntireDirNoTar() throws IOException {
    createDirWithContents(
        Map.of("file1", "testcontent1", "file2", "testcontent2"),
        backupTestHelper.getArchiverDirectory().resolve("testIndex"));
    backupTestHelper
        .getFileCompressAndUploaderWithNoTar()
        .upload(
            "testservice",
            "testresource",
            "abcdef",
            backupTestHelper.getArchiverDirectory().resolve("testIndex"),
            true);
  }

  @Test
  public void uploadWithNoTarImpl() throws IOException {
    Path indexDir =
        Files.createDirectory(backupTestHelper.getArchiverDirectory().resolve("testIndex"));
    Path indexFile = Paths.get(indexDir.toString(), "file1");
    Files.writeString(indexFile, "abcdef");
    backupTestHelper
        .getFileCompressAndUploaderWithNoTar()
        .upload("testservice", "testresource", "file1", indexDir, false);
    S3Object s3Object =
        backupTestHelper.getS3().getObject(BUCKET_NAME, "testservice/testresource/file1");
    assertEquals("abcdef", IOUtils.toString(s3Object.getObjectContent()));
  }
}
