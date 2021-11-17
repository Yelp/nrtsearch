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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ContentDownloaderImplTest {
  private final String BUCKET_NAME = "contentdownloader-unittest";

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
  public void getVersionContentTar() throws IOException {
    final TarEntry tarEntry = new TarEntry("file1", "testcontent");
    TarEntry.uploadToS3(
        backupTestHelper.getS3(),
        BUCKET_NAME,
        Arrays.asList(tarEntry),
        "testservice/testresource/abcdef");
    Path downloadDir =
        backupTestHelper
            .getArchiverDirectory()
            .resolve("downloads"); // this dir will be created by getVersionContent
    backupTestHelper
        .getContentDownloaderTar()
        .getVersionContent("testservice", "testresource", "abcdef", downloadDir);
    assertEquals("testcontent", new String(Files.readAllBytes(downloadDir.resolve("file1"))));
  }

  @Test
  public void getVersionContentNoTar() throws IOException {
    backupTestHelper
        .getS3()
        .putObject(BUCKET_NAME, "testservice/testresource/abcdef", "testcontent");
    Path downloadDir =
        backupTestHelper
            .getArchiverDirectory()
            .resolve("downloads"); // this dir will be created by getVersionContent
    backupTestHelper
        .getContentDownloaderNoTar()
        .getVersionContent("testservice", "testresource", "abcdef", downloadDir);
    assertEquals(
        "testcontent", new String(Files.readAllBytes(Paths.get(downloadDir.toString(), "abcdef"))));
  }

  @Test
  public void getVersionContentMultipleFiles() throws IOException {
    final TarEntry tarEntry1 = new TarEntry("file1", "testcontent1");
    final TarEntry tarEntry2 = new TarEntry("file2", "testcontent2");
    TarEntry.uploadToS3(
        backupTestHelper.getS3(),
        BUCKET_NAME,
        Arrays.asList(tarEntry1, tarEntry2),
        "testservice/testresource/abcdef");
    Path downloadDir =
        backupTestHelper
            .getArchiverDirectory()
            .resolve("downloads"); // this dir will be created by getVersionContent
    backupTestHelper
        .getContentDownloaderTar()
        .getVersionContent("testservice", "testresource", "abcdef", downloadDir);
    assertEquals(
        "testcontent1", new String(Files.readAllBytes(Paths.get(downloadDir.toString(), "file1"))));
    assertEquals(
        "testcontent2", new String(Files.readAllBytes(Paths.get(downloadDir.toString(), "file2"))));
  }

  @Test
  public void getVersionContentdownloadDirExists() throws IOException {
    final TarEntry tarEntry = new TarEntry("file1", "testcontent");
    TarEntry.uploadToS3(
        backupTestHelper.getS3(),
        BUCKET_NAME,
        Arrays.asList(tarEntry),
        "testservice/testresource/abcdef");
    Path downloadDir = backupTestHelper.getArchiverDirectory().resolve("downloads");
    Files.createDirectories(downloadDir);
    Files.writeString(
        Files.createFile(downloadDir.resolve("file1")), "localcontent", StandardCharsets.UTF_8);
    backupTestHelper
        .getContentDownloaderNoTar()
        .getVersionContent("testservice", "testresource", "abcdef", downloadDir);
    assertEquals("localcontent", new String(Files.readAllBytes(downloadDir.resolve("file1"))));
  }

  @Test
  public void getS3Client() {
    assertEquals(
        backupTestHelper.getS3(), backupTestHelper.getContentDownloaderTar().getS3Client());
  }

  @Test
  public void getBucketName() {
    assertEquals(BUCKET_NAME, backupTestHelper.getContentDownloaderTar().getBucketName());
  }
}
