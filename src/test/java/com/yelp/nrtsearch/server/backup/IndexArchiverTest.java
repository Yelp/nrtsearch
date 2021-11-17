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

import static com.yelp.nrtsearch.server.backup.IndexArchiver.getIndexDataDir;
import static com.yelp.nrtsearch.server.backup.IndexArchiver.getIndexStateDir;
import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IndexArchiverTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private final String BUCKET_NAME = "indexarchiver-unittest";
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
  public void download() throws IOException {
    IndexArchiver indexArchiver =
        new IndexArchiver(
            new BackupDiffManager(
                backupTestHelper.getContentDownloaderNoTar(),
                backupTestHelper.getFileCompressAndUploaderWithNoTar(),
                backupTestHelper.getVersionManager(),
                backupTestHelper.getArchiverDirectory()),
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            backupTestHelper.getContentDownloaderTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    Map<String, String> filesAndContents = Map.of("file1", "testcontent1", "file2", "testcontent2");
    backupTestHelper.uploadBlessAndValidate(
        filesAndContents, indexArchiver, "testservice", "testresource");

    Path resourcePath = indexArchiver.download("testservice", "testresource");
    resourcePath = resourcePath.resolve("testresource");
    for (Path path : List.of(getIndexDataDir(resourcePath), getIndexStateDir(resourcePath))) {
      for (Map.Entry<String, String> entry : filesAndContents.entrySet()) {
        assertEquals(true, Files.exists(Paths.get(path.toString(), entry.getKey())));
        assertEquals(
            entry.getValue(), Files.readString(Paths.get(path.toString(), entry.getKey())));
      }
    }
  }

  @Test
  public void upload() throws IOException {
    IndexArchiver indexArchiver =
        new IndexArchiver(
            new BackupDiffManager(
                backupTestHelper.getContentDownloaderNoTar(),
                backupTestHelper.getFileCompressAndUploaderWithNoTar(),
                backupTestHelper.getVersionManager(),
                backupTestHelper.getArchiverDirectory()),
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            backupTestHelper.getContentDownloaderTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    backupTestHelper.uploadBlessAndValidate(
        Map.of("file1", "testcontent1", "file2", "testcontent2"),
        indexArchiver,
        "testservice",
        "testresource");
  }

  @Test
  public void uploadMetadata() throws IOException {
    IndexArchiver indexArchiver =
        new IndexArchiver(
            null,
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            null,
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    backupTestHelper.uploadBlessAndValidateMetadata(
        Map.of("indices1", "testcontent1", "indices2", "testcontent2"),
        indexArchiver,
        "testservice",
        "testresource_metadata");
  }

  @Test
  public void downloadMetadata() throws IOException {
    IndexArchiver indexArchiver =
        new IndexArchiver(
            null,
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            backupTestHelper.getContentDownloaderTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    Map<String, String> filesAndContents =
        Map.of("indices1", "testcontent1", "indices2", "testcontent2");
    backupTestHelper.uploadBlessAndValidateMetadata(
        filesAndContents, indexArchiver, "testservice", "testresource_metadata");
    Path resourcePath = indexArchiver.download("testservice", "testresource_metadata");
    Path path = IndexArchiver.getIndexStateDir(resourcePath);
    for (Map.Entry<String, String> entry : filesAndContents.entrySet()) {
      assertEquals(true, Files.exists(Paths.get(path.toString(), entry.getKey())));
      assertEquals(entry.getValue(), Files.readString(Paths.get(path.toString(), entry.getKey())));
    }
  }

  @Test
  public void deleteVersion() throws IOException {
    IndexArchiver indexArchiver =
        new IndexArchiver(
            new BackupDiffManager(
                backupTestHelper.getContentDownloaderNoTar(),
                backupTestHelper.getFileCompressAndUploaderWithNoTar(),
                backupTestHelper.getVersionManager(),
                backupTestHelper.getArchiverDirectory()),
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            backupTestHelper.getContentDownloaderTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    backupTestHelper.testDeleteVersion(indexArchiver);
  }

  @Test
  public void getResources() throws IOException {
    IndexArchiver indexArchiver =
        new IndexArchiver(
            new BackupDiffManager(
                backupTestHelper.getContentDownloaderNoTar(),
                backupTestHelper.getFileCompressAndUploaderWithNoTar(),
                backupTestHelper.getVersionManager(),
                backupTestHelper.getArchiverDirectory()),
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            backupTestHelper.getContentDownloaderTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    backupTestHelper.testGetResources(indexArchiver);
  }

  @Test
  public void getVersionedResource() throws IOException {
    IndexArchiver indexArchiver =
        new IndexArchiver(
            new BackupDiffManager(
                backupTestHelper.getContentDownloaderNoTar(),
                backupTestHelper.getFileCompressAndUploaderWithNoTar(),
                backupTestHelper.getVersionManager(),
                backupTestHelper.getArchiverDirectory()),
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            backupTestHelper.getContentDownloaderTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    backupTestHelper.testGetVersionedResources(indexArchiver);
  }

  @Test
  public void validGlobalStateDir() throws IOException {
    IndexArchiver indexArchiver = new IndexArchiver();
    Files.createFile(backupTestHelper.getArchiverDirectory().resolve("indices.0"));
    assertTrue(indexArchiver.validGlobalStateDir(backupTestHelper.getArchiverDirectory()));
  }

  @Test
  public void invalidGlobalStateDir() throws IOException {
    IndexArchiver indexArchiver = new IndexArchiver();
    Files.createFile(backupTestHelper.getArchiverDirectory().resolve("bad_apple.0"));
    assertEquals(false, indexArchiver.validGlobalStateDir(backupTestHelper.getArchiverDirectory()));
  }

  @Test
  public void validIndexDir() throws IOException {
    IndexArchiver indexArchiver = new IndexArchiver();
    createValidIndexDir();
    assertEquals(true, indexArchiver.validIndexDir(backupTestHelper.getArchiverDirectory()));
  }

  @Test
  public void invalidIndexDirNoData() throws IOException {
    IndexArchiver indexArchiver = new IndexArchiver();
    createValidIndexDir();
    Files.delete(
        backupTestHelper
            .getArchiverDirectory()
            .resolve(IndexArchiver.SHARD_0)
            .resolve(IndexArchiver.INDEX));
    assertEquals(false, indexArchiver.validIndexDir(backupTestHelper.getArchiverDirectory()));
  }

  @Test
  public void invalidIndexDirNoState() throws IOException {
    IndexArchiver indexArchiver = new IndexArchiver();
    createValidIndexDir();
    Files.delete(backupTestHelper.getArchiverDirectory().resolve(IndexArchiver.STATE));
    assertEquals(false, indexArchiver.validIndexDir(backupTestHelper.getArchiverDirectory()));
  }

  private void createValidIndexDir() throws IOException {
    Path indexStateDir = backupTestHelper.getArchiverDirectory().resolve(IndexArchiver.STATE);
    Path shardDir = backupTestHelper.getArchiverDirectory().resolve(IndexArchiver.SHARD_0);
    Path indexDataDir = shardDir.resolve(IndexArchiver.INDEX);
    Files.createDirectories(indexDataDir);
    Files.createDirectory(indexStateDir);
  }
}
