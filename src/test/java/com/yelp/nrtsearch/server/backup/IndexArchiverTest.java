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
  private BackupHelper backupHelper;

  @Before
  public void setup() throws IOException {
    backupHelper = new BackupHelper(BUCKET_NAME, folder);
  }

  @After
  public void teardown() {
    backupHelper.shutdown();
  }

  @Test
  public void download() throws IOException {
    IndexArchiver indexArchiver =
        new IndexArchiver(
            new BackupDiffManager(
                backupHelper.getContentDownloaderNoTar(),
                backupHelper.getFileCompressAndUploaderWithNoTar(),
                backupHelper.getVersionManager(),
                backupHelper.getArchiverDirectory()),
            backupHelper.getFileCompressAndUploaderWithTar(),
            backupHelper.getContentDownloaderTar(),
            backupHelper.getVersionManager(),
            backupHelper.getArchiverDirectory());
    Map<String, String> filesAndContents = Map.of("file1", "testcontent1", "file2", "testcontent2");
    backupHelper.uploadBlessAndValidate(
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
                backupHelper.getContentDownloaderNoTar(),
                backupHelper.getFileCompressAndUploaderWithNoTar(),
                backupHelper.getVersionManager(),
                backupHelper.getArchiverDirectory()),
            backupHelper.getFileCompressAndUploaderWithTar(),
            backupHelper.getContentDownloaderTar(),
            backupHelper.getVersionManager(),
            backupHelper.getArchiverDirectory());
    backupHelper.uploadBlessAndValidate(
        Map.of("file1", "testcontent1", "file2", "testcontent2"),
        indexArchiver,
        "testservice",
        "testresource");
  }

  @Test
  public void deleteVersion() throws IOException {
    IndexArchiver indexArchiver =
        new IndexArchiver(
            new BackupDiffManager(
                backupHelper.getContentDownloaderNoTar(),
                backupHelper.getFileCompressAndUploaderWithNoTar(),
                backupHelper.getVersionManager(),
                backupHelper.getArchiverDirectory()),
            backupHelper.getFileCompressAndUploaderWithTar(),
            backupHelper.getContentDownloaderTar(),
            backupHelper.getVersionManager(),
            backupHelper.getArchiverDirectory());
    backupHelper.testDeleteVersion(indexArchiver);
  }

  @Test
  public void getResources() throws IOException {
    IndexArchiver indexArchiver =
        new IndexArchiver(
            new BackupDiffManager(
                backupHelper.getContentDownloaderNoTar(),
                backupHelper.getFileCompressAndUploaderWithNoTar(),
                backupHelper.getVersionManager(),
                backupHelper.getArchiverDirectory()),
            backupHelper.getFileCompressAndUploaderWithTar(),
            backupHelper.getContentDownloaderTar(),
            backupHelper.getVersionManager(),
            backupHelper.getArchiverDirectory());
    backupHelper.testGetResources(indexArchiver);
  }

  @Test
  public void getVersionedResource() throws IOException {
    IndexArchiver indexArchiver =
        new IndexArchiver(
            new BackupDiffManager(
                backupHelper.getContentDownloaderNoTar(),
                backupHelper.getFileCompressAndUploaderWithNoTar(),
                backupHelper.getVersionManager(),
                backupHelper.getArchiverDirectory()),
            backupHelper.getFileCompressAndUploaderWithTar(),
            backupHelper.getContentDownloaderTar(),
            backupHelper.getVersionManager(),
            backupHelper.getArchiverDirectory());
    backupHelper.testGetVersionedResources(indexArchiver);
  }

  @Test
  public void validGlobalStateDir() throws IOException {
    IndexArchiver indexArchiver = new IndexArchiver();
    Files.createFile(backupHelper.getArchiverDirectory().resolve("indices.0"));
    assertEquals(true, indexArchiver.validGlobalStateDir(backupHelper.getArchiverDirectory()));
  }

  @Test
  public void invalidGlobalStateDir() throws IOException {
    IndexArchiver indexArchiver = new IndexArchiver();
    Files.createFile(backupHelper.getArchiverDirectory().resolve("bad_apple.0"));
    assertEquals(false, indexArchiver.validGlobalStateDir(backupHelper.getArchiverDirectory()));
  }

  @Test
  public void validIndexDir() throws IOException {
    IndexArchiver indexArchiver = new IndexArchiver();
    createValidIndexDir();
    assertEquals(true, indexArchiver.validIndexDir(backupHelper.getArchiverDirectory()));
  }

  @Test
  public void invalidIndexDirNoData() throws IOException {
    IndexArchiver indexArchiver = new IndexArchiver();
    createValidIndexDir();
    Files.delete(
        backupHelper
            .getArchiverDirectory()
            .resolve(IndexArchiver.SHARD_0)
            .resolve(IndexArchiver.INDEX));
    assertEquals(false, indexArchiver.validIndexDir(backupHelper.getArchiverDirectory()));
  }

  @Test
  public void invalidIndexDirNoState() throws IOException {
    IndexArchiver indexArchiver = new IndexArchiver();
    createValidIndexDir();
    Files.delete(backupHelper.getArchiverDirectory().resolve(IndexArchiver.STATE));
    assertEquals(false, indexArchiver.validIndexDir(backupHelper.getArchiverDirectory()));
  }

  private void createValidIndexDir() throws IOException {
    Path indexStateDir = backupHelper.getArchiverDirectory().resolve(IndexArchiver.STATE);
    Path shardDir = backupHelper.getArchiverDirectory().resolve(IndexArchiver.SHARD_0);
    Path indexDataDir = shardDir.resolve(IndexArchiver.INDEX);
    Files.createDirectories(indexDataDir);
    Files.createDirectory(indexStateDir);
  }

  private void create() throws IOException {}
}
