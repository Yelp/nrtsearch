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
import static org.junit.Assert.*;

import com.amazonaws.util.IOUtils;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
    for (Path path : List.of(getIndexDataDir(resourcePath))) {
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

  private void createValidIndexDir() throws IOException {
    Path indexStateDir = backupTestHelper.getArchiverDirectory().resolve(IndexArchiver.STATE);
    Path shardDir = backupTestHelper.getArchiverDirectory().resolve(IndexArchiver.SHARD_0);
    Path indexDataDir = shardDir.resolve(IndexArchiver.INDEX);
    Files.createDirectories(indexDataDir);
    Files.createDirectory(indexStateDir);
  }

  @Test
  public void testDeleteLocalFiles() throws IOException {
    IndexArchiver archiver = getArchiverForLocalDelete();
    Path archiverDirectory = backupTestHelper.getArchiverDirectory();
    createLocalFiles("resource1", 3, true, archiverDirectory);
    createLocalFiles("resource2", 1, true, archiverDirectory);
    createLocalFiles("resource3", 5, true, archiverDirectory);

    assertTrue(archiver.deleteLocalFiles("resource2"));
    assertTrue(archiverDirectory.resolve("resource1").toFile().exists());
    assertFalse(archiverDirectory.resolve("resource2").toFile().exists());
    assertTrue(archiverDirectory.resolve("resource3").toFile().exists());

    assertTrue(archiver.deleteLocalFiles("resource3"));
    assertTrue(archiverDirectory.resolve("resource1").toFile().exists());
    assertFalse(archiverDirectory.resolve("resource2").toFile().exists());
    assertFalse(archiverDirectory.resolve("resource3").toFile().exists());

    assertTrue(archiver.deleteLocalFiles("resource1"));
    assertFalse(archiverDirectory.resolve("resource1").toFile().exists());
    assertFalse(archiverDirectory.resolve("resource2").toFile().exists());
    assertFalse(archiverDirectory.resolve("resource3").toFile().exists());
  }

  @Test
  public void testDeleteLocalFiles_noCurrent() throws IOException {
    IndexArchiver archiver = getArchiverForLocalDelete();
    Path archiverDirectory = backupTestHelper.getArchiverDirectory();
    createLocalFiles("resource1", 3, false, archiverDirectory);
    createLocalFiles("resource2", 1, false, archiverDirectory);
    createLocalFiles("resource3", 5, false, archiverDirectory);

    assertTrue(archiver.deleteLocalFiles("resource2"));
    assertTrue(archiverDirectory.resolve("resource1").toFile().exists());
    assertFalse(archiverDirectory.resolve("resource2").toFile().exists());
    assertTrue(archiverDirectory.resolve("resource3").toFile().exists());

    assertTrue(archiver.deleteLocalFiles("resource3"));
    assertTrue(archiverDirectory.resolve("resource1").toFile().exists());
    assertFalse(archiverDirectory.resolve("resource2").toFile().exists());
    assertFalse(archiverDirectory.resolve("resource3").toFile().exists());

    assertTrue(archiver.deleteLocalFiles("resource1"));
    assertFalse(archiverDirectory.resolve("resource1").toFile().exists());
    assertFalse(archiverDirectory.resolve("resource2").toFile().exists());
    assertFalse(archiverDirectory.resolve("resource3").toFile().exists());
  }

  @Test
  public void testDeleteLocalFiles_notExist() throws IOException {
    IndexArchiver archiver = getArchiverForLocalDelete();
    Path archiverDirectory = backupTestHelper.getArchiverDirectory();
    createLocalFiles("resource1", 3, true, archiverDirectory);
    assertTrue(archiver.deleteLocalFiles("resource2"));
    assertTrue(archiverDirectory.resolve("resource1").toFile().exists());
  }

  @Test
  public void testDeleteLocalFiles_notDirectory() throws IOException {
    IndexArchiver archiver = getArchiverForLocalDelete();
    Path archiverDirectory = backupTestHelper.getArchiverDirectory();
    try (ByteArrayInputStream test1content = new ByteArrayInputStream("test1content".getBytes());
        ByteArrayInputStream test2content = new ByteArrayInputStream("test2content".getBytes());
        FileOutputStream fileOutputStream1 =
            new FileOutputStream(archiverDirectory.resolve("resource1").toFile());
        FileOutputStream fileOutputStream2 =
            new FileOutputStream(archiverDirectory.resolve("resource2").toFile()); ) {
      IOUtils.copy(test1content, fileOutputStream1);
      IOUtils.copy(test2content, fileOutputStream2);
    }
    assertFalse(archiver.deleteLocalFiles("resource1"));
    assertTrue(Files.exists(archiverDirectory.resolve("resource1")));
    assertTrue(Files.exists(archiverDirectory.resolve("resource2")));
    assertFalse(archiver.deleteLocalFiles("resource2"));
    assertTrue(Files.exists(archiverDirectory.resolve("resource1")));
    assertTrue(Files.exists(archiverDirectory.resolve("resource2")));
  }

  private IndexArchiver getArchiverForLocalDelete() {
    return new IndexArchiver(
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderNoTar(),
            backupTestHelper.getFileCompressAndUploaderWithNoTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory()),
        backupTestHelper.getFileCompressAndUploaderWithTar(),
        backupTestHelper.getContentDownloaderTar(),
        backupTestHelper.getVersionManager(),
        backupTestHelper.getArchiverDirectory());
  }

  private void createLocalFiles(
      String resourceName, int numVersions, boolean withCurrent, Path archiverDirectory)
      throws IOException {
    assertTrue(numVersions > 0);
    Path resourceRoot = archiverDirectory.resolve(resourceName);
    Files.createDirectory(resourceRoot);
    String hash = null;
    for (int i = 0; i < numVersions; ++i) {
      hash = UUID.randomUUID().toString();
      Path versionRoot = resourceRoot.resolve(hash);
      Files.createDirectory(versionRoot);
      try (ByteArrayInputStream test1content = new ByteArrayInputStream("test1content".getBytes());
          ByteArrayInputStream test2content = new ByteArrayInputStream("test2content".getBytes());
          FileOutputStream fileOutputStream1 =
              new FileOutputStream(versionRoot.resolve("test1").toFile());
          FileOutputStream fileOutputStream2 =
              new FileOutputStream(versionRoot.resolve("test2").toFile()); ) {
        IOUtils.copy(test1content, fileOutputStream1);
        IOUtils.copy(test2content, fileOutputStream2);
      }
    }
    if (withCurrent) {
      Files.createSymbolicLink(resourceRoot.resolve("current"), resourceRoot.resolve(hash));
    }
  }
}
