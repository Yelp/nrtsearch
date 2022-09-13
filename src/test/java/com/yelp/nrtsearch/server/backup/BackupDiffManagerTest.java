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
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class BackupDiffManagerTest {

  private BackupTestHelper backupTestHelper;

  public static class BackupDiffInfoTest {
    @Test
    public void getBackupDiffInfo() {
      Set<String> oldFileNames = Set.of("1", "2", "3", "4");
      Set<String> currentFileNames = Set.of("1", "4", "5");
      BackupDiffManager.BackupDiffInfo backupDiffInfo =
          BackupDiffManager.BackupDiffInfo.generateBackupDiffInfo(oldFileNames, currentFileNames);
      assertEquals(Set.of("5"), backupDiffInfo.getToBeAdded());
      assertEquals(Set.of("1", "4"), backupDiffInfo.getAlreadyUploaded());
      assertEquals(Set.of("2", "3"), backupDiffInfo.getToBeRemoved());
    }
  }

  public static class TempDirManagerTest {
    @Rule public final TemporaryFolder folder = new TemporaryFolder();
    private Path someTempDir;

    @Before
    public void setUp() throws IOException {
      someTempDir = folder.newFolder("some_temp_dir").toPath();
    }

    @Test
    public void tempDirCreateAndDelete() throws Exception {
      Path tempDir;
      try (BackupDiffManager.TempDirManager tempDirManager =
          new BackupDiffManager.TempDirManager(someTempDir)) {

        Path tempDirPath = Paths.get(tempDirManager.getPath().toString());
        Path someTempFile = Paths.get(tempDirPath.toString(), "someTempFile");
        assertEquals(false, Files.exists(tempDirPath));
        Files.createDirectory(tempDirPath);
        Files.createFile(someTempFile);
        assertEquals(true, Files.exists(tempDirPath));
        assertEquals(true, Files.exists(someTempFile));
        tempDir = tempDirManager.getPath();
      }
      assertEquals(false, Files.exists(tempDir));
    }

    @Test
    public void tempDirDelete() throws Exception {
      Path tempDir;
      try (BackupDiffManager.TempDirManager tempDirManager =
          new BackupDiffManager.TempDirManager(someTempDir)) {

        Path tempDirPath = Paths.get(tempDirManager.getPath().toString());
        assertEquals(false, Files.exists(tempDirPath));
        tempDir = tempDirManager.getPath();
      }
      assertEquals(false, Files.exists(tempDir));
    }
  }

  public static class BackupDiffMarshallerTest {
    @Rule public final TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testSerDe() throws IOException {
      List<String> indexFileNames = List.of("1", "2", "3");
      final Path tempFile = folder.newFile("tempFile.txt").toPath();
      BackupDiffManager.BackupDiffMarshaller.serializeFileNames(indexFileNames, tempFile);
      assertEquals(
          indexFileNames, BackupDiffManager.BackupDiffMarshaller.deserializeFileNames(tempFile));
    }
  }

  public static class BackupDiffDirValidatorTest {
    @Rule public final TemporaryFolder folder = new TemporaryFolder();

    @Test(expected = BackupDiffManager.BackupDiffDirValidator.InvalidMetaFileException.class)
    public void baseDirDoesNotExist() throws IOException {
      Path path = folder.newFolder().toPath().resolve("temp");
      BackupDiffManager.BackupDiffDirValidator.validateMetaFile(path);
    }

    @Test(expected = BackupDiffManager.BackupDiffDirValidator.InvalidMetaFileException.class)
    public void baseDirHasSubDir() throws IOException {
      Path path = folder.newFolder("subDir").toPath();
      Path parentDir = path.getParent();
      Files.createFile(Paths.get(parentDir.toString(), "file1"));
      BackupDiffManager.BackupDiffDirValidator.validateMetaFile(parentDir);
    }

    @Test(expected = BackupDiffManager.BackupDiffDirValidator.InvalidMetaFileException.class)
    public void baseDirHasMultipleFiles() throws IOException {
      File f1 = folder.newFile("file1");
      folder.newFile("file2");
      Path parentDir = f1.toPath().getParent();
      BackupDiffManager.BackupDiffDirValidator.validateMetaFile(parentDir);
    }

    @Test
    public void baseDirHasOneFile() throws IOException {
      Path f1 = folder.newFile("file1").toPath();
      Files.writeString(f1, "contents", StandardCharsets.UTF_8);
      Path parentDir = f1.getParent();
      Path fileName = BackupDiffManager.BackupDiffDirValidator.validateMetaFile(parentDir);
      assertEquals("contents", new String(Files.readAllBytes(fileName)));
    }
  }

  private final String BUCKET_NAME = "backupdiffmanager-unittest";
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    backupTestHelper = new BackupTestHelper(BUCKET_NAME, folder);
  }

  @After
  public void teardown() {
    backupTestHelper.shutdown();
  }

  @Test
  public void generateDiffNoPriorBackup() throws IOException {
    BackupDiffManager backupDiffManager =
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderTar(),
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    List<String> currentIndexFileNames = List.of("1", "2", "3");
    BackupDiffManager.BackupDiffInfo backupDiffInfo =
        backupDiffManager.generateDiff("testservice", "testresource", currentIndexFileNames);
    assertEquals(new HashSet(currentIndexFileNames), backupDiffInfo.getToBeAdded());
    assertEquals(true, backupDiffInfo.getAlreadyUploaded().isEmpty());
    assertEquals(true, backupDiffInfo.getToBeRemoved().isEmpty());
  }

  @Test
  public void generateDiffHasPriorBackup() throws IOException {
    BackupDiffManager backupDiffManager =
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderTar(),
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    backupTestHelper
        .getS3()
        .putObject(BUCKET_NAME, "testservice/_version/testresource/_latest_version", "1");
    backupTestHelper
        .getS3()
        .putObject(BUCKET_NAME, "testservice/_version/testresource/1", "abcdef");
    String contents =
        List.of("1", "2", "3").stream().map(String::valueOf).collect(Collectors.joining("\n"));
    final TarEntry tarEntry = new TarEntry("file1", contents);
    TarEntry.uploadToS3(
        backupTestHelper.getS3(),
        BUCKET_NAME,
        Arrays.asList(tarEntry),
        "testservice/testresource/abcdef");

    List<String> currentIndexFileNames = List.of("1", "4", "5");
    BackupDiffManager.BackupDiffInfo backupDiffInfo =
        backupDiffManager.generateDiff("testservice", "testresource", currentIndexFileNames);
    assertEquals(Set.of("4", "5"), backupDiffInfo.getToBeAdded());
    assertEquals(Set.of("1"), backupDiffInfo.getAlreadyUploaded());
    assertEquals(Set.of("2", "3"), backupDiffInfo.getToBeRemoved());
  }

  @Test
  public void uploadDiff() throws IOException {
    ImmutableSet<String> toBeAdded = ImmutableSet.of("1", "2");
    BackupDiffManager.BackupDiffInfo backupDiffInfo =
        new BackupDiffManager.BackupDiffInfo(ImmutableSet.of(), toBeAdded, ImmutableSet.of());
    BackupDiffManager backupDiffManager =
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderTar(),
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    String diffName = backupDiffManager.uploadDiff("testservice", "testresource", backupDiffInfo);
    backupTestHelper.testUpload(
        "testservice", "testresource", diffName, Map.of(diffName, "1\n2\n"), backupDiffManager);
  }

  @Test
  public void uploadDiffArchiveDirNotExist() throws IOException {
    backupTestHelper.shutdown();
    folder.delete();
    backupTestHelper = new BackupTestHelper(BUCKET_NAME, folder, false);

    ImmutableSet<String> toBeAdded = ImmutableSet.of("1", "2");
    BackupDiffManager.BackupDiffInfo backupDiffInfo =
        new BackupDiffManager.BackupDiffInfo(ImmutableSet.of(), toBeAdded, ImmutableSet.of());
    BackupDiffManager backupDiffManager =
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderTar(),
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    String diffName = backupDiffManager.uploadDiff("testservice", "testresource", backupDiffInfo);
    backupTestHelper.testUpload(
        "testservice", "testresource", diffName, Map.of(diffName, "1\n2\n"), backupDiffManager);
  }

  @Test
  public void uploadDiffNoTar() throws IOException {
    ImmutableSet<String> toBeAdded = ImmutableSet.of("1", "2");
    BackupDiffManager.BackupDiffInfo backupDiffInfo =
        new BackupDiffManager.BackupDiffInfo(ImmutableSet.of(), toBeAdded, ImmutableSet.of());
    BackupDiffManager backupDiffManager =
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderNoTar(),
            backupTestHelper.getFileCompressAndUploaderWithNoTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    String diffName = backupDiffManager.uploadDiff("testservice", "testresource", backupDiffInfo);
    backupTestHelper.testUploadNoTar(
        "testservice", "testresource", Map.of(diffName, "1\n2\n"), backupDiffManager);
  }

  @Test
  public void download() throws IOException {
    Map<String, String> filesAndContents = Map.of("file1", "contents1", "file2", "contents2");
    backupTestHelper.uploadBlessAndValidate(
        filesAndContents,
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderTar(),
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory()),
        "testservice",
        "testresource");
    BackupDiffManager backupDiffManager =
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderTar(),
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    Path downloadPath = backupDiffManager.download("testservice", "testresource");
    for (Map.Entry<String, String> entry : filesAndContents.entrySet()) {
      assertEquals(true, Files.exists(Paths.get(downloadPath.toString(), entry.getKey())));
      assertEquals(
          entry.getValue(), Files.readString(Paths.get(downloadPath.toString(), entry.getKey())));
    }
  }

  @Test
  public void downloadNoTar() throws IOException {
    Map<String, String> filesAndContents = Map.of("file1", "contents1", "file2", "contents2");
    BackupDiffManager backupDiffManager =
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderNoTar(),
            backupTestHelper.getFileCompressAndUploaderWithNoTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    backupTestHelper.uploadBlessAndValidate(
        filesAndContents, backupDiffManager, "testservice", "testresource");
    Path downloadPath = backupDiffManager.download("testservice", "testresource");
    for (Map.Entry<String, String> entry : filesAndContents.entrySet()) {
      assertEquals(true, Files.exists(Paths.get(downloadPath.toString(), entry.getKey())));
      assertEquals(
          entry.getValue(), Files.readString(Paths.get(downloadPath.toString(), entry.getKey())));
    }
  }

  @Test
  public void uploadNoPriorBackup() throws IOException {
    backupTestHelper.uploadBlessAndValidate(
        Map.of("file1", "contents1", "file2", "contents2"),
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderTar(),
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory()),
        "testservice",
        "testresource");
  }

  @Test
  public void uploadNoPriorBackupNoTar() throws IOException {
    backupTestHelper.uploadBlessAndValidate(
        Map.of("file1", "contents1", "file2", "contents2"),
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderNoTar(),
            backupTestHelper.getFileCompressAndUploaderWithNoTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory()),
        "testservice",
        "testresource");
  }

  @Test
  public void uploadWhenPriorBackup() throws IOException {
    // 1st backup
    BackupDiffManager backupDiffManager =
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderTar(),
            backupTestHelper.getFileCompressAndUploaderWithTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    validateBackups(backupDiffManager);
  }

  @Test
  public void uploadWhenPriorBackupNoTar() throws IOException {
    // 1st backup
    BackupDiffManager backupDiffManager =
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderNoTar(),
            backupTestHelper.getFileCompressAndUploaderWithNoTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory());
    validateBackups(backupDiffManager);
  }

  @Test
  public void testGetResources() throws IOException {
    backupTestHelper.testGetResources(
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderNoTar(),
            backupTestHelper.getFileCompressAndUploaderWithNoTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory()));
  }

  @Test
  public void testGetVersionedResource() throws IOException {
    backupTestHelper.testGetVersionedResources(
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderNoTar(),
            backupTestHelper.getFileCompressAndUploaderWithNoTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory()));
  }

  @Test
  public void deleteVersion() throws IOException {
    backupTestHelper.testDeleteVersion(
        new BackupDiffManager(
            backupTestHelper.getContentDownloaderNoTar(),
            backupTestHelper.getFileCompressAndUploaderWithNoTar(),
            backupTestHelper.getVersionManager(),
            backupTestHelper.getArchiverDirectory()));
  }

  private void validateBackups(BackupDiffManager backupDiffManager) throws IOException {
    String versionHashOld =
        backupTestHelper.uploadBlessAndValidate(
            Map.of("file1", "contents1", "file2", "contents2"),
            backupDiffManager,
            "testservice",
            "testresource");
    // Incremental Backup
    String versionHashNew =
        backupTestHelper.uploadBlessAndValidate(
            Map.of("file2", "contents2", "file3", "contents3"),
            backupDiffManager,
            "testservice",
            "testresource");
    // old backup file still exists
    assertEquals(
        true,
        backupTestHelper.getS3().doesObjectExist(BUCKET_NAME, "testservice/testresource/file1"));
    // confirm latest_version matches the most recent one
    assertEquals(
        "1",
        IOUtils.toString(
            backupTestHelper
                .getS3()
                .getObject(BUCKET_NAME, "testservice/_version/testresource/_latest_version/")
                .getObjectContent()));
    assertEquals(
        versionHashNew,
        IOUtils.toString(
            backupTestHelper
                .getS3()
                .getObject(BUCKET_NAME, "testservice/_version/testresource/1")
                .getObjectContent()));
  }
}
