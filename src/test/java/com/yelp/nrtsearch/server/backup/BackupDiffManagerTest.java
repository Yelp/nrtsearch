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

import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.util.IOUtils;
import com.google.common.collect.ImmutableSet;
import io.findify.s3mock.S3Mock;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BackupDiffManagerTest {

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

    @Test(expected = RuntimeException.class)
    public void baseDirDoesNotExist() throws IOException {
      Path path = folder.newFolder().toPath().resolve("temp");
      BackupDiffManager.BackupDiffDirValidator.validateMetaFile(path);
    }

    @Test(expected = RuntimeException.class)
    public void baseDirHasSubDir() throws IOException {
      Path path = folder.newFolder("subDir").toPath();
      Path parentDir = path.getParent();
      Files.createFile(Paths.get(parentDir.toString(), "file1"));
      BackupDiffManager.BackupDiffDirValidator.validateMetaFile(parentDir);
    }

    @Test(expected = RuntimeException.class)
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

  private final String BUCKET_NAME = "backup-diff-computer-unittest";
  private ContentDownloader contentDownloader;
  private FileCompressAndUploader fileCompressAndUploader;
  private VersionManager versionManager;
  private S3Mock api;
  private AmazonS3 s3;
  private Path s3Directory;
  private Path archiverDirectory;
  private boolean downloadAsStream = true;
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    s3Directory = folder.newFolder("s3").toPath();
    archiverDirectory = folder.newFolder("archiver").toPath();

    api = S3Mock.create(8011, s3Directory.toAbsolutePath().toString());
    api.start();
    s3 = new AmazonS3Client(new AnonymousAWSCredentials());
    s3.setEndpoint("http://127.0.0.1:8011");
    s3.createBucket(BUCKET_NAME);
    TransferManager transferManager =
        TransferManagerBuilder.standard().withS3Client(s3).withShutDownThreadPools(false).build();

    contentDownloader =
        new ContentDownloaderImpl(
            new TarImpl(TarImpl.CompressionMode.LZ4),
            transferManager,
            BUCKET_NAME,
            downloadAsStream);
    fileCompressAndUploader =
        new FileCompressAndUploader(
            new TarImpl(TarImpl.CompressionMode.LZ4), transferManager, BUCKET_NAME);
    versionManager = new VersionManager(s3, BUCKET_NAME);
  }

  @After
  public void teardown() {
    api.shutdown();
  }

  @Test
  public void generateDiffNoPriorBackup() throws IOException {
    BackupDiffManager backupDiffManager =
        new BackupDiffManager(
            contentDownloader, fileCompressAndUploader, versionManager, archiverDirectory);
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
            contentDownloader, fileCompressAndUploader, versionManager, archiverDirectory);
    s3.putObject(BUCKET_NAME, "testservice/_version/testresource/_latest_version", "1");
    s3.putObject(BUCKET_NAME, "testservice/_version/testresource/1", "abcdef");
    String contents =
        List.of("1", "2", "3").stream().map(String::valueOf).collect(Collectors.joining("\n"));
    final TarEntry tarEntry = new TarEntry("file1", contents);
    TarEntry.uploadToS3(
        s3, BUCKET_NAME, Arrays.asList(tarEntry), "testservice/testresource/abcdef");

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
            contentDownloader, fileCompressAndUploader, versionManager, archiverDirectory);
    String diffName = backupDiffManager.uploadDiff("testservice", "testresource", backupDiffInfo);
    testUpload("testservice", "testresource", diffName, "1\n2\n");
  }

  private Path getTmpDir() throws IOException {
    return Files.createDirectory(Paths.get(archiverDirectory.toString(), randomUUID().toString()));
  }

  @Test
  public void download() throws IOException {
    Map<String, String> filesAndContents = Map.of("file1", "contents1", "file2", "contents2");
    uploadBlessAndValidate(filesAndContents);
    BackupDiffManager backupDiffManager =
        new BackupDiffManager(
            contentDownloader, fileCompressAndUploader, versionManager, archiverDirectory);
    Path downloadPath = backupDiffManager.download("testservice", "testresource");
    for (Map.Entry<String, String> entry : filesAndContents.entrySet()) {
      assertEquals(true, Files.exists(Paths.get(downloadPath.toString(), entry.getKey())));
      assertEquals(
          entry.getValue(), Files.readString(Paths.get(downloadPath.toString(), entry.getKey())));
    }
  }

  @Test
  public void uploadNoPriorBackup() throws IOException {
    uploadBlessAndValidate(Map.of("file1", "contents1", "file2", "contents2"));
  }

  private String uploadBlessAndValidate(Map<String, String> filesAndContents) throws IOException {
    Path indexDir = createIndexDir(filesAndContents);
    BackupDiffManager backupDiffManager =
        new BackupDiffManager(
            contentDownloader, fileCompressAndUploader, versionManager, archiverDirectory);
    String versionHash =
        backupDiffManager.upload(
            "testservice",
            "testresource",
            indexDir,
            filesAndContents.keySet(),
            Collections.emptyList(),
            true);
    assertEquals(true, backupDiffManager.blessVersion("testservice", "testresource", versionHash));
    // files are uploaded
    for (String file : filesAndContents.keySet()) {
      assertEquals(
          true,
          s3.doesObjectExist(BUCKET_NAME, String.format("testservice/testresource/%s", file)));
    }
    // versionHash is uploaded
    assertEquals(
        true,
        s3.doesObjectExist(BUCKET_NAME, String.format("testservice/testresource/%s", versionHash)));
    // latest versionHash exists
    assertEquals(
        true, s3.doesObjectExist(BUCKET_NAME, "testservice/_version/testresource/_latest_version"));
    return versionHash;
  }

  @Test
  public void uploadWhenPriorBackup() throws IOException {
    // 1st backup
    String versionHashOld =
        uploadBlessAndValidate(Map.of("file1", "contents1", "file2", "contents2"));
    // Incremental Backup
    String versionHashNew =
        uploadBlessAndValidate(Map.of("file2", "contents2", "file3", "contents3"));
    // old backup file still exists
    assertEquals(true, s3.doesObjectExist(BUCKET_NAME, "testservice/testresource/file1"));
    // confirm latest_version matches the most recent one
    assertEquals(
        "1",
        IOUtils.toString(
            s3.getObject(
                    BUCKET_NAME,
                    String.format("testservice/_version/testresource/_latest_version/"))
                .getObjectContent()));
    assertEquals(
        versionHashNew,
        IOUtils.toString(
            s3.getObject(BUCKET_NAME, String.format("testservice/_version/testresource/1"))
                .getObjectContent()));
  }

  private Path createIndexDir(Map<String, String> fileAndContents) throws IOException {
    Path indexDir = archiverDirectory.resolve("index");
    if (!Files.exists(indexDir)) {
      Files.createDirectory(indexDir);
    }
    for (Map.Entry<String, String> entry : fileAndContents.entrySet()) {
      Path file = Paths.get(indexDir.toString(), entry.getKey());
      if (!Files.exists(file)) {
        Files.writeString(Files.createFile(file), entry.getValue(), StandardCharsets.UTF_8);
      }
    }
    return indexDir;
  }

  private void testUpload(String service, String resource, String fileName, String testContent)
      throws IOException {
    Path actualDownloadDir = Files.createDirectory(archiverDirectory.resolve("actualDownload"));
    try (S3Object s3Object =
            s3.getObject(BUCKET_NAME, String.format("%s/%s/%s", service, resource, fileName));
        S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
        LZ4FrameInputStream lz4CompressorInputStream =
            new LZ4FrameInputStream(s3ObjectInputStream);
        TarArchiveInputStream tarArchiveInputStream =
            new TarArchiveInputStream(lz4CompressorInputStream)) {
      new TarImpl(TarImpl.CompressionMode.LZ4).extractTar(tarArchiveInputStream, actualDownloadDir);
      assertEquals(
          testContent, new String(Files.readAllBytes(actualDownloadDir.resolve(fileName))));
    }
  }
}
