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

import static com.yelp.nrtsearch.server.backup.IndexArchiver.getIndexDataResourceName;
import static org.junit.Assert.assertEquals;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.util.IOUtils;
import io.findify.s3mock.S3Mock;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

public class BackupTestHelper {
  private final String bucketName;
  private final ContentDownloaderImpl contentDownloaderTar;
  private final ContentDownloaderImpl contentDownloaderNoTar;
  private final FileCompressAndUploader fileCompressAndUploaderWithTar;
  private final FileCompressAndUploader fileCompressAndUploaderWithNoTar;
  private final VersionManager versionManager;
  private S3Mock api;
  private final AmazonS3 s3;

  public Path getArchiverDirectory() {
    return archiverDirectory;
  }

  private final Path archiverDirectory;

  private final Path s3Directory;

  public BackupTestHelper(String bucketName, TemporaryFolder folder) throws IOException {
    this(bucketName, folder, true);
  }

  public BackupTestHelper(String bucketName, TemporaryFolder folder, boolean createArchiverDir)
      throws IOException {
    this.bucketName = bucketName;
    s3Directory = folder.newFolder("s3").toPath();
    if (createArchiverDir) {
      archiverDirectory = folder.newFolder("archiver").toPath();
    } else {
      archiverDirectory = folder.getRoot().toPath().resolve("archiver");
    }

    api = S3Mock.create(8011, s3Directory.toAbsolutePath().toString());
    api.start();
    s3 = new AmazonS3Client(new AnonymousAWSCredentials());
    s3.setEndpoint("http://127.0.0.1:8011");
    s3.createBucket(bucketName);
    TransferManager transferManager =
        TransferManagerBuilder.standard().withS3Client(s3).withShutDownThreadPools(false).build();
    contentDownloaderTar =
        new ContentDownloaderImpl(
            new TarImpl(TarImpl.CompressionMode.LZ4), transferManager, bucketName, true);
    contentDownloaderNoTar =
        new ContentDownloaderImpl(new NoTarImpl(), transferManager, bucketName, true);
    fileCompressAndUploaderWithTar =
        new FileCompressAndUploader(
            new TarImpl(TarImpl.CompressionMode.LZ4), transferManager, bucketName);
    fileCompressAndUploaderWithNoTar =
        new FileCompressAndUploader(new NoTarImpl(), transferManager, bucketName);
    versionManager = new VersionManager(s3, bucketName);
  }

  public void shutdown() {
    api.shutdown();
  }

  public void testUpload(
      String service,
      String resource,
      String versionHash,
      Map<String, String> fileContents,
      Archiver archiver)
      throws IOException {
    Path actualDownloadDir = Files.createDirectory(archiverDirectory.resolve("actualDownload"));
    try (S3Object s3Object =
            s3.getObject(
                bucketName,
                getS3ResourceKey(
                    service,
                    resource,
                    versionHash,
                    archiver,
                    archiver != null && archiver instanceof IndexArchiver));
        S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
        LZ4FrameInputStream lz4CompressorInputStream =
            new LZ4FrameInputStream(s3ObjectInputStream);
        TarArchiveInputStream tarArchiveInputStream =
            new TarArchiveInputStream(lz4CompressorInputStream)) {
      new TarImpl(TarImpl.CompressionMode.LZ4).extractTar(tarArchiveInputStream, actualDownloadDir);
      if (Files.isDirectory(Files.list(actualDownloadDir).findFirst().get())) {
        actualDownloadDir = Files.list(actualDownloadDir).findFirst().get();
      }
      for (Map.Entry<String, String> entry : fileContents.entrySet()) {
        assertEquals(
            entry.getValue(),
            new String(Files.readAllBytes(actualDownloadDir.resolve(entry.getKey()))));
      }
    }
  }

  public void testUploadNoTar(
      String service, String resource, Map<String, String> fileContents, Archiver archiver)
      throws IOException {
    for (Map.Entry<String, String> entry : fileContents.entrySet()) {
      S3Object s3Object =
          s3.getObject(
              bucketName,
              getS3ResourceKey(
                  service,
                  resource,
                  entry.getKey(),
                  archiver,
                  archiver != null && archiver instanceof IndexArchiver));
      assertEquals(entry.getValue(), IOUtils.toString(s3Object.getObjectContent()));
    }
  }

  public ContentDownloaderImpl getContentDownloaderTar() {
    return contentDownloaderTar;
  }

  public ContentDownloaderImpl getContentDownloaderNoTar() {
    return contentDownloaderNoTar;
  }

  public FileCompressAndUploader getFileCompressAndUploaderWithTar() {
    return fileCompressAndUploaderWithTar;
  }

  public FileCompressAndUploader getFileCompressAndUploaderWithNoTar() {
    return fileCompressAndUploaderWithNoTar;
  }

  public VersionManager getVersionManager() {
    return versionManager;
  }

  public AmazonS3 getS3() {
    return s3;
  }

  public Path createIndexDir(Map<String, String> fileAndContents, Archiver archiver)
      throws IOException {
    Path indexDir = IndexArchiver.getIndexDataDir(archiverDirectory);
    if (!Files.exists(indexDir)) {
      Files.createDirectories(indexDir);
    }
    for (Map.Entry<String, String> entry : fileAndContents.entrySet()) {
      for (Path path : List.of(indexDir)) {
        Path file = Paths.get(path.toString(), entry.getKey());
        if (!Files.exists(file)) {
          Files.writeString(Files.createFile(file), entry.getValue(), StandardCharsets.UTF_8);
        }
      }
    }
    if (archiver != null && archiver instanceof IndexArchiver) {
      return archiverDirectory;
    } else {
      return indexDir;
    }
  }

  public String uploadBlessAndValidate(
      Map<String, String> filesAndContents, Archiver archiver, String service, String resource)
      throws IOException {
    Path indexDir = createIndexDir(filesAndContents, archiver);
    String versionHash =
        archiver.upload(
            service, resource, indexDir, filesAndContents.keySet(), Collections.emptyList(), true);
    assertEquals(true, archiver.blessVersion(service, resource, versionHash));
    // files are uploaded
    for (String file : filesAndContents.keySet()) {
      assertEquals(
          true,
          getS3()
              .doesObjectExist(
                  bucketName,
                  getS3ResourceKey(
                      service,
                      resource,
                      file,
                      archiver,
                      archiver != null && archiver instanceof IndexArchiver)));
    }
    // versionHash is uploaded
    assertEquals(
        true,
        getS3()
            .doesObjectExist(
                bucketName,
                getS3ResourceKey(
                    service,
                    resource,
                    versionHash,
                    archiver,
                    archiver != null && archiver instanceof IndexArchiver)));

    if (archiver != null && archiver instanceof IndexArchiver) {
      resource = IndexArchiver.getIndexDataResourceName(resource);
    }
    // latest versionHash exists
    assertEquals(
        true,
        getS3()
            .doesObjectExist(
                bucketName, String.format("%s/_version/%s/_latest_version", service, resource)));
    return versionHash;
  }

  private String getS3ResourceKey(
      String service, String resource, String file, Archiver archiver, boolean isIndexArchiver) {
    if (isIndexArchiver) {
      return String.format(
          "%s/%s/%s", service, IndexArchiver.getIndexDataResourceName(resource), file);
    } else {
      return String.format("%s/%s/%s", service, resource, file);
    }
  }

  public void testGetVersionedResources(Archiver archiver) throws IOException {
    String service = "testservice";
    String resource = "testresource";
    Map<String, String> filesAndContents = Map.of("file1", "contents1", "file2", "contents2");
    Path sourceDir = createIndexDir(filesAndContents, archiver);
    String versionHash1 =
        archiver.upload(service, resource, sourceDir, filesAndContents.keySet(), List.of(), true);
    String versionHash2 =
        archiver.upload(service, resource, sourceDir, filesAndContents.keySet(), List.of(), true);
    List<VersionedResource> actualResources = archiver.getVersionedResource(service, resource);

    List<String> versionHashes =
        actualResources.stream()
            .map(VersionedResource::getVersionHash)
            .collect(Collectors.toList());

    Assert.assertTrue(versionHashes.contains(versionHash1));
    Assert.assertTrue(versionHashes.contains(versionHash2));
    Assert.assertTrue(versionHashes.contains("file1"));
    Assert.assertTrue(versionHashes.contains("file2"));
    Assert.assertEquals(4, versionHashes.size());
  }

  public void testGetResources(Archiver archiver) throws IOException {
    String service = "testservice";
    String[] resources = new String[] {"testresource"};
    Map<String, String> filesAndContents = Map.of("file1", "contents1", "file2", "contents2");

    Path sourceDir = createIndexDir(filesAndContents, archiver);
    String versionHash =
        archiver.upload(service, resources[0], sourceDir, List.of(), List.of(), true);
    List<String> actualResources = archiver.getResources(service);
    String[] actual = actualResources.toArray(new String[0]);
    if (archiver != null && archiver instanceof IndexArchiver) {
      Assert.assertArrayEquals(new String[] {getIndexDataResourceName(resources[0])}, actual);
    } else {
      Assert.assertArrayEquals(resources, actual);
    }
  }

  public void testDeleteVersion(Archiver archiver) throws IOException {
    String service = "testservice";
    String resource = "testresource";
    Map<String, String> filesAndContents = Map.of("file1", "contents1", "file2", "contents2");

    Path sourceDir = createIndexDir(filesAndContents, archiver);

    String versionHash1 = archiver.upload(service, resource, sourceDir, List.of(), List.of(), true);
    String versionHash2 = archiver.upload(service, resource, sourceDir, List.of(), List.of(), true);

    archiver.deleteVersion(service, resource, versionHash1);

    List<VersionedResource> actualResources = archiver.getVersionedResource(service, resource);

    List<String> versionHashes =
        actualResources.stream()
            .map(VersionedResource::getVersionHash)
            .collect(Collectors.toList());

    Assert.assertFalse(versionHashes.contains(versionHash1));
    Assert.assertTrue(versionHashes.contains(versionHash2));
  }
}
