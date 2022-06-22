/*
 * Copyright 2020 Yelp Inc.
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

import static com.yelp.nrtsearch.server.grpc.GrpcServer.rmDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import io.findify.s3mock.S3Mock;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class ArchiverTest {
  private final String BUCKET_NAME = "archiver-unittest";
  private Archiver archiver;
  private S3Mock api;
  private AmazonS3 s3;
  private Path s3Directory;
  private Path archiverDirectory;

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
    s3.putObject(BUCKET_NAME, "testservice/_version/testresource/_latest_version", "1");
    s3.putObject(BUCKET_NAME, "testservice/_version/testresource/1", "abcdef");

    archiver =
        new ArchiverImpl(
            s3, BUCKET_NAME, archiverDirectory, new TarImpl(TarImpl.CompressionMode.LZ4));
  }

  @After
  public void teardown() {
    api.shutdown();
  }

  @Test
  public void testDownload() throws IOException {
    final TarEntry tarEntry = new TarEntry("foo", "testcontent");
    TarEntry.uploadToS3(
        s3, BUCKET_NAME, Arrays.asList(tarEntry), "testservice/testresource/abcdef");

    final Path location = archiver.download("testservice", "testresource");
    final List<String> allLines = Files.readAllLines(location.resolve("foo"));

    assertEquals(1, allLines.size());
    assertEquals("testcontent", allLines.get(0));
  }

  @Test
  public void testUpload() throws IOException {
    upload(false);
  }

  @Test
  public void testUploadStream() throws IOException {
    upload(true);
  }

  private void upload(boolean stream) throws IOException {
    String service = "testservice";
    String resource = "testresource";
    Path sourceDir = createDirWithFiles(service, resource);
    String subDirPath = sourceDir.resolve("subDir").toString();

    testUploadWithParameters(service, resource, sourceDir, List.of(), List.of(), List.of(), stream);
    testUploadWithParameters(
        service,
        resource,
        sourceDir,
        List.of("test1"),
        List.of(),
        List.of("test2", "subDir"),
        stream);
    testUploadWithParameters(
        service, resource, sourceDir, List.of(), List.of(subDirPath), List.of("test1"), stream);
    testUploadWithParameters(
        service, resource, sourceDir, List.of("test1"), List.of(subDirPath), List.of(), stream);
  }

  private void testUploadWithParameters(
      String service,
      String resource,
      Path sourceDir,
      List<String> includeFiles,
      List<String> includeDirs,
      List<String> ignoreVerifying,
      boolean stream)
      throws IOException {
    String versionHash =
        archiver.upload(service, resource, sourceDir, includeFiles, includeDirs, stream);

    Path actualDownloadDir = Files.createDirectory(archiverDirectory.resolve("actualDownload"));
    try (S3Object s3Object =
            s3.getObject(BUCKET_NAME, String.format("%s/%s/%s", service, resource, versionHash));
        S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
        LZ4FrameInputStream lz4CompressorInputStream =
            new LZ4FrameInputStream(s3ObjectInputStream);
        TarArchiveInputStream tarArchiveInputStream =
            new TarArchiveInputStream(lz4CompressorInputStream)) {
      new TarImpl(TarImpl.CompressionMode.LZ4).extractTar(tarArchiveInputStream, actualDownloadDir);
    }
    assertTrue(
        TarImplTest.dirsMatch(
            actualDownloadDir.resolve(resource).toFile(), sourceDir.toFile(), ignoreVerifying));

    rmDir(actualDownloadDir);
  }

  @Test
  public void testUploadDownload() throws IOException {
    uploadDownload(false);
  }

  @Test
  public void testUploadDownloadStream() throws IOException {
    uploadDownload(true);
  }

  private void uploadDownload(boolean stream) throws IOException {
    String service = "testservice";
    String resource = "testresource";
    Path sourceDir = createDirWithFiles(service, resource);
    String versionHash =
        archiver.upload(service, resource, sourceDir, List.of(), List.of(), stream);
    archiver.blessVersion(service, resource, versionHash);
    Path downloadPath = archiver.download(service, resource);
    Path parentPath = downloadPath.getParent();
    Path path = parentPath.resolve(versionHash);
    assertTrue(path.toFile().exists());
  }

  private Path createDirWithFiles(String service, String resource) throws IOException {
    Path serviceDir = Files.createDirectory(archiverDirectory.resolve(service));
    Path resourceDir = Files.createDirectory(serviceDir.resolve(resource));
    Path subDir = Files.createDirectory(resourceDir.resolve("subDir"));
    try (ByteArrayInputStream test1content = new ByteArrayInputStream("test1content".getBytes());
        ByteArrayInputStream test2content = new ByteArrayInputStream("test2content".getBytes());
        FileOutputStream fileOutputStream1 =
            new FileOutputStream(resourceDir.resolve("test1").toFile());
        FileOutputStream fileOutputStream2 =
            new FileOutputStream(subDir.resolve("test2").toFile()); ) {
      IOUtils.copy(test1content, fileOutputStream1);
      IOUtils.copy(test2content, fileOutputStream2);
    }
    return resourceDir;
  }

  @Test
  public void testCleanup() throws IOException {
    final Path dontDeleteThisDirectory =
        Files.createDirectory(archiverDirectory.resolve("somerandomsubdirectory"));

    final TarEntry tarEntry = new TarEntry("testresource/foo", "testcontent");
    TarEntry.uploadToS3(
        s3, BUCKET_NAME, Arrays.asList(tarEntry), "testservice/testresource/abcdef");
    TarEntry.uploadToS3(s3, BUCKET_NAME, Arrays.asList(tarEntry), "testservice/testresource/cafe");

    final Path firstLocation = archiver.download("testservice", "testresource").toRealPath();
    Assert.assertTrue(Files.exists(firstLocation.resolve("testresource/foo")));

    s3.putObject(BUCKET_NAME, "testservice/_version/testresource/1", "cafe");

    final Path secondLocation = archiver.download("testservice", "testresource").toRealPath();

    assertFalse(Files.exists(firstLocation.resolve("testresource/foo")));
    Assert.assertTrue(Files.exists(secondLocation.resolve("testresource/foo")));
    Assert.assertTrue(Files.exists(dontDeleteThisDirectory));
  }

  @Test
  public void testGetResources() throws IOException {
    getResources(false);
  }

  @Test
  public void testGetResourcesStream() throws IOException {
    getResources(true);
  }

  private void getResources(boolean stream) throws IOException {
    String service = "testservice";
    String[] resources = new String[] {"testresource"};
    Path sourceDir = createDirWithFiles(service, resources[0]);
    String versionHash =
        archiver.upload(service, resources[0], sourceDir, List.of(), List.of(), stream);
    archiver.blessVersion(service, resources[0], versionHash);
    List<String> actualResources = archiver.getResources(service);
    String[] actual = actualResources.toArray(new String[0]);
    Assert.assertArrayEquals(resources, actual);
  }

  @Test
  public void testGetVersionedResource() throws IOException {
    getVersionedResource(false);
  }

  @Test
  public void testGetVersionedResourceStream() throws IOException {
    getVersionedResource(true);
  }

  private void getVersionedResource(boolean stream) throws IOException {
    String service = "testservice";
    String resource = "testresource";
    Path sourceDir = createDirWithFiles(service, resource);
    String versionHash1 =
        archiver.upload(service, resource, sourceDir, List.of(), List.of(), stream);
    String versionHash2 =
        archiver.upload(service, resource, sourceDir, List.of(), List.of(), stream);
    List<VersionedResource> actualResources = archiver.getVersionedResource(service, resource);

    List<String> versionHashes =
        actualResources.stream()
            .map(VersionedResource::getVersionHash)
            .collect(Collectors.toList());

    Assert.assertTrue(versionHashes.contains(versionHash1));
    Assert.assertTrue(versionHashes.contains(versionHash2));
    Assert.assertEquals(2, versionHashes.size());
  }

  @Test
  public void testDeleteVersion() throws IOException {
    deleteVersion(false);
  }

  @Test
  public void testDeleteVersionStream() throws IOException {
    deleteVersion(true);
  }

  private void deleteVersion(boolean stream) throws IOException {
    String service = "testservice";
    String resource = "testresource";
    Path sourceDir = createDirWithFiles(service, resource);
    String versionHash1 =
        archiver.upload(service, resource, sourceDir, List.of(), List.of(), stream);
    String versionHash2 =
        archiver.upload(service, resource, sourceDir, List.of(), List.of(), stream);

    archiver.deleteVersion(service, resource, versionHash1);

    List<VersionedResource> actualResources = archiver.getVersionedResource(service, resource);

    List<String> versionHashes =
        actualResources.stream()
            .map(VersionedResource::getVersionHash)
            .collect(Collectors.toList());

    assertFalse(versionHashes.contains(versionHash1));
    Assert.assertTrue(versionHashes.contains(versionHash2));
    Assert.assertEquals(1, versionHashes.size());
  }

  @Test
  public void testDeleteLocalFiles() throws IOException {
    createLocalFiles("resource1", 3, true);
    createLocalFiles("resource2", 1, true);
    createLocalFiles("resource3", 5, true);

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
    createLocalFiles("resource1", 3, false);
    createLocalFiles("resource2", 1, false);
    createLocalFiles("resource3", 5, false);

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
    createLocalFiles("resource1", 3, true);
    assertTrue(archiver.deleteLocalFiles("resource2"));
    assertTrue(archiverDirectory.resolve("resource1").toFile().exists());
  }

  @Test
  public void testDeleteLocalFiles_notDirectory() throws IOException {
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

  private void createLocalFiles(String resourceName, int numVersions, boolean withCurrent)
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
