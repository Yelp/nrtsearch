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
package com.yelp.nrtsearch.server.utils;

import static org.junit.Assert.assertEquals;

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
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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
    String service = "testservice";
    String resource = "testresource";
    Path sourceDir = createDirWithFiles(service, resource);
    String versionHash = archiver.upload(service, resource, sourceDir);

    Path actualDownloadDir = Files.createDirectory(archiverDirectory.resolve("actualDownload"));
    try (S3Object s3Object =
            s3.getObject(BUCKET_NAME, String.format("%s/%s/%s", service, resource, versionHash));
        final S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
        final LZ4FrameInputStream lz4CompressorInputStream =
            new LZ4FrameInputStream(s3ObjectInputStream);
        final TarArchiveInputStream tarArchiveInputStream =
            new TarArchiveInputStream(lz4CompressorInputStream); ) {
      new TarImpl(TarImpl.CompressionMode.LZ4).extractTar(tarArchiveInputStream, actualDownloadDir);
    }
    assertEquals(
        true,
        TarImplTest.dirsMatch(actualDownloadDir.resolve(resource).toFile(), sourceDir.toFile()));
  }

  @Test
  public void testUploadDownload() throws IOException {
    String service = "testservice";
    String resource = "testresource";
    Path sourceDir = createDirWithFiles(service, resource);
    String versionHash = archiver.upload(service, resource, sourceDir);
    archiver.blessVersion(service, resource, versionHash);
    Path downloadPath = archiver.download(service, resource);
    Path parentPath = downloadPath.getParent();
    Path path = parentPath.resolve(versionHash);
    assertEquals(true, path.toFile().exists());
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

    Assert.assertFalse(Files.exists(firstLocation.resolve("testresource/foo")));
    Assert.assertTrue(Files.exists(secondLocation.resolve("testresource/foo")));
    Assert.assertTrue(Files.exists(dontDeleteThisDirectory));
  }

  @Test
  public void testGetResources() throws IOException {
    String service = "testservice";
    String[] resources = new String[] {"testresource"};
    Path sourceDir = createDirWithFiles(service, resources[0]);
    String versionHash = archiver.upload(service, resources[0], sourceDir);
    archiver.blessVersion(service, resources[0], versionHash);
    List<String> actualResources = archiver.getResources(service);
    String[] actual = actualResources.toArray(new String[0]);
    Assert.assertArrayEquals(resources, actual);
  }
}
