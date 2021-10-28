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

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import io.findify.s3mock.S3Mock;
import java.io.IOException;
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
  private final String BUCKET_NAME = "content-downloader-unittest";
  private ContentDownloader contentDownloaderTar;
  private ContentDownloader contentDownloaderNoTar;
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

    contentDownloaderTar =
        new ContentDownloaderImpl(
            new TarImpl(TarImpl.CompressionMode.LZ4),
            transferManager,
            BUCKET_NAME,
            downloadAsStream);
    contentDownloaderNoTar =
        new ContentDownloaderImpl(new NoTarImpl(), transferManager, BUCKET_NAME, downloadAsStream);
  }

  @After
  public void teardown() {
    api.shutdown();
  }

  @Test
  public void getVersionContentTar() throws IOException {
    final TarEntry tarEntry = new TarEntry("file1", "testcontent");
    TarEntry.uploadToS3(
        s3, BUCKET_NAME, Arrays.asList(tarEntry), "testservice/testresource/abcdef");
    Path downloadDir =
        archiverDirectory.resolve("downloads"); // this dir will be created by getVersionContent
    assertEquals(
        downloadDir,
        contentDownloaderTar.getVersionContent(
            "testservice", "testresource", "abcdef", downloadDir));
    assertEquals(
        "testcontent", new String(Files.readAllBytes(Paths.get(downloadDir.toString(), "file1"))));
  }

  @Test
  public void getVersionContentNoTar() throws IOException {
    s3.putObject(BUCKET_NAME, "testservice/testresource/abcdef", "testcontent");
    Path downloadDir =
        archiverDirectory.resolve("downloads"); // this dir will be created by getVersionContent
    assertEquals(
        downloadDir,
        contentDownloaderNoTar.getVersionContent(
            "testservice", "testresource", "abcdef", downloadDir));
    assertEquals(
        "testcontent", new String(Files.readAllBytes(Paths.get(downloadDir.toString(), "abcdef"))));
  }

  @Test
  public void getVersionContentMultipleFiles() throws IOException {
    final TarEntry tarEntry1 = new TarEntry("file1", "testcontent1");
    final TarEntry tarEntry2 = new TarEntry("file2", "testcontent2");
    TarEntry.uploadToS3(
        s3, BUCKET_NAME, Arrays.asList(tarEntry1, tarEntry2), "testservice/testresource/abcdef");
    Path downloadDir =
        archiverDirectory.resolve("downloads"); // this dir will be created by getVersionContent
    assertEquals(
        downloadDir,
        contentDownloaderTar.getVersionContent(
            "testservice", "testresource", "abcdef", downloadDir));
    assertEquals(
        "testcontent1", new String(Files.readAllBytes(Paths.get(downloadDir.toString(), "file1"))));
    assertEquals(
        "testcontent2", new String(Files.readAllBytes(Paths.get(downloadDir.toString(), "file2"))));
  }

  @Test
  public void getVersionContentdownloadDirExists() throws IOException {
    final TarEntry tarEntry = new TarEntry("file1", "testcontent");
    TarEntry.uploadToS3(
        s3, BUCKET_NAME, Arrays.asList(tarEntry), "testservice/testresource/abcdef");
    Path downloadDir = archiverDirectory.resolve("downloads");
    Files.createDirectories(downloadDir);
    assertEquals(
        downloadDir,
        contentDownloaderTar.getVersionContent(
            "testservice", "testresource", "abcdef", downloadDir));
  }

  @Test
  public void getS3Client() {
    assertEquals(s3, contentDownloaderTar.getS3Client());
  }

  @Test
  public void getBucketName() {
    assertEquals(BUCKET_NAME, contentDownloaderTar.getBucketName());
  }

  @Test
  public void downloadAsStream() {
    assertEquals(downloadAsStream, contentDownloaderTar.downloadAsStream());
  }
}
