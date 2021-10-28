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
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.util.IOUtils;
import io.findify.s3mock.S3Mock;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileCompressAndUploaderTest {
  private final String BUCKET_NAME = "content-uploader-unittest";
  private FileCompressAndUploader fileCompressAndUploader1;
  private FileCompressAndUploader fileCompressAndUploader2;
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

    fileCompressAndUploader1 =
        new FileCompressAndUploader(
            new TarImpl(TarImpl.CompressionMode.LZ4), transferManager, BUCKET_NAME);
    fileCompressAndUploader2 =
        new FileCompressAndUploader(new NoTarImpl(), transferManager, BUCKET_NAME);
  }

  @After
  public void teardown() {
    api.shutdown();
  }

  @Test
  public void uploadWithTarImpl() throws IOException {
    Path indexDir = Files.createDirectory(archiverDirectory.resolve("testIndex"));
    Path indexFile = Paths.get(indexDir.toString(), "file1");
    Files.writeString(indexFile, "testcontent");
    fileCompressAndUploader1.upload("testservice", "testresource", "file1", indexDir);
    testUpload("testservice", "testresource", "file1", "testcontent");
  }

  @Test
  public void uploadWithNoTarImpl() throws IOException {
    Path indexDir = Files.createDirectory(archiverDirectory.resolve("testIndex"));
    Path indexFile = Paths.get(indexDir.toString(), "file1");
    Files.writeString(indexFile, "abcdef");
    fileCompressAndUploader2.upload("testservice", "testresource", "file1", indexDir);
    S3Object s3Object = s3.getObject(BUCKET_NAME, "testservice/testresource/file1");
    assertEquals("abcdef", IOUtils.toString(s3Object.getObjectContent()));
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
          "testcontent", new String(Files.readAllBytes(actualDownloadDir.resolve(fileName))));
    }
  }
}
