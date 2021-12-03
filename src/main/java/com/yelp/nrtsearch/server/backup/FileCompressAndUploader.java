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

import com.amazonaws.services.s3.transfer.TransferManager;
import com.google.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileCompressAndUploader {
  private static final Logger logger = LoggerFactory.getLogger(FileCompressAndUploader.class);
  private static final int NUM_S3_THREADS = 20;
  private final ThreadPoolExecutor executor =
      (ThreadPoolExecutor) Executors.newFixedThreadPool(NUM_S3_THREADS);
  private final Tar tar;
  private final TransferManager transferManager;
  private final String bucketName;

  @Inject
  public FileCompressAndUploader(Tar tar, TransferManager transferManager, String bucketName) {
    this.tar = tar;
    this.transferManager = transferManager;
    this.bucketName = bucketName;
  }

  /**
   * @param serviceName name of the service as stored on s3 key namespace
   * @param resource name of the resource as stored on s3 key namespace
   * @param fileName file that will be compressed and uploaded to s3 if uploadEntireDir is false
   *     else versionHash that completes the s3 key {serviceName/resourceName/fileName}
   * @param sourceDir Directory path containing the fileName
   * @param uploadEntireDir boolean to indicate if all files under sourceDir need to be uploaded
   *     instead of just fileName Uploading entire dir is only possible when using TarImpl
   * @throws IOException
   */
  public void upload(
      final String serviceName,
      final String resource,
      final String fileName,
      Path sourceDir,
      boolean uploadEntireDir)
      throws IOException {
    if (!Files.exists(sourceDir)) {
      throw new IOException(
          String.format(
              "Source directory %s, for service %s, and resource %s does not exist",
              sourceDir, serviceName, resource));
    }
    uploadAsStream(serviceName, resource, fileName, sourceDir, uploadEntireDir);
  }

  private void uploadAsStream(
      final String serviceName,
      final String resource,
      final String fileName,
      Path sourceDir,
      boolean uploadEntireDir)
      throws IOException {
    final String absoluteResourcePath = String.format("%s/%s/%s", serviceName, resource, fileName);
    long uncompressedSize = getTotalSize(sourceDir.toString(), fileName);
    logger.debug("Uploading: " + absoluteResourcePath);
    logger.debug("Uncompressed total size: " + uncompressedSize);
    TarUploadOutputStream uploadStream = null;
    try {
      uploadStream =
          new TarUploadOutputStream(
              bucketName,
              absoluteResourcePath,
              uncompressedSize,
              transferManager.getAmazonS3Client(),
              executor);
      if (!uploadEntireDir) {
        tar.buildTar(
            Paths.get(sourceDir.toString(), fileName),
            uploadStream,
            Collections.emptyList(),
            Collections.emptyList());
      } else {
        if (!(tar instanceof TarImpl)) {
          uploadStream = null;
          throw new IllegalStateException("Need to use TarImpl to upload entire directory to s3");
        }
        tar.buildTar(sourceDir, uploadStream, Collections.emptyList(), Collections.emptyList());
      }
      uploadStream.complete();
    } catch (IOException e) {
      if (uploadStream != null) {
        uploadStream.cancel();
      }
      throw new IOException("Error uploading tar to s3", e);
    }
  }

  private long getTotalSize(String filePath, String fileName) {
    File file = new File(Paths.get(filePath, fileName).toString());
    return file.length();
  }
}
