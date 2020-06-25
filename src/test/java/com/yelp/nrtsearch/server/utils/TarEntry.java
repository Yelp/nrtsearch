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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

public class TarEntry {
  public final String path;
  public final String content;

  public TarEntry(String path, String content) {
    this.path = path;
    this.content = content;
  }

  public static void uploadToS3(
      AmazonS3 s3, String bucketName, List<TarEntry> tarEntries, String key) throws IOException {
    byte[] tarContent = getTarFile(tarEntries);
    final ObjectMetadata objectMetadata = new ObjectMetadata();

    try (final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(tarContent)) {
      s3.putObject(bucketName, key, byteArrayInputStream, objectMetadata);
    }
  }

  private static byte[] getTarFile(List<TarEntry> tarEntries) throws IOException {
    try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final LZ4FrameOutputStream lz4CompressorOutputStream =
            new LZ4FrameOutputStream(byteArrayOutputStream);
        final TarArchiveOutputStream tarArchiveOutputStream =
            new TarArchiveOutputStream(lz4CompressorOutputStream); ) {
      for (final TarEntry tarEntry : tarEntries) {
        final byte[] data = tarEntry.content.getBytes(StandardCharsets.UTF_8);
        final TarArchiveEntry archiveEntry = new TarArchiveEntry(tarEntry.path);
        archiveEntry.setSize(data.length);
        tarArchiveOutputStream.putArchiveEntry(archiveEntry);
        tarArchiveOutputStream.write(data);
        tarArchiveOutputStream.closeArchiveEntry();
      }

      tarArchiveOutputStream.close();
      return byteArrayOutputStream.toByteArray();
    }
  }
}
