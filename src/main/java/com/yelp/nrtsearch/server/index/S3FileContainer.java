/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.index;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import java.io.IOException;
import java.io.InputStream;

public class S3FileContainer implements RemoteFileContainer {
  private final AmazonS3 s3Client;
  private final RemoteFilePath prefix;
  private final String bucket;

  public S3FileContainer(AmazonS3 s3Client, RemoteFilePath prefix, String bucket) {
    this.s3Client = s3Client;
    this.prefix = prefix;
    this.bucket = bucket;
  }

  @Override
  public RemoteFilePath getPath() {
    return prefix;
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    String key = prefix.getPath() + name;
    try {
      return s3Client.doesObjectExist(bucket, key);
    } catch (Exception e) {
      throw new IOException("Error checking if file exists in S3: " + key, e);
    }
  }

  @Override
  public InputStream readFile(String name) throws IOException {
    String key = prefix.getPath() + name;
    try {
      S3Object s3Object = s3Client.getObject(bucket, key);
      return s3Object.getObjectContent();
    } catch (Exception e) {
      throw new IOException("Error reading file from S3: " + key, e);
    }
  }

  @Override
  public long fileLength(String name) throws IOException {
    String key = prefix.getPath() + name;
    try {
      return s3Client.getObjectMetadata(bucket, key).getContentLength();
    } catch (Exception e) {
      throw new IOException("Error getting file length from S3: " + key, e);
    }
  }
}
