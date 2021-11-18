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

import com.amazonaws.services.s3.AmazonS3;
import java.io.IOException;
import java.nio.file.Path;

public interface ContentDownloader {
  /**
   * @param serviceName name of cluster or service
   * @param resource name of index or resource
   * @param hash name or hash of specific entity/file within namespace serviceName/resource
   * @param destDirectory local Path to download the serviceName/resource/hash to
   * @throws IOException
   */
  void getVersionContent(
      final String serviceName, final String resource, final String hash, final Path destDirectory)
      throws IOException;

  /** @return amazonS3 Client used by this ContentDownloader */
  AmazonS3 getS3Client();

  /** @return bucketName used by this ContentDownloader */
  String getBucketName();

  /** @return boolean to indicate if this ContentDownloader operates in stream mode */
  boolean downloadAsStream();
}
