/*
 * Copyright 2023 Yelp Inc.
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

import com.amazonaws.services.s3.AmazonS3URI;
import com.google.common.base.Strings;

/** Utility class for working with S3 paths. */
public class S3Util {

  /**
   * Check if a string is a valid S3 file path.
   *
   * @param path String to check
   * @return True if the string is a valid S3 file path, false otherwise
   */
  public static boolean isValidS3FilePath(String path) {
    try {
      AmazonS3URI s3URI = new AmazonS3URI(path);
      String key = s3URI.getKey();
      return isKeyValidForFile(key);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Get name of file in S3 from the S3 path, assuming that the name is the last part of a key with
   * directories separated by "/".
   *
   * @param path S3 path
   * @return Name of file
   */
  public static String getS3FileName(String path) {
    AmazonS3URI s3URI = new AmazonS3URI(path);
    String key = s3URI.getKey();
    if (!isKeyValidForFile(key)) {
      throw new IllegalArgumentException(String.format("S3 path %s is not valid for a file", path));
    }
    String[] split = key.split("/");
    return split[split.length - 1];
  }

  private static boolean isKeyValidForFile(String key) {
    return !Strings.isNullOrEmpty(key) && !key.endsWith("/");
  }
}
