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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.inject.Inject;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionManager {
  Logger logger = LoggerFactory.getLogger(VersionManager.class);

  private final AmazonS3 s3;
  private final String bucketName;

  @Inject
  public VersionManager(final AmazonS3 s3, final String bucketName) {
    this.s3 = s3;
    this.bucketName = bucketName;
  }

  /** Get S3 client. */
  public AmazonS3 getS3() {
    return s3;
  }

  /** Get S3 bucket name. */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * @param serviceName
   * @param resourceName
   * @return -1 if no prior versions found else most recent version number of this resource
   * @throws IOException
   */
  /*
  Gets the latest version number of a service and a resource
  Returns: The latest version number in s3 of a service and resource
  */
  public long getLatestVersionNumber(String serviceName, String resourceName) throws IOException {
    final String versionPath = String.format("%s/_version/%s", serviceName, resourceName);
    long maxVersion = getListedLatestVersionNumber(versionPath);
    return ensureLatestVersionNumber(versionPath, maxVersion);
  }

  /* Gets the latest listed version number in s3 */
  long getListedLatestVersionNumber(String versionPath) throws IOException {
    long version = -1;
    String latestVersionFile = String.format("%s/_latest_version", versionPath);
    if (s3.doesObjectExist(bucketName, latestVersionFile)) {
      try (final S3Object s3Object = s3.getObject(bucketName, latestVersionFile); ) {
        final String versionString = IOUtils.toString(s3Object.getObjectContent());
        return Integer.valueOf(versionString);
      }
    }
    return version;
  }

  /*
  Attempts to fix s3's eventual consistency for LIST operations by
  using immediately consistent GET operations to check for later versions

  Returns: listed_version if no GETs are successful, else the actual
      latest version.
  */
  long ensureLatestVersionNumber(String versionPath, long maxVersion) {
    long versionProbe = maxVersion;
    while (true) {
      versionProbe += 1;
      String probeVersion = String.format("%s/%s", versionPath, versionProbe);
      if (!s3.doesObjectExist(bucketName, probeVersion)) {
        break;
      }
    }
    versionProbe = versionProbe - 1;

    if (versionProbe != maxVersion) {
      String latestVersionFile = String.format("%s/_latest_version", versionPath);
      s3.putObject(bucketName, latestVersionFile, String.valueOf(versionProbe));
    }
    return versionProbe;
  }

  /* Blesses a particular version of a resource. */
  public boolean blessVersion(String serviceName, String resourceName, String resourceHash) {
    final String resourceKey = String.format("%s/%s/%s", serviceName, resourceName, resourceHash);
    if (!s3.doesObjectExist(bucketName, resourceKey)) {
      logger.error(
          String.format(
              "bless_version -- %s/%s/%s does not exist in s3",
              serviceName, resourceName, resourceHash));
      return false;
    }
    long newVersion;
    try {
      newVersion = getLatestVersionNumber(serviceName, resourceName) + 1;
    } catch (IOException e) {
      logger.error("Error while getting current latest version from s3 ", e);
      return false;
    }
    if (newVersion == 0) {
      logger.warn(
          String.format(
              "This is the first blessing of this resource, %s, make sure it is not a typo",
              resourceName));
    }

    String versionPath = String.format("%s/_version/%s", serviceName, resourceName);
    String versionKey = String.format("%s/%s", versionPath, newVersion);
    s3.putObject(bucketName, versionKey, resourceHash);

    String latestVersionKey = String.format("%s/_latest_version", versionPath);
    s3.putObject(bucketName, latestVersionKey, String.valueOf(newVersion));

    return true;
  }

  /* Deletes a particular version of a resource. */
  public boolean deleteVersion(String serviceName, String resourceName, String resourceHash) {
    final String resourceKey = String.format("%s/%s/%s", serviceName, resourceName, resourceHash);
    if (!s3.doesObjectExist(bucketName, resourceKey)) {
      logger.error(
          "Unable to delete object: {}/{}/{} does not exist in s3",
          serviceName,
          resourceName,
          resourceHash);
      return false;
    }
    DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, resourceKey);
    s3.deleteObject(deleteObjectRequest);
    return true;
  }

  /**
   * @param serviceName name of cluster or service
   * @param resource name of index or resource
   * @param version name or versionHash of specific entity/file within namespace
   *     serviceName/resource
   * @throws IOException
   */
  public String getVersionString(
      final String serviceName, final String resource, final String version) throws IOException {
    final String absoluteResourcePath =
        String.format("%s/_version/%s/%s", serviceName, resource, version);
    try (final S3Object s3Object = s3.getObject(bucketName, absoluteResourcePath)) {
      return IOUtils.toString(s3Object.getObjectContent());
    }
  }
}
