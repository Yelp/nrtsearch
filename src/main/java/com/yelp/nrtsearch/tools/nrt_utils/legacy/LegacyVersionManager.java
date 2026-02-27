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
package com.yelp.nrtsearch.tools.nrt_utils.legacy;

import com.google.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class LegacyVersionManager {
  Logger logger = LoggerFactory.getLogger(LegacyVersionManager.class);

  private final S3Client s3;
  private final String bucketName;

  @Inject
  public LegacyVersionManager(final S3Client s3, final String bucketName) {
    this.s3 = s3;
    this.bucketName = bucketName;
  }

  /** Get S3 client. */
  public S3Client getS3() {
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
    try {
      s3.headObject(HeadObjectRequest.builder().bucket(bucketName).key(latestVersionFile).build());
      GetObjectRequest getObjectRequest =
          GetObjectRequest.builder().bucket(bucketName).key(latestVersionFile).build();
      final String versionString =
          IOUtils.toString(s3.getObject(getObjectRequest), StandardCharsets.UTF_8);
      return Integer.parseInt(versionString);
    } catch (NoSuchKeyException e) {
      return version;
    }
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
      try {
        s3.headObject(HeadObjectRequest.builder().bucket(bucketName).key(probeVersion).build());
      } catch (NoSuchKeyException e) {
        break;
      }
    }
    versionProbe = versionProbe - 1;

    if (versionProbe != maxVersion) {
      String latestVersionFile = String.format("%s/_latest_version", versionPath);
      PutObjectRequest putObjectRequest =
          PutObjectRequest.builder().bucket(bucketName).key(latestVersionFile).build();
      s3.putObject(putObjectRequest, RequestBody.fromString(String.valueOf(versionProbe)));
    }
    return versionProbe;
  }

  /* Blesses a particular version of a resource. */
  public boolean blessVersion(String serviceName, String resourceName, String resourceHash) {
    final String resourceKey = String.format("%s/%s/%s", serviceName, resourceName, resourceHash);
    try {
      s3.headObject(HeadObjectRequest.builder().bucket(bucketName).key(resourceKey).build());
    } catch (NoSuchKeyException e) {
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
    PutObjectRequest putVersionRequest =
        PutObjectRequest.builder().bucket(bucketName).key(versionKey).build();
    s3.putObject(putVersionRequest, RequestBody.fromString(resourceHash));

    String latestVersionKey = String.format("%s/_latest_version", versionPath);
    PutObjectRequest putLatestRequest =
        PutObjectRequest.builder().bucket(bucketName).key(latestVersionKey).build();
    s3.putObject(putLatestRequest, RequestBody.fromString(String.valueOf(newVersion)));

    return true;
  }

  /* Deletes a particular version of a resource. */
  public boolean deleteVersion(String serviceName, String resourceName, String resourceHash) {
    final String resourceKey = String.format("%s/%s/%s", serviceName, resourceName, resourceHash);
    try {
      s3.headObject(HeadObjectRequest.builder().bucket(bucketName).key(resourceKey).build());
    } catch (NoSuchKeyException e) {
      logger.error(
          "Unable to delete object: {}/{}/{} does not exist in s3",
          serviceName,
          resourceName,
          resourceHash);
      return false;
    }
    DeleteObjectRequest deleteObjectRequest =
        DeleteObjectRequest.builder().bucket(bucketName).key(resourceKey).build();
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
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder().bucket(bucketName).key(absoluteResourcePath).build();
    return IOUtils.toString(s3.getObject(getObjectRequest), StandardCharsets.UTF_8);
  }
}
