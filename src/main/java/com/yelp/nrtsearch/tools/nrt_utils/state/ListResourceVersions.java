/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.tools.nrt_utils.state;

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.remote.s3.S3Util;
import com.yelp.nrtsearch.server.utils.TimeStringUtils;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;

@CommandLine.Command(
    name = ListResourceVersions.LIST_RESOURCE_VERSIONS,
    description = "List index resource or global state versions in S3 by prefix")
public class ListResourceVersions implements Callable<Integer> {
  public static final String LIST_RESOURCE_VERSIONS = "listResourceVersions";
  public static final int MAX_LIST_SIZE = 1000;

  @CommandLine.Option(
      names = {"-s", "--serviceName"},
      description = "Name of nrtsearch cluster",
      required = true)
  private String serviceName;

  @CommandLine.Option(
      names = {"-t", "--indexResourceType"},
      description =
          "Type of index resource, must be set when not requesting global state. One of: "
              + "INDEX_STATE, POINT_STATE, WARMING_QUERIES")
  private String resourceType;

  @CommandLine.Option(
      names = {"--versionPrefix"},
      description = "Prefix of versions to list, default: \"\"",
      defaultValue = "")
  private String versionPrefix;

  @CommandLine.Option(
      names = {"-r", "--resourceName"},
      description =
          "Resource name, should be index name or \""
              + StateCommandUtils.GLOBAL_STATE_RESOURCE
              + "\"",
      required = true)
  private String resourceName;

  @CommandLine.Option(
      names = {"--exactResourceName"},
      description = "If resource name already has unique identifier")
  private boolean exactResourceName;

  @CommandLine.Option(
      names = {"-b", "--bucketName"},
      description = "Name of bucket containing state files",
      required = true)
  private String bucketName;

  @CommandLine.Option(
      names = {"--region"},
      description = "AWS region name, such as us-west-1, us-west-2, us-east-1")
  private String region;

  @CommandLine.Option(
      names = {"-c", "--credsFile"},
      description =
          "File holding AWS credentials; Will use DefaultCredentialProvider if this is unset.")
  private String credsFile;

  @CommandLine.Option(
      names = {"-p", "--credsProfile"},
      description = "Profile to use from creds file; Neglected when credsFile is unset.",
      defaultValue = "default")
  private String credsProfile;

  @CommandLine.Option(
      names = {"--maxRetry"},
      description = "Maximum number of retry attempts for S3 failed requests",
      defaultValue = "20")
  private int maxRetry;

  private S3Util.S3ClientBundle s3ClientBundle;

  @VisibleForTesting
  void setS3ClientBundle(S3Util.S3ClientBundle s3ClientBundle) {
    this.s3ClientBundle = s3ClientBundle;
  }

  @Override
  public Integer call() throws Exception {
    if (s3ClientBundle == null) {
      s3ClientBundle =
          StateCommandUtils.createS3ClientBundle(
              bucketName, region, credsFile, credsProfile, maxRetry);
    }
    S3Backend s3Backend =
        new S3Backend(bucketName, false, S3Backend.DEFAULT_CONFIG, s3ClientBundle);
    String resolvedResourceName =
        StateCommandUtils.getResourceName(s3Backend, serviceName, resourceName, exactResourceName);

    String prefix;
    if (StateCommandUtils.isGlobalState(resolvedResourceName)) {
      prefix = S3Backend.getGlobalStateResourcePrefix(serviceName);
    } else {
      RemoteBackend.IndexResourceType indexResourceType =
          StateCommandUtils.parseIndexResourceType(resourceType);
      prefix =
          S3Backend.getIndexResourcePrefix(serviceName, resolvedResourceName, indexResourceType);
    }
    String listPrefix = prefix + versionPrefix;
    System.out.println(
        "Listing versions for "
            + resolvedResourceName
            + " with prefix "
            + listPrefix
            + " (max "
            + MAX_LIST_SIZE
            + ")");
    ListObjectsRequest listObjectsRequest =
        ListObjectsRequest.builder()
            .bucket(bucketName)
            .prefix(listPrefix)
            .maxKeys(MAX_LIST_SIZE)
            .build();
    ListObjectsResponse listing = s3Backend.getS3().listObjects(listObjectsRequest);
    listing
        .contents()
        .forEach(
            object -> {
              String suffix = object.key().split(prefix)[1];
              printVersion(suffix);
            });
    return 0;
  }

  private static void printVersion(String keySuffix) {
    String timestampStr = "";
    String[] splits = keySuffix.split("-");
    if (splits.length > 1) {
      String timeString = splits[0];
      if (TimeStringUtils.isTimeStringSec(timeString)) {
        timestampStr = " (" + TimeStringUtils.parseTimeStringSec(timeString).toString() + ")";
      }
    }
    System.out.println(keySuffix + timestampStr);
  }
}
