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

import static com.yelp.nrtsearch.tools.nrt_utils.state.StateCommandUtils.NOT_SET;

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.remote.s3.S3Util;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import software.amazon.awssdk.services.s3.S3Client;

@CommandLine.Command(
    name = SetResourceVersionCommand.SET_RESOURCE_VERSION,
    description = "Set current index resource or global state version in S3")
public class SetResourceVersionCommand implements Callable<Integer> {
  public static final String SET_RESOURCE_VERSION = "setResourceVersion";

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
      names = {"--resourceVersion"},
      description = "Version to set for resource",
      required = true)
  private String resourceVersion;

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

  private S3Client s3Client;

  @VisibleForTesting
  void setS3Client(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  @Override
  public Integer call() throws Exception {
    if (s3Client == null) {
      s3Client =
          StateCommandUtils.createS3Client(bucketName, region, credsFile, credsProfile, maxRetry);
    }
    S3Backend s3Backend =
        new S3Backend(
            bucketName, false, S3Backend.DEFAULT_CONFIG, new S3Util.S3ClientBundle(s3Client, null));
    String resolvedResourceName =
        StateCommandUtils.getResourceName(s3Backend, serviceName, resourceName, exactResourceName);

    String prefix;
    String previousVersion = NOT_SET;
    if (StateCommandUtils.isGlobalState(resolvedResourceName)) {
      prefix = S3Backend.getGlobalStateResourcePrefix(serviceName);
      if (s3Backend.exists(serviceName, RemoteBackend.GlobalResourceType.GLOBAL_STATE)) {
        previousVersion = s3Backend.getCurrentResourceName(prefix);
      }
    } else {
      RemoteBackend.IndexResourceType indexResourceType =
          StateCommandUtils.parseIndexResourceType(resourceType);
      prefix =
          S3Backend.getIndexResourcePrefix(serviceName, resolvedResourceName, indexResourceType);
      if (s3Backend.exists(serviceName, resolvedResourceName, indexResourceType)) {
        previousVersion = s3Backend.getCurrentResourceName(prefix);
      }
    }
    try {
      software.amazon.awssdk.services.s3.model.HeadObjectRequest headRequest =
          software.amazon.awssdk.services.s3.model.HeadObjectRequest.builder()
              .bucket(bucketName)
              .key(prefix + resourceVersion)
              .build();
      s3Client.headObject(headRequest);
    } catch (software.amazon.awssdk.services.s3.model.NoSuchKeyException e) {
      throw new IllegalArgumentException("Resource version does not exist: " + resourceVersion);
    }
    System.out.println("Previous version: " + previousVersion);
    s3Backend.setCurrentResource(prefix, resourceVersion);
    return 0;
  }
}
