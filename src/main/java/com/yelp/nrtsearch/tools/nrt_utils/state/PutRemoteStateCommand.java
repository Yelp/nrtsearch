/*
 * Copyright 2022 Yelp Inc.
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

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.server.luceneserver.IndexBackupUtils;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import com.yelp.nrtsearch.server.luceneserver.state.backend.RemoteStateBackend;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = PutRemoteStateCommand.PUT_REMOTE_STATE,
    description = "Force push index or global state to remote backend")
public class PutRemoteStateCommand implements Callable<Integer> {
  public static final String PUT_REMOTE_STATE = "putRemoteState";

  @CommandLine.Option(
      names = {"-s", "--serviceName"},
      description = "Name of nrtsearch cluster",
      required = true)
  private String serviceName;

  @CommandLine.Option(
      names = {"-r", "--resourceName"},
      description =
          "Resource name, should be index name or \""
              + RemoteStateBackend.GLOBAL_STATE_RESOURCE
              + "\"",
      required = true)
  private String resourceName;

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
      description = "File holding AWS credentials, uses default locations if not set")
  private String credsFile;

  @CommandLine.Option(
      names = {"-p", "--credsProfile"},
      description = "Profile to use from creds file",
      defaultValue = "default")
  private String credsProfile;

  @CommandLine.Option(
      names = {"-f", "--stateFile"},
      description = "File to write to remote state",
      required = true)
  private String stateFile;

  @CommandLine.Option(
      names = {"--skipValidate"},
      description = "Skip file data validation before uploading")
  private boolean skipValidate;

  @CommandLine.Option(
      names = {"--exactResourceName"},
      description = "If index resource name already has unique identifier")
  private boolean exactResourceName;

  @CommandLine.Option(
      names = {"--backupFile"},
      description = "If present, writes current state to this file before uploading")
  private String backupFile;

  @CommandLine.Option(
      names = {"--maxRetry"},
      description = "Maximum number of retry attempts for S3 failed requests",
      defaultValue = "20")
  private int maxRetry;

  private AmazonS3 s3Client;

  @VisibleForTesting
  void setS3Client(AmazonS3 s3Client) {
    this.s3Client = s3Client;
  }

  @Override
  public Integer call() throws Exception {
    if (s3Client == null) {
      s3Client =
          StateCommandUtils.createS3Client(bucketName, region, credsFile, credsProfile, maxRetry);
    }
    VersionManager versionManager = new VersionManager(s3Client, bucketName);

    String resolvedResourceName =
        StateCommandUtils.getResourceName(
            versionManager, serviceName, resourceName, exactResourceName);
    String stateFileName =
        IndexBackupUtils.isBackendGlobalState(resolvedResourceName)
            ? StateUtils.GLOBAL_STATE_FILE
            : StateUtils.INDEX_STATE_FILE;
    byte[] fileBytes = Files.readAllBytes(Path.of(stateFile));
    if (!skipValidate) {
      StateCommandUtils.validateConfigData(
          fileBytes, IndexBackupUtils.isBackendGlobalState(resolvedResourceName));
    }

    if (backupFile != null) {
      String currentFileContents =
          StateCommandUtils.getStateFileContents(
              versionManager, serviceName, resolvedResourceName, stateFileName);
      if (currentFileContents != null) {
        StateCommandUtils.writeStringToFile(currentFileContents, Path.of(backupFile).toFile());
      } else {
        System.out.println("No existing state in backend, skipping backup");
      }
    }

    StateCommandUtils.writeStateDataToBackend(
        versionManager, serviceName, resolvedResourceName, stateFileName, fileBytes);
    return 0;
  }
}
