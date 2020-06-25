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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.grpc.BackupIndexRequest;
import com.yelp.nrtsearch.server.grpc.BackupIndexResponse;
import com.yelp.nrtsearch.server.grpc.CreateSnapshotRequest;
import com.yelp.nrtsearch.server.grpc.CreateSnapshotResponse;
import com.yelp.nrtsearch.server.grpc.ReleaseSnapshotRequest;
import com.yelp.nrtsearch.server.grpc.ReleaseSnapshotResponse;
import com.yelp.nrtsearch.server.utils.Archiver;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupIndexRequestHandler implements Handler<BackupIndexRequest, BackupIndexResponse> {
  Logger logger = LoggerFactory.getLogger(BackupIndexRequestHandler.class);
  private final Archiver archiver;

  public BackupIndexRequestHandler(Archiver archiver) {
    this.archiver = archiver;
  }

  @Override
  public BackupIndexResponse handle(IndexState indexState, BackupIndexRequest backupIndexRequest)
      throws HandlerException {
    BackupIndexResponse.Builder backupIndexResponseBuilder = BackupIndexResponse.newBuilder();
    String indexName = backupIndexRequest.getIndexName();
    try {
      // only upload metadata in case we are replica
      if (indexState.getShard(0).isReplica()) {
        uploadMetadata(
            backupIndexRequest.getServiceName(),
            backupIndexRequest.getResourceName(),
            indexState,
            backupIndexResponseBuilder);
      } else {
        indexState.commit();

        CreateSnapshotRequest createSnapshotRequest =
            CreateSnapshotRequest.newBuilder().setIndexName(indexName).build();

        CreateSnapshotResponse createSnapshotResponse =
            new CreateSnapshotHandler().createSnapshot(indexState, createSnapshotRequest);

        uploadArtifacts(
            backupIndexRequest.getServiceName(),
            backupIndexRequest.getResourceName(),
            indexState,
            backupIndexResponseBuilder);

        ReleaseSnapshotRequest releaseSnapshotRequest =
            ReleaseSnapshotRequest.newBuilder()
                .setIndexName(indexName)
                .setSnapshotId(createSnapshotResponse.getSnapshotId())
                .build();
        ReleaseSnapshotResponse releaseSnapshotResponse =
            new ReleaseSnapshotHandler().handle(indexState, releaseSnapshotRequest);
      }

    } catch (IOException e) {
      logger.error(
          String.format(
              "Error while trying to backup index %s with serviceName %s, resourceName %s",
              indexName, backupIndexRequest.getServiceName(), backupIndexRequest.getResourceName()),
          e);
      return backupIndexResponseBuilder.build();
    }

    return backupIndexResponseBuilder.build();
  }

  public static String getResourceMetadata(String resourceName) {
    return String.format("%s_metadata", resourceName);
  }

  public static String getResourceData(String resourceName) {
    return String.format("%s_data", resourceName);
  }

  public void uploadArtifacts(
      String serviceName,
      String resourceName,
      IndexState indexState,
      BackupIndexResponse.Builder backupIndexResponseBuilder)
      throws IOException {
    String resourceData = getResourceData(resourceName);
    String versionHash = archiver.upload(serviceName, resourceData, indexState.rootDir);
    archiver.blessVersion(serviceName, resourceData, versionHash);
    backupIndexResponseBuilder.setDataVersionHash(versionHash);

    uploadMetadata(serviceName, resourceName, indexState, backupIndexResponseBuilder);
  }

  public void uploadMetadata(
      String serviceName,
      String resourceName,
      IndexState indexState,
      BackupIndexResponse.Builder backupIndexResponseBuilder)
      throws IOException {
    String resourceMetadata = getResourceMetadata(resourceName);
    String versionHash =
        archiver.upload(serviceName, resourceMetadata, indexState.globalState.stateDir);
    archiver.blessVersion(serviceName, resourceMetadata, versionHash);
    backupIndexResponseBuilder.setMetadataVersionHash(versionHash);
  }
}
