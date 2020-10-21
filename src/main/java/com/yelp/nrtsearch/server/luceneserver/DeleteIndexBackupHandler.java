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

import com.yelp.nrtsearch.server.grpc.DeleteIndexBackupRequest;
import com.yelp.nrtsearch.server.grpc.DeleteIndexBackupResponse;
import com.yelp.nrtsearch.server.utils.Archiver;
import com.yelp.nrtsearch.server.utils.VersionedResource;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteIndexBackupHandler
    implements Handler<DeleteIndexBackupRequest, DeleteIndexBackupResponse> {
  private static final Logger logger = LoggerFactory.getLogger(DeleteIndexBackupHandler.class);
  private final Archiver archiver;

  public DeleteIndexBackupHandler(Archiver archiver) {
    this.archiver = archiver;
  }

  @Override
  public DeleteIndexBackupResponse handle(
      IndexState indexState, DeleteIndexBackupRequest deleteIndexBackupRequest)
      throws HandlerException {

    DeleteIndexBackupResponse.Builder deleteIndexBackupResponseBuilder =
        DeleteIndexBackupResponse.newBuilder();

    String indexName = deleteIndexBackupRequest.getIndexName();
    String serviceName = deleteIndexBackupRequest.getServiceName();
    String resourceName = deleteIndexBackupRequest.getResourceName();
    String resourceData = IndexBackupUtils.getResourceData(resourceName);
    String resourceMetadata = IndexBackupUtils.getResourceMetadata(resourceName);
    int nDays = deleteIndexBackupRequest.getNDays();

    Set<String> deletedVersionHashes = new HashSet<>();

    try {
      List<VersionedResource> versionedResourceData =
          archiver.getVersionedResource(serviceName, resourceData);
      List<VersionedResource> versionedResourceMetadata =
          archiver.getVersionedResource(serviceName, resourceMetadata);

      Instant now = Instant.now();

      List<VersionedResource> objectsOlderThanNDays =
          Stream.concat(versionedResourceData.stream(), versionedResourceMetadata.stream())
              .filter(resourceObject -> isOlderThanNDays(resourceObject, now, nDays))
              .collect(Collectors.toList());

      for (VersionedResource objectToDelete : objectsOlderThanNDays) {
        archiver.deleteVersion(
            objectToDelete.getServiceName(),
            objectToDelete.getResourceName(),
            objectToDelete.getVersionHash());
        deletedVersionHashes.add(objectToDelete.getVersionHash());
      }
    } catch (IOException e) {
      logger.error(
          "Error while trying to delete backup of index {} with serviceName {}, resourceName {}, nDays: {}",
          indexName,
          serviceName,
          resourceName,
          nDays,
          e);
      return deleteIndexBackupResponseBuilder.build();
    }

    return deleteIndexBackupResponseBuilder
        .addAllDeletedVersionHashes(deletedVersionHashes)
        .build();
  }

  public static boolean isOlderThanNDays(VersionedResource resource, Instant now, int nDays) {
    return now.minus(nDays, ChronoUnit.DAYS).isAfter(resource.getCreationTimestamp());
  }
}
