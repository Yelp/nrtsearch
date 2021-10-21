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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.backup.VersionedResource;
import com.yelp.nrtsearch.server.grpc.DeleteIndexBackupRequest;
import com.yelp.nrtsearch.server.grpc.DeleteIndexBackupResponse;
import com.yelp.nrtsearch.server.luceneserver.Handler.HandlerException;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

public class DeleteIndexBackupHandlerTest {

  @Test
  public void testDeleteIndexBackupHandlerHandle() throws HandlerException, IOException {
    String indexName = "testindex";
    String serviceName = "testservice";
    String resourceName = "testresource";
    int nDays = 30;

    Archiver archiver = mock(Archiver.class);
    DeleteIndexBackupHandler handler = new DeleteIndexBackupHandler(archiver);
    DeleteIndexBackupRequest request =
        DeleteIndexBackupRequest.newBuilder()
            .setIndexName(indexName)
            .setServiceName(serviceName)
            .setResourceName(resourceName)
            .setNDays(nDays)
            .build();

    IndexState testIndex = mock(IndexState.class);

    Instant now = Instant.now();

    Instant date1 = now.minus(20, ChronoUnit.DAYS);
    Instant date2 = now.minus(40, ChronoUnit.DAYS);

    String versionHash1 = "hash_1";
    String versionHash2 = "hash_2";

    String resourceData = IndexBackupUtils.getResourceData(resourceName);
    String resourceMetadata = IndexBackupUtils.getResourceMetadata(resourceName);
    String resourceVersionData = IndexBackupUtils.getResourceVersionData(resourceName);
    String resourceVersionMetadata = IndexBackupUtils.getResourceVersionMetadata(resourceName);

    VersionedResource resourceData1 =
        buildVersionedResource(serviceName, resourceData, date1, versionHash1);
    VersionedResource resourceMetadata1 =
        buildVersionedResource(serviceName, resourceMetadata, date1, versionHash1);
    VersionedResource resourceVersionData1 =
        buildVersionedResource(serviceName, resourceVersionData, date1, versionHash1);
    VersionedResource resourceVersionMetadata1 =
        buildVersionedResource(serviceName, resourceVersionMetadata, date1, versionHash1);

    VersionedResource resourceData2 =
        buildVersionedResource(serviceName, resourceData, date2, versionHash2);
    VersionedResource resourceMetadata2 =
        buildVersionedResource(serviceName, resourceMetadata, date2, versionHash2);
    VersionedResource resourceVersionData2 =
        buildVersionedResource(serviceName, resourceVersionData, date2, versionHash2);
    VersionedResource resourceVersionMetadata2 =
        buildVersionedResource(serviceName, resourceVersionMetadata, date2, versionHash2);

    // data from {resource}_data folder
    when(archiver.getVersionedResource(serviceName, resourceData))
        .thenReturn(Arrays.asList(resourceData1, resourceData2));

    // data from {resource}_metadata folder
    when(archiver.getVersionedResource(serviceName, resourceMetadata))
        .thenReturn(Arrays.asList(resourceMetadata1, resourceMetadata2));

    // data from _version/{resource}_data folder
    when(archiver.getVersionedResource(serviceName, resourceVersionData))
        .thenReturn(Arrays.asList(resourceVersionData1, resourceVersionData2));

    // data from _version/{resource}_metadata folder
    when(archiver.getVersionedResource(serviceName, resourceVersionMetadata))
        .thenReturn(Arrays.asList(resourceVersionMetadata1, resourceVersionMetadata2));

    DeleteIndexBackupResponse response = handler.handle(testIndex, request);

    // verify that a 20 days old backup doesn't get deleted
    verify(archiver, never()).deleteVersion(serviceName, resourceData, versionHash1);
    verify(archiver, never()).deleteVersion(serviceName, resourceMetadata, versionHash1);
    verify(archiver, never()).deleteVersion(serviceName, resourceVersionData, versionHash1);
    verify(archiver, never()).deleteVersion(serviceName, resourceVersionMetadata, versionHash1);

    // verify that a 40 days old backup gets deleted completely
    verify(archiver).deleteVersion(serviceName, resourceData, versionHash2);
    verify(archiver).deleteVersion(serviceName, resourceMetadata, versionHash2);
    verify(archiver).deleteVersion(serviceName, resourceVersionData, versionHash2);
    verify(archiver).deleteVersion(serviceName, resourceVersionMetadata, versionHash2);

    // verify the response format: data from all four locations should be deleted. The locations:
    // {resource}_data
    Assert.assertEquals(response.getDeletedResourceDataHashesList(), Arrays.asList(versionHash2));
    // {resource}_metadata
    Assert.assertEquals(
        response.getDeletedResourceMetadataHashesList(), Arrays.asList(versionHash2));
    // _version/{resource}_data
    Assert.assertEquals(response.getDeletedDataVersionsList(), Arrays.asList(versionHash2));
    // _version/{resource}_metadata
    Assert.assertEquals(response.getDeletedMetadataVersionsList(), Arrays.asList(versionHash2));
  }

  @Test
  public void testIsOlderThanNDays() {

    Instant now = Instant.now();
    Instant date1 = now.minus(20, ChronoUnit.DAYS);
    Instant date2 = now.minus(35, ChronoUnit.DAYS);

    int nDays = 30;

    VersionedResource testObj1 =
        VersionedResource.builder().setCreationTimestamp(date1).createVersionedResource();

    Assert.assertEquals(false, DeleteIndexBackupHandler.isOlderThanNDays(testObj1, now, nDays));

    VersionedResource testObj2 =
        VersionedResource.builder().setCreationTimestamp(date2).createVersionedResource();

    Assert.assertEquals(true, DeleteIndexBackupHandler.isOlderThanNDays(testObj2, now, nDays));
  }

  private VersionedResource buildVersionedResource(
      String serviceName, String resourceName, Instant timestamp, String versionHash) {
    VersionedResource versionedResource =
        VersionedResource.builder()
            .setServiceName(serviceName)
            .setResourceName(resourceName)
            .setCreationTimestamp(timestamp)
            .setVersionHash(versionHash)
            .createVersionedResource();

    return versionedResource;
  }
}
