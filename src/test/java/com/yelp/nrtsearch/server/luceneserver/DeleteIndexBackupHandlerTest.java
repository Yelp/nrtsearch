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

import com.yelp.nrtsearch.server.grpc.DeleteIndexBackupRequest;
import com.yelp.nrtsearch.server.grpc.DeleteIndexBackupResponse;
import com.yelp.nrtsearch.server.luceneserver.Handler.HandlerException;
import com.yelp.nrtsearch.server.utils.Archiver;
import com.yelp.nrtsearch.server.utils.VersionedResource;
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
    VersionedResource testVersionedResource1 =
        VersionedResource.builder()
            .setServiceName(serviceName)
            .setResourceName(resourceName + "_data")
            .setCreationTimestamp(date1)
            .setVersionHash(versionHash1)
            .createVersionedResource();

    VersionedResource testVersionedResource2 =
        VersionedResource.builder()
            .setServiceName(serviceName)
            .setResourceName(resourceName + "_data")
            .setCreationTimestamp(date2)
            .setVersionHash(versionHash2)
            .createVersionedResource();

    when(archiver.getVersionedResource(serviceName, resourceName + "_data"))
        .thenReturn(Arrays.asList(testVersionedResource1, testVersionedResource2));

    DeleteIndexBackupResponse response = handler.handle(testIndex, request);

    verify(archiver, never()).deleteVersion(serviceName, resourceName + "_data", versionHash1);
    verify(archiver).deleteVersion(serviceName, resourceName + "_data", versionHash2);

    Assert.assertEquals(response.getDeletedVersionHashesList(), Arrays.asList(versionHash2));
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
}
