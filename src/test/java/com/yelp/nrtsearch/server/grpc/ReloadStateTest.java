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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.yelp.nrtsearch.server.config.IndexStartConfig;
import java.io.IOException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ReloadStateTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  @Test
  public void testReloadStateExistingIndex() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.PRIMARY, 0, IndexStartConfig.IndexDataLocationType.REMOTE)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);
    assertTrue(primaryServer.isReady());
    assertTrue(primaryServer.isStarted("test_index"));

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true,
                Mode.REPLICA,
                primaryServer.getReplicationPort(),
                IndexStartConfig.IndexDataLocationType.REMOTE)
            .withSyncInitialNrtPoint(false)
            .build();
    assertTrue(replicaServer.isReady());
    assertTrue(replicaServer.isStarted("test_index"));

    Field newField =
        Field.newBuilder()
            .setName("new_field")
            .setStoreDocValues(true)
            .setSearch(true)
            .setType(FieldType.ATOM)
            .build();
    primaryServer.registerFields("test_index", ImmutableList.of(newField));

    replicaServer.reloadState();
    replicaServer.verifyFieldName("test_index", "new_field");
  }

  @Test
  public void testReloadStateNewCreatedIndex() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.PRIMARY, 0, IndexStartConfig.IndexDataLocationType.REMOTE)
            .build();
    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true,
                Mode.REPLICA,
                primaryServer.getReplicationPort(),
                IndexStartConfig.IndexDataLocationType.REMOTE)
            .withSyncInitialNrtPoint(false)
            .build();

    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);
    assertTrue(primaryServer.isReady());
    assertTrue(primaryServer.isStarted("test_index"));

    replicaServer.reloadState();

    assertTrue(replicaServer.isReady());
    assertTrue(replicaServer.isStarted("test_index"));
  }
}
