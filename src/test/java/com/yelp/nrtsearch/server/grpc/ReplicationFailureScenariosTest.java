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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ReplicationFailureScenariosTest {
  public static final String TEST_INDEX = "test_index";

  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  /**
   * This rule ensure the temporary folder which maintains stateDir are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  @Test
  public void replicaDownedWhenPrimaryIndexing() throws IOException {
    // startIndex Primary
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    primaryServer.createSimpleIndex(TEST_INDEX);
    primaryServer.startPrimaryIndex(TEST_INDEX, -1, null);

    // startIndex replica
    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true,
                Mode.REPLICA,
                primaryServer.getReplicationPort(),
                IndexDataLocationType.REMOTE)
            .build();

    // add 2 docs to primary
    primaryServer.addSimpleDocs(TEST_INDEX, 1, 2);
    // backup index
    primaryServer.commit(TEST_INDEX);
    // refresh (also sends NRTPoint to replicas)
    primaryServer.refresh(TEST_INDEX);

    primaryServer.verifySimpleDocs(TEST_INDEX, 2);
    replicaServer.waitForReplication(TEST_INDEX);
    replicaServer.verifySimpleDocs(TEST_INDEX, 2);

    // stop replica instance
    replicaServer.stop();

    // add 2 docs to primary
    primaryServer.addSimpleDocs(TEST_INDEX, 3, 4);

    // re-start replica instance from a fresh index state i.e. empty index dir
    replicaServer.restart(true);

    // add 2 more docs (6 total now), annoying, sendNRTPoint gets called from primary only upon a
    // flush i.e. an index operation
    primaryServer.addSimpleDocs(TEST_INDEX, 5, 6);
    primaryServer.refresh(TEST_INDEX);

    primaryServer.verifySimpleDocs(TEST_INDEX, 6);
    replicaServer.waitForReplication(TEST_INDEX);
    replicaServer.verifySimpleDocs(TEST_INDEX, 6);
  }

  @Test
  public void primaryStoppedAndRestartedWithPreviousLocalIndex() throws IOException {
    // startIndex primary
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .withWriteDiscoveryFile(true)
            .build();
    primaryServer.createSimpleIndex(TEST_INDEX);
    primaryServer.startPrimaryIndex(TEST_INDEX, -1, null);

    // startIndex replica
    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, -1, IndexDataLocationType.LOCAL)
            .withAdditionalConfig("discoveryFileUpdateIntervalMs: 1000")
            .build();

    // add 2 docs to primary
    primaryServer.addSimpleDocs(TEST_INDEX, 1, 2);
    primaryServer.refresh(TEST_INDEX);

    // both primary and replica should have 2 docs
    primaryServer.verifySimpleDocs(TEST_INDEX, 2);
    replicaServer.waitForReplication(TEST_INDEX);
    replicaServer.verifySimpleDocs(TEST_INDEX, 2);

    // commit primary
    primaryServer.commit(TEST_INDEX);

    // gracefully stop and restart primary
    primaryServer.restart();
    // restart secondary to connect to "new" primary
    replicaServer.restart();

    // add 2 docs to primary
    primaryServer.addSimpleDocs(TEST_INDEX, 3, 4);
    primaryServer.refresh(TEST_INDEX);

    primaryServer.verifySimpleDocs(TEST_INDEX, 4);
    replicaServer.waitForReplication(TEST_INDEX);
    replicaServer.verifySimpleDocs(TEST_INDEX, 4);
  }

  @Test
  public void primaryDurabilityBasic() throws IOException {
    // startIndex primary
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    primaryServer.createSimpleIndex(TEST_INDEX);
    primaryServer.startPrimaryIndex(TEST_INDEX, -1, null);

    // add 2 docs to primary
    primaryServer.addSimpleDocs(TEST_INDEX, 1, 2);
    primaryServer.refresh(TEST_INDEX);
    // backup index
    primaryServer.commit(TEST_INDEX);

    primaryServer.verifySimpleDocs(TEST_INDEX, 2);

    // non-graceful primary restart (i.e. blow away index and state directory)
    primaryServer.restart(true);

    // add 2 docs to primary
    primaryServer.addSimpleDocs(TEST_INDEX, 3, 4);
    primaryServer.refresh(TEST_INDEX);

    // primary should show numDocs hits now
    primaryServer.verifySimpleDocs(TEST_INDEX, 4);
  }

  @Test
  public void primaryDurabilitySyncWithReplica() throws IOException {
    // startIndex primary
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withWriteDiscoveryFile(true)
            .build();
    primaryServer.createSimpleIndex(TEST_INDEX);
    primaryServer.startPrimaryIndex(TEST_INDEX, -1, null);

    // startIndex replica
    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, -1, IndexDataLocationType.REMOTE)
            .withAdditionalConfig("discoveryFileUpdateIntervalMs: 1000")
            .build();

    // add 2 docs to primary
    primaryServer.addSimpleDocs(TEST_INDEX, 1, 2);
    primaryServer.refresh(TEST_INDEX);

    // both primary and replica should have 2 docs
    primaryServer.verifySimpleDocs(TEST_INDEX, 2);
    replicaServer.waitForReplication(TEST_INDEX);
    replicaServer.verifySimpleDocs(TEST_INDEX, 2);

    // backupIndex (with 2 docs)
    primaryServer.commit(TEST_INDEX);

    // add 6 more docs to primary but do not commit, NRT is at 8 docs but commit point at 2 docs
    primaryServer.addSimpleDocs(TEST_INDEX, 3, 4);
    primaryServer.addSimpleDocs(TEST_INDEX, 5, 6);
    primaryServer.addSimpleDocs(TEST_INDEX, 7, 8);
    primaryServer.refresh(TEST_INDEX);

    primaryServer.verifySimpleDocs(TEST_INDEX, 8);
    replicaServer.waitForReplication(TEST_INDEX);
    replicaServer.verifySimpleDocs(TEST_INDEX, 8);

    // non-graceful primary restart (i.e. blow away index directory and stateDir)
    primaryServer.restart(true);
    // restart secondary to connect to "new" primary
    replicaServer.restart();
    replicaServer.registerWithPrimary(TEST_INDEX);

    // add 2 more docs to primary
    primaryServer.addSimpleDocs(TEST_INDEX, 9, 10);
    primaryServer.refresh(TEST_INDEX);

    primaryServer.verifySimpleDocs(TEST_INDEX, 4);
    replicaServer.waitForReplication(TEST_INDEX);
    replicaServer.verifySimpleDocs(TEST_INDEX, 4);
  }

  @Test
  public void primaryDurabilityWithMultipleCommits() throws IOException {
    // startIndex primary
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    primaryServer.createSimpleIndex(TEST_INDEX);
    primaryServer.startPrimaryIndex(TEST_INDEX, -1, null);

    // add 4 docs to primary
    primaryServer.addSimpleDocs(TEST_INDEX, 1, 2);
    primaryServer.addSimpleDocs(TEST_INDEX, 3, 4);
    primaryServer.commit(TEST_INDEX);
    // add 4 docs to primary
    primaryServer.addSimpleDocs(TEST_INDEX, 5, 6);
    primaryServer.addSimpleDocs(TEST_INDEX, 7, 8);
    primaryServer.commit(TEST_INDEX);

    // non-graceful primary restart (i.e. blow away index directory and stateDir)
    primaryServer.restart(true);

    // add 2 docs to primary
    primaryServer.addSimpleDocs(TEST_INDEX, 9, 10);
    primaryServer.refresh(TEST_INDEX);

    // primary should show numDocs hits now
    primaryServer.verifySimpleDocs(TEST_INDEX, 10);
  }

  @Test
  public void testPrimaryEphemeralIdChanges() throws IOException {
    // startIndex primary
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    primaryServer.createSimpleIndex(TEST_INDEX);
    primaryServer.startPrimaryIndex(TEST_INDEX, -1, null);

    // add docs to primary and check ephemeral id
    AddDocumentResponse response =
        primaryServer.addDocs(Stream.of(primaryServer.getSimpleDocRequest(TEST_INDEX, 1)));
    String firstId = response.getPrimaryId();
    response = primaryServer.addDocs(Stream.of(primaryServer.getSimpleDocRequest(TEST_INDEX, 2)));
    assertEquals(firstId, response.getPrimaryId());

    // backup index
    primaryServer.commit(TEST_INDEX);

    // non-graceful primary restart (i.e. blow away index and state directory)
    primaryServer.restart(true);

    // add docs to primary and check ephemeral id
    response = primaryServer.addDocs(Stream.of(primaryServer.getSimpleDocRequest(TEST_INDEX, 3)));
    String secondId = response.getPrimaryId();
    assertNotEquals(firstId, secondId);
    response = primaryServer.addDocs(Stream.of(primaryServer.getSimpleDocRequest(TEST_INDEX, 4)));
    assertEquals(secondId, response.getPrimaryId());
  }
}
