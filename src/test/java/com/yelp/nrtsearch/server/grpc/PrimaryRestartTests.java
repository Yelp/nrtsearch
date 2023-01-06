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

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import java.io.IOException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PrimaryRestartTests {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  @Test
  public void testReplication() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);

    primaryServer.addSimpleDocs("test_index", 1, 2, 3);
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    primaryServer.verifySimpleDocs("test_index", 3);

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true,
                Mode.REPLICA,
                primaryServer.getReplicationPort(),
                IndexDataLocationType.REMOTE)
            .build();
    replicaServer.verifySimpleDocs("test_index", 3);

    primaryServer.addSimpleDocs("test_index", 4, 5);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    primaryServer.verifySimpleDocs("test_index", 5);
    replicaServer.verifySimpleDocs("test_index", 5);
  }

  // This test models the current behavior when the primary restarts. This behavior is not
  // desired, and the test should be updated/extended once it is fixed.
  @Test
  public void testPrimaryRestartReplication() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withWriteDiscoveryFile(true)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);

    primaryServer.addSimpleDocs("test_index", 1, 2, 3);
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, -1, IndexDataLocationType.REMOTE)
            .withAdditionalConfig("discoveryFileUpdateIntervalMs: 1000")
            .build();

    primaryServer.addSimpleDocs("test_index", 4, 5);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    primaryServer.restart();
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5);

    replicaServer.registerWithPrimary("test_index");

    primaryServer.addSimpleDocs("test_index", 6, 7, 8);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 6, 7, 8);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5);

    primaryServer.addSimpleDocs("test_index", 9);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 6, 7, 8, 9);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5);
  }
}
