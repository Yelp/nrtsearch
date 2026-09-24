/*
 * Copyright 2021 Yelp Inc.
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

import com.yelp.nrtsearch.server.config.IndexStartConfig;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AckedCopyTest {
  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  /**
   * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  @Test
  public void ackAllLimit1() throws IOException, InterruptedException {
    testReplication(2, 1, 1);
  }

  @Test
  public void ack2Limit2() throws IOException, InterruptedException {
    testReplication(2, 2, 2);
  }

  @Test
  public void ack2Limit4() throws IOException, InterruptedException {
    testReplication(2, 2, 4);
  }

  @Test
  public void ack2Limit2LargeChunk() throws IOException, InterruptedException {
    testReplication(1024, 2, 2);
  }

  private void testReplication(int chunkSize, int ackEvery, int maxInFlight)
      throws IOException, InterruptedException {
    String extraConfig =
        String.join(
            "\n",
            "FileCopyConfig:",
            "  ackedCopy: true",
            "  chunkSize: " + chunkSize,
            "  ackEvery: " + ackEvery,
            "  maxInFlight: " + maxInFlight);

    // index 2 documents to primary
    TestServer testServerPrimary =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.PRIMARY, 0, IndexStartConfig.IndexDataLocationType.LOCAL)
            .withAdditionalConfig(extraConfig)
            .build();
    testServerPrimary.createSimpleIndex("test_index");
    testServerPrimary.startPrimaryIndex("test_index", -1, null);
    testServerPrimary.addSimpleDocs("test_index", 1, 2);

    // refresh (also sends NRTPoint to replicas, but none started at this point)
    testServerPrimary.refresh("test_index");
    testServerPrimary.verifySimpleDocIds("test_index", 1, 2);

    // startIndex replica
    TestServer testServerReplica =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true,
                Mode.REPLICA,
                testServerPrimary.getReplicationPort(),
                IndexStartConfig.IndexDataLocationType.LOCAL)
            .withAdditionalConfig(extraConfig)
            .build();
    testServerReplica.registerWithPrimary("test_index");

    // add 2 more docs to primary
    testServerPrimary.addSimpleDocs("test_index", 3, 4);

    // publish new NRT point (retrieve the current searcher version on primary)
    testServerPrimary.refresh("test_index");

    // primary should show 4 hits now
    testServerPrimary.verifySimpleDocs("test_index", 4);

    // replica should too!
    testServerReplica.waitForReplication("test_index");
    testServerReplica.verifySimpleDocIds("test_index", 1, 2, 3, 4);
  }
}
