/*
 * Copyright 2026 Yelp Inc.
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

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Replication where the primary uploads every refresh to S3 and replicas download refreshes from
 * S3, instead of copying them from the primary.
 */
public class S3RefreshReplicationTest {
  private static final String TEST_INDEX = "test_index";
  private static final String PRIMARY_CONFIG =
      String.join("\n", "s3RefreshUpload: true", "s3RefreshNotifyAfterUpload: true");
  private static final String REPLICA_CONFIG = "s3ReplicaRefreshDownload: true";

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupServers();
  }

  @AfterClass
  public static void cleanupClass() {
    TestServer.cleanupAll();
  }

  @Test
  public void testRefreshReplicatesThroughS3() throws IOException {
    TestServer primary = startPrimary();
    TestServer replica = startReplica(primary);

    primary.addSimpleDocs(TEST_INDEX, 1, 2);
    primary.refresh(TEST_INDEX);
    replica.waitForReplication(TEST_INDEX, primary);
    replica.verifySimpleDocs(TEST_INDEX, 2);

    primary.addSimpleDocs(TEST_INDEX, 3, 4);
    primary.refresh(TEST_INDEX);
    replica.waitForReplication(TEST_INDEX, primary);
    replica.verifySimpleDocs(TEST_INDEX, 4);
  }

  /** Commit refreshes through the same upload-then-notify path as a regular refresh. */
  @Test
  public void testCommitReplicatesThroughS3() throws Exception {
    TestServer primary = startPrimary();
    TestServer replica = startReplica(primary);

    primary.addSimpleDocs(TEST_INDEX, 1, 2);
    primary.refresh(TEST_INDEX);
    replica.waitForReplication(TEST_INDEX, primary);

    // no explicit refresh, the commit creates and publishes the new version
    primary.addSimpleDocs(TEST_INDEX, 3, 4);
    primary.commit(TEST_INDEX);
    replica.waitForReplication(TEST_INDEX, primary);
    replica.verifySimpleDocs(TEST_INDEX, 4);

    // the version must not be applied a second time
    long end = System.currentTimeMillis() + 2000;
    while (System.currentTimeMillis() < end) {
      replica.verifySimpleDocs(TEST_INDEX, 4);
      Thread.sleep(50);
    }
  }

  /**
   * writeNRTPoint sends the new version to replicas before it is in S3. The replica must not reload
   * the version it already has from S3, since a second searcher with the same version makes
   * SearcherLifetimeManager.record() fail every search.
   */
  @Test
  public void testNotifiedVersionNotInS3DoesNotReapplyCurrentVersion() throws Exception {
    TestServer primary = startPrimary();
    TestServer replica = startReplica(primary);

    primary.addSimpleDocs(TEST_INDEX, 1, 2);
    primary.refresh(TEST_INDEX);
    replica.waitForReplication(TEST_INDEX, primary);
    replica.verifySimpleDocs(TEST_INDEX, 2);
    long replicaVersion = getReplicaVersion(replica);

    primary.addSimpleDocs(TEST_INDEX, 3, 4);
    primary.getReplicationClient().writeNRTPoint(TEST_INDEX);

    // give a (wrongly) started copy job time to finish and swap in a new searcher
    long end = System.currentTimeMillis() + 2000;
    while (System.currentTimeMillis() < end) {
      replica.verifySimpleDocs(TEST_INDEX, 2);
      Thread.sleep(50);
    }
    assertEquals(replicaVersion, getReplicaVersion(replica));

    // the next uploaded refresh still replicates
    primary.addSimpleDocs(TEST_INDEX, 5, 6);
    primary.refresh(TEST_INDEX);
    replica.waitForReplication(TEST_INDEX, primary);
    replica.verifySimpleDocs(TEST_INDEX, 6);
  }

  /**
   * Mixed config, as during a rollout: replicas download from S3, but the primary notifies them at
   * refresh time instead of after the upload. Each refresh races the upload against the replica's
   * S3 read. Replicas that read S3 first must skip the point, not reload their current version.
   *
   * <p>This depends on timing, and locally the upload usually wins the race, so it may pass even
   * without the guard. {@link #testNotifiedVersionNotInS3DoesNotReapplyCurrentVersion} covers the
   * same mechanism deterministically.
   */
  @Test
  public void testPrimaryNotifiesBeforeUpload() throws Exception {
    TestServer primary = startPrimary("s3RefreshUpload: true");
    TestServer replica = startReplica(primary);

    int numDocs = 0;
    for (int i = 0; i < 20; i++) {
      primary.addSimpleDocs(TEST_INDEX, ++numDocs);
      primary.refresh(TEST_INDEX);
      for (int j = 0; j < 5; j++) {
        // must never fail, whichever version the replica is on
        replica.getClient().getBlockingStub().search(simpleSearch());
        Thread.sleep(10);
      }
    }
  }

  private static SearchRequest simpleSearch() {
    return SearchRequest.newBuilder()
        .setIndexName(TEST_INDEX)
        .addAllRetrieveFields(TestServer.simpleFieldNames)
        .setTopHits(10)
        .build();
  }

  private TestServer startPrimary() throws IOException {
    return startPrimary(PRIMARY_CONFIG);
  }

  private TestServer startPrimary(String config) throws IOException {
    TestServer primary =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withAdditionalConfig(config)
            .build();
    primary.createSimpleIndex(TEST_INDEX);
    primary.startPrimaryIndex(TEST_INDEX, -1, null);
    return primary;
  }

  private TestServer startReplica(TestServer primary) throws IOException {
    return TestServer.builder(folder)
        .withAutoStartConfig(
            true, Mode.REPLICA, primary.getReplicationPort(), IndexDataLocationType.REMOTE)
        .withAdditionalConfig(REPLICA_CONFIG)
        .build();
  }

  private static long getReplicaVersion(TestServer replica) throws IOException {
    return replica
        .getGlobalState()
        .getIndexOrThrow(TEST_INDEX)
        .getShard(0)
        .nrtReplicaNode
        .getCurrentSearchingVersion();
  }
}
