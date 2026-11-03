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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.index.ShardState;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PrimaryGenFencingTest {
  private static final String INDEX = "test_index";
  private static final String ENFORCE_CONFIG = "enforcePrimaryGenOrdering: true";

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupServers();
  }

  @AfterClass
  public static void cleanupClass() {
    TestServer.cleanupAll();
  }

  private TestServer startPrimary(long gen, String additionalConfig) throws IOException {
    TestServer.Builder builder =
        TestServer.builder(folder)
            .withAutoStartConfig(false, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE);
    if (additionalConfig != null) {
      builder.withAdditionalConfig(additionalConfig);
    }
    TestServer primary = builder.build();
    primary.createSimpleIndex(INDEX);
    primary.startPrimaryIndex(INDEX, gen, null);
    return primary;
  }

  private TestServer startReplica(TestServer primary, long gen, String additionalConfig)
      throws IOException {
    TestServer.Builder builder =
        TestServer.builder(folder)
            .withAutoStartConfig(false, Mode.REPLICA, 0, IndexDataLocationType.REMOTE);
    if (additionalConfig != null) {
      builder.withAdditionalConfig(additionalConfig);
    }
    TestServer replica = builder.build();
    replica.startReplicaIndex(INDEX, gen, primary.getReplicationPort(), null);
    return replica;
  }

  private StatusRuntimeException sendNrtPoint(TestServer replica, long primaryGen, long version)
      throws IOException {
    String indexId = replica.getGlobalState().getIndexStateManagerOrThrow(INDEX).getIndexId();
    try (ReplicationServerClient client =
        new ReplicationServerClient("localhost", replica.getReplicationPort())) {
      return assertThrows(
          StatusRuntimeException.class,
          () -> client.newNRTPoint(INDEX, indexId, primaryGen, version));
    }
  }

  @Test
  public void testReplicaRejectsStalePrimaryGen() throws IOException {
    TestServer primary = startPrimary(10, ENFORCE_CONFIG);
    primary.addSimpleDocs(INDEX, 1, 2, 3);
    primary.refresh(INDEX);
    TestServer replica = startReplica(primary, 10, ENFORCE_CONFIG);
    replica.waitForReplication(INDEX, primary);
    replica.verifySimpleDocIds(INDEX, 1, 2, 3);

    StatusRuntimeException e = sendNrtPoint(replica, 9, Long.MAX_VALUE);
    assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    assertTrue(e.getMessage(), e.getMessage().contains("Stale primary generation"));

    ShardState replicaShard = replica.getGlobalState().getIndexOrThrow(INDEX).getShard(0);
    assertEquals(10, replicaShard.nrtReplicaNode.getLastPrimaryGenSeen());
    replica.verifySimpleDocIds(INDEX, 1, 2, 3);

    // current primary is unaffected
    primary.addSimpleDocs(INDEX, 4);
    primary.refresh(INDEX);
    replica.waitForReplication(INDEX, primary);
    replica.verifySimpleDocIds(INDEX, 1, 2, 3, 4);
  }

  @Test
  public void testPrimaryRejectsGenNotNewerThanRestored() throws IOException {
    TestServer primary = startPrimary(10, ENFORCE_CONFIG);
    primary.addSimpleDocs(INDEX, 1, 2);
    primary.commit(INDEX);
    primary.stopIndex(INDEX);

    RestoreIndex restore =
        RestoreIndex.newBuilder()
            .setServiceName(primary.getServiceName())
            .setResourceName(INDEX)
            .setDeleteExistingData(true)
            .build();
    StatusRuntimeException e =
        assertThrows(
            StatusRuntimeException.class, () -> primary.startPrimaryIndex(INDEX, 10, restore));
    assertTrue(e.getMessage(), e.getMessage().contains("restored index data has primaryGen 10"));

    primary.startPrimaryIndex(INDEX, 11, restore);
    primary.verifySimpleDocIds(INDEX, 1, 2);
  }

  @Test
  public void testWritesRejectedOnReplica() throws IOException {
    TestServer primary = startPrimary(10, null);
    primary.addSimpleDocs(INDEX, 1);
    primary.refresh(INDEX);
    TestServer replica = startReplica(primary, 10, null);
    replica.waitForReplication(INDEX, primary);

    assertWriteRejected(() -> replica.deleteAllDocuments(INDEX));
    assertWriteRejected(
        () ->
            replica
                .getClient()
                .getBlockingStub()
                .delete(
                    AddDocumentRequest.newBuilder()
                        .setIndexName(INDEX)
                        .putFields(
                            "id",
                            AddDocumentRequest.MultiValuedField.newBuilder().addValue("1").build())
                        .build()));
    assertWriteRejected(
        () ->
            replica
                .getClient()
                .getBlockingStub()
                .forceMerge(
                    ForceMergeRequest.newBuilder()
                        .setIndexName(INDEX)
                        .setMaxNumSegments(1)
                        .setDoWait(true)
                        .build()));
    assertWriteRejected(
        () ->
            replica
                .getClient()
                .getBlockingStub()
                .forceMergeDeletes(
                    ForceMergeDeletesRequest.newBuilder()
                        .setIndexName(INDEX)
                        .setDoWait(true)
                        .build()));
    replica.verifySimpleDocIds(INDEX, 1);
  }

  private static void assertWriteRejected(Runnable r) {
    StatusRuntimeException e = assertThrows(StatusRuntimeException.class, r::run);
    assertTrue(e.getMessage(), e.getMessage().contains("on a replica node is not supported"));
  }
}
