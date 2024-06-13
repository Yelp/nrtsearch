/*
 * Copyright 2024 Yelp Inc.
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

import static org.junit.Assert.*;

import com.yelp.nrtsearch.server.config.IndexStartConfig;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class VerifyIndexIdTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  private TestServer getVerifyIndexIdPrimary() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.PRIMARY, 0, IndexStartConfig.IndexDataLocationType.LOCAL)
            .withAdditionalConfig("verifyReplicationIndexId: true")
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);
    return primaryServer;
  }

  private TestServer getVerifyIndexIdReplica() throws IOException {
    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, 0, IndexStartConfig.IndexDataLocationType.LOCAL)
            .withAdditionalConfig("verifyReplicationIndexId: true")
            .build();
    return replicaServer;
  }

  private void assertException(StatusRuntimeException e) {
    assertEquals(Status.FAILED_PRECONDITION.getCode(), e.getStatus().getCode());
    assertTrue(e.getMessage().startsWith("FAILED_PRECONDITION: Index id mismatch"));
    assertTrue(e.getMessage().contains("actual: invalid_id"));
  }

  @Test
  public void testVerifyIndexId_addReplicas() throws IOException {
    TestServer primary = getVerifyIndexIdPrimary();
    try {
      primary
          .getReplicationClient()
          .getBlockingStub()
          .addReplicas(
              AddReplicaRequest.newBuilder()
                  .setIndexName("test_index")
                  .setIndexId("invalid_id")
                  .setHostName("host")
                  .setPort(1234)
                  .setMagicNumber(ReplicationServerClient.BINARY_MAGIC)
                  .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertException(e);
    }
  }

  @Test
  public void testVerifyIndexId_recvRawFile() throws IOException {
    TestServer primary = getVerifyIndexIdPrimary();
    try {
      Iterator<RawFileChunk> it =
          primary
              .getReplicationClient()
              .getBlockingStub()
              .recvRawFile(
                  FileInfo.newBuilder()
                      .setIndexName("test_index")
                      .setIndexId("invalid_id")
                      .setFileName("file")
                      .setFpStart(0)
                      .build());
      it.next();
      fail();
    } catch (StatusRuntimeException e) {
      assertException(e);
    }
  }

  @Test
  public void testVerifyIndexId_recvRawFileV2() throws IOException {
    TestServer primary = getVerifyIndexIdPrimary();
    AtomicReference<Throwable> t = new AtomicReference<>();
    final AtomicReference<Boolean> done = new AtomicReference<>(false);
    StreamObserver<FileInfo> so =
        primary
            .getReplicationClient()
            .getAsyncStub()
            .recvRawFileV2(
                new StreamObserver<>() {
                  @Override
                  public void onNext(RawFileChunk rawFileChunk) {
                    synchronized (done) {
                      done.set(true);
                      done.notifyAll();
                    }
                  }

                  @Override
                  public void onError(Throwable throwable) {
                    System.out.println("error received: " + throwable);
                    t.set(throwable);
                    synchronized (done) {
                      done.set(true);
                      done.notifyAll();
                    }
                  }

                  @Override
                  public void onCompleted() {
                    synchronized (done) {
                      done.set(true);
                      done.notifyAll();
                    }
                  }
                });
    so.onNext(
        FileInfo.newBuilder()
            .setIndexName("test_index")
            .setIndexId("invalid_id")
            .setFileName("file")
            .setFpStart(0)
            .build());
    synchronized (done) {
      while (true) {
        if (done.get()) {
          break;
        }
        try {
          done.wait(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    assertTrue(t.get() instanceof StatusRuntimeException);
    assertException((StatusRuntimeException) t.get());
  }

  @Test
  public void testVerifyIndexId_recvCopyState() throws IOException {
    TestServer primary = getVerifyIndexIdPrimary();
    try {
      primary
          .getReplicationClient()
          .getBlockingStub()
          .recvCopyState(
              CopyStateRequest.newBuilder()
                  .setIndexName("test_index")
                  .setIndexId("invalid_id")
                  .setMagicNumber(ReplicationServerClient.BINARY_MAGIC)
                  .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertException(e);
    }
  }

  @Test
  public void testVerifyIndexId_copyFiles() throws IOException {
    TestServer primary = getVerifyIndexIdPrimary();
    try {
      Iterator<TransferStatus> it =
          primary
              .getReplicationClient()
              .getBlockingStub()
              .copyFiles(
                  CopyFiles.newBuilder()
                      .setIndexName("test_index")
                      .setIndexId("invalid_id")
                      .setMagicNumber(ReplicationServerClient.BINARY_MAGIC)
                      .build());
      it.next();
      fail();
    } catch (StatusRuntimeException e) {
      assertException(e);
    }
  }

  @Test
  public void testVerifyIndexId_newNRTPoint() throws IOException {
    TestServer primary = getVerifyIndexIdPrimary();
    try {
      primary
          .getReplicationClient()
          .getBlockingStub()
          .newNRTPoint(
              NewNRTPoint.newBuilder()
                  .setIndexName("test_index")
                  .setIndexId("invalid_id")
                  .setMagicNumber(ReplicationServerClient.BINARY_MAGIC)
                  .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertException(e);
    }
  }

  @Test
  public void testReplication() throws IOException {
    TestServer primaryServer = getVerifyIndexIdPrimary();

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
                IndexStartConfig.IndexDataLocationType.LOCAL)
            .withAdditionalConfig("verifyReplicationIndexId: true")
            .build();
    replicaServer.verifySimpleDocs("test_index", 3);

    primaryServer.addSimpleDocs("test_index", 4, 5);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    primaryServer.verifySimpleDocs("test_index", 5);
    replicaServer.verifySimpleDocs("test_index", 5);
  }

  @Test
  public void testCheckIndexId_same() {
    LuceneServer.ReplicationServerImpl.checkIndexId("id1", "id1", false);
  }

  @Test
  public void testCheckIndexId_different() {
    LuceneServer.ReplicationServerImpl.checkIndexId("id1", "id2", false);
  }

  @Test
  public void testCheckIndexId_verifySame() {
    LuceneServer.ReplicationServerImpl.checkIndexId("id1", "id1", true);
  }

  @Test
  public void testCheckIndexId_verifyDifferent() {
    try {
      LuceneServer.ReplicationServerImpl.checkIndexId("id1", "id2", true);
      fail();
    } catch (StatusRuntimeException e) {
      assertEquals(Status.FAILED_PRECONDITION.getCode(), e.getStatus().getCode());
      assertEquals(
          "FAILED_PRECONDITION: Index id mismatch, expected: id2, actual: id1", e.getMessage());
    }
  }
}
