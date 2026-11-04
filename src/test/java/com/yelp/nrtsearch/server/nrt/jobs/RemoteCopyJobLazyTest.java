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
package com.yelp.nrtsearch.server.nrt.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.config.LazyRemoteDataConfig;
import com.yelp.nrtsearch.server.nrt.NrtDataManager;
import com.yelp.nrtsearch.server.nrt.lazy.LazyRemoteDirectory;
import com.yelp.nrtsearch.server.nrt.lazy.RemoteBlockCache;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Lazy remote stubs written by {@link RemoteCopyJob} follow the copy job temp file lifecycle. */
public class RemoteCopyJobLazyTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private LazyRemoteDirectory lazyDir;
  private NIOFSDirectory base;
  private ReplicaNode replica;
  private NrtDataManager dataManager;
  private NrtPointState pointState;

  @Before
  public void setUp() throws Exception {
    Path path = folder.newFolder("index").toPath();
    base = new NIOFSDirectory(path);
    lazyDir =
        new LazyRemoteDirectory(
            base,
            path,
            "service",
            mock(RemoteBackend.class),
            new RemoteBlockCache.Memory(1024, 1024),
            new LazyRemoteDataConfig(
                true, List.of("vec"), LazyRemoteDataConfig.CacheType.MEMORY, "", 1, 4));

    NrtFileMetaData meta =
        new NrtFileMetaData(new byte[] {1, 2}, new byte[] {3, 4}, 100, 99, "primary", "time");
    CopyState copyState =
        new CopyState(
            Map.of("_0.vec", meta.toFileMetaData()), 1, 2, new byte[] {1}, Set.of(), 3, null);
    pointState = new NrtPointState(copyState, Map.of("_0.vec", meta), "primary");

    replica = mock(ReplicaNode.class);
    when(replica.getDirectory()).thenReturn(lazyDir);
    // every file needs copying
    when(replica.getFilesToCopy(any()))
        .thenAnswer(
            inv -> {
              Map<String, FileMetaData> files = inv.getArgument(0);
              return new ArrayList<>(files.entrySet());
            });
    dataManager = mock(NrtDataManager.class);
    when(dataManager.getIndexIdentifier()).thenReturn("index-id");
  }

  private RemoteCopyJob job() throws Exception {
    return new RemoteCopyJob(
        "test",
        pointState,
        Instant.now(),
        pointState.toCopyState(),
        dataManager,
        replica,
        pointState.toCopyState().files(),
        false,
        mock(CopyJob.OnceDone.class));
  }

  @Test
  public void testStubWrittenAsTempAndRenamedOnFinish() throws Exception {
    RemoteCopyJob job = job();
    job.start();
    job.runBlocking();

    // before finish only a temp file exists, the stub is not visible
    assertFalse(lazyDir.hasStub("_0.vec"));
    List<String> files = Arrays.asList(lazyDir.listAll());
    assertEquals(1, files.size());
    assertTrue(files.get(0), files.get(0).endsWith(".tmp"));
    // stubs are never downloaded
    verify(dataManager, never()).downloadIndexFile(any(), any());

    job.finish();
    assertTrue(lazyDir.hasStub("_0.vec"));
    assertEquals(List.of("_0.vec"), Arrays.asList(lazyDir.listAll()));
    assertEquals(100, lazyDir.fileLength("_0.vec"));
    assertEquals(100, job.getTotalBytesCopied());
  }

  @Test
  public void testCancelledJobLeavesReplicaFilesUnchanged() throws Exception {
    // the replica already has _0.vec as a real file
    try (IndexOutput out = base.createOutput("_0.vec", IOContext.DEFAULT)) {
      out.writeByte((byte) 7);
    }
    RemoteCopyJob job = job();
    job.start();
    job.runBlocking();
    // what CopyJob.cancel does with copiedFiles: delete every temp file
    for (String tmp : Arrays.asList(lazyDir.listAll())) {
      if (tmp.endsWith(".tmp")) {
        lazyDir.deleteFile(tmp);
      }
    }
    assertEquals(List.of("_0.vec"), Arrays.asList(lazyDir.listAll()));
    assertFalse(lazyDir.hasStub("_0.vec"));
    assertEquals(1, lazyDir.fileLength("_0.vec"));
  }
}
