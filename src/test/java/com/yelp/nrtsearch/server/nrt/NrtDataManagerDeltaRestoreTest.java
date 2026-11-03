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
package com.yelp.nrtsearch.server.nrt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NrtDataManagerDeltaRestoreTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  /** Write a file with a valid Lucene header and footer, return its metadata. */
  private static NrtFileMetaData writeLuceneFile(Path dir, String name, String content)
      throws IOException {
    try (FSDirectory fsDir = FSDirectory.open(dir)) {
      try (IndexOutput out = fsDir.createOutput(name, IOContext.DEFAULT)) {
        CodecUtil.writeHeader(out, "test", 0);
        out.writeString(content);
        CodecUtil.writeFooter(out);
      }
      try (IndexInput in = fsDir.openInput(name, IOContext.READONCE)) {
        return new NrtFileMetaData(
            new byte[0], new byte[0], in.length(), CodecUtil.retrieveChecksum(in), "p", "t");
      }
    }
  }

  @Test
  public void testKeepsMatchingFilesAndDeletesOthers() throws IOException {
    Path local = folder.newFolder("local").toPath();
    Path other = folder.newFolder("other").toPath();

    NrtFileMetaData same = writeLuceneFile(local, "_0.cfs", "same");
    writeLuceneFile(other, "_0.cfs", "same");
    writeLuceneFile(local, "_1.cfs", "local version");
    NrtFileMetaData changed = writeLuceneFile(other, "_1.cfs", "remote version!!");
    NrtFileMetaData missing = writeLuceneFile(other, "_2.cfs", "only remote");
    writeLuceneFile(local, "_3.cfs", "stale local only");
    Files.writeString(local.resolve("segments_5"), "old segments");
    Files.writeString(local.resolve("write.lock"), "");

    Map<String, NrtFileMetaData> pointFiles = new HashMap<>();
    pointFiles.put("_0.cfs", same);
    pointFiles.put("_1.cfs", changed);
    pointFiles.put("_2.cfs", missing);

    Map<String, NrtFileMetaData> toDownload = NrtDataManager.prepareDeltaRestore(local, pointFiles);

    assertEquals(Map.of("_1.cfs", changed, "_2.cfs", missing), toDownload);
    assertTrue(Files.exists(local.resolve("_0.cfs")));
    assertFalse(Files.exists(local.resolve("_1.cfs")));
    assertFalse(Files.exists(local.resolve("_3.cfs")));
    assertFalse(Files.exists(local.resolve("segments_5")));
    assertFalse(Files.exists(local.resolve("write.lock")));
  }

  @Test
  public void testSameLengthDifferentChecksumIsDownloaded() throws IOException {
    Path local = folder.newFolder("local").toPath();
    Path other = folder.newFolder("other").toPath();
    writeLuceneFile(local, "_0.cfs", "aaaa");
    NrtFileMetaData remote = writeLuceneFile(other, "_0.cfs", "bbbb");

    Map<String, NrtFileMetaData> toDownload =
        NrtDataManager.prepareDeltaRestore(local, Map.of("_0.cfs", remote));
    assertEquals(Map.of("_0.cfs", remote), toDownload);
    assertFalse(Files.exists(local.resolve("_0.cfs")));
  }

  @Test
  public void testCorruptLocalFileIsDownloaded() throws IOException {
    Path local = folder.newFolder("local").toPath();
    Path other = folder.newFolder("other").toPath();
    NrtFileMetaData remote = writeLuceneFile(other, "_0.cfs", "content");
    Files.write(local.resolve("_0.cfs"), new byte[(int) remote.length]);

    Map<String, NrtFileMetaData> toDownload =
        NrtDataManager.prepareDeltaRestore(local, Map.of("_0.cfs", remote));
    assertEquals(Map.of("_0.cfs", remote), toDownload);
  }

  @Test
  public void testEmptyLocalDir() throws IOException {
    Path local = folder.newFolder("local").toPath();
    Path other = folder.newFolder("other").toPath();
    NrtFileMetaData remote = writeLuceneFile(other, "_0.cfs", "content");
    assertEquals(
        Map.of("_0.cfs", remote),
        NrtDataManager.prepareDeltaRestore(local, Map.of("_0.cfs", remote)));
  }

  @Test
  public void testStartOptionsIsolatedReplicaOverride() {
    com.yelp.nrtsearch.server.config.IsolatedReplicaConfig server =
        new com.yelp.nrtsearch.server.config.IsolatedReplicaConfig(false, 120, 0, 0);
    com.yelp.nrtsearch.server.config.IsolatedReplicaConfig standby =
        new com.yelp.nrtsearch.server.config.IsolatedReplicaConfig(true, 10, 0, 0);
    NrtDataManager defaults = new NrtDataManager("s", "i", "p", null, null, true, true, true);
    assertFalse(defaults.isStandbyFollower());
    assertTrue(defaults.getIsolatedReplicaConfig(server) == server);

    NrtDataManager follower =
        new NrtDataManager(
            "s",
            "i",
            "p",
            null,
            null,
            true,
            true,
            true,
            new NrtDataManager.StartOptions(false, standby));
    assertTrue(follower.isStandbyFollower());
    assertTrue(follower.getIsolatedReplicaConfig(server) == standby);
  }

  @Test
  public void testSubdirectoriesRemoved() throws IOException {
    Path local = folder.newFolder("local").toPath();
    Path other = folder.newFolder("other").toPath();
    NrtFileMetaData remote = writeLuceneFile(other, "_0.cfs", "content");
    Path sub = Files.createDirectories(local.resolve("stale_dir"));
    Files.writeString(sub.resolve("f"), "x");
    assertEquals(
        Map.of("_0.cfs", remote),
        NrtDataManager.prepareDeltaRestore(local, Map.of("_0.cfs", remote)));
    assertFalse(Files.exists(sub));
  }

  /** Without remote data to compare against, keepMatchingLocalFiles must still wipe local data. */
  @Test
  public void testStaleLocalFilesRemovedWithoutRemoteData() throws IOException {
    Path local = folder.newFolder("local").toPath();
    Files.writeString(local.resolve("_0.cfs"), "stale");
    com.yelp.nrtsearch.server.remote.RemoteBackend backend =
        org.mockito.Mockito.mock(com.yelp.nrtsearch.server.remote.RemoteBackend.class);
    NrtDataManager manager =
        new NrtDataManager(
            "s",
            "i",
            "p",
            backend,
            com.yelp.nrtsearch.server.grpc.RestoreIndex.newBuilder()
                .setDeleteExistingData(true)
                .build(),
            true,
            true,
            true,
            new NrtDataManager.StartOptions(true, null));
    manager.restoreIfNeeded(local);
    assertFalse(Files.exists(local.resolve("_0.cfs")));
  }
}
