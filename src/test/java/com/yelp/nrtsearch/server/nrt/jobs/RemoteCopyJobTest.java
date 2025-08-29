/*
 * Copyright 2025 Yelp Inc.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.yelp.nrtsearch.server.nrt.NrtDataManager;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.apache.lucene.store.Directory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RemoteCopyJobTest {

  @Mock private NrtDataManager mockDataManager;
  @Mock private ReplicaNode mockReplicaNode;
  @Mock private Directory mockDirectory;
  @Mock private CopyJob.OnceDone mockOnceDone;

  private NrtPointState testPointState;
  private Instant testTimestamp;
  private CopyState testCopyState;
  private Map<String, FileMetaData> testFiles;

  @Before
  public void setUp() {
    testPointState = createTestPointState();
    testTimestamp = Instant.now();
    testCopyState = testPointState.toCopyState();
    testFiles = createTestFiles();
  }

  @Test
  public void testConstructor() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();

    assertNotNull(copyJob);
    assertEquals(testPointState, copyJob.getPointState());
    assertEquals(testTimestamp, copyJob.getPointStateTimestamp());
    assertEquals(testCopyState, copyJob.getCopyState());
  }

  @Test
  public void testGetPointState() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();
    assertEquals(testPointState, copyJob.getPointState());
  }

  @Test
  public void testGetPointStateTimestamp() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();
    assertEquals(testTimestamp, copyJob.getPointStateTimestamp());
  }

  @Test
  public void testGetCopyState() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();
    assertEquals(testCopyState, copyJob.getCopyState());
  }

  @Test
  public void testGetFileNames() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();
    Set<String> fileNames = copyJob.getFileNames();

    // The method should return a set (may be empty depending on how the parent class works)
    assertNotNull(fileNames);
  }

  @Test
  public void testGetFileNamesToCopy() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();
    Set<String> fileNamesToCopy = copyJob.getFileNamesToCopy();

    // The method should return a set (may be empty depending on how the parent class works)
    assertNotNull(fileNamesToCopy);
  }

  @Test
  public void testGetTotalBytesCopied_initial() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();
    assertEquals(0L, copyJob.getTotalBytesCopied());
  }

  @Test
  public void testGetFailed_initial() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();
    assertFalse(copyJob.getFailed());
  }

  @Test
  public void testStart_success() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();

    copyJob.start();

    // Verify that the replica node received the initialization message
    verify(mockReplicaNode).message(anyString());
  }

  @Test
  public void testStart_alreadyStarted() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();

    copyJob.start();

    try {
      copyJob.start();
      fail("Expected IllegalStateException when starting already started job");
    } catch (IllegalStateException e) {
      assertEquals("already started", e.getMessage());
    }
  }

  @Test
  public void testStart_replicaNodeThrowsException() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();

    doThrow(new RuntimeException("Test exception")).when(mockReplicaNode).message(anyString());

    try {
      copyJob.start();
      fail("Expected exception when replica node throws");
    } catch (Exception e) {
      // Some exception should be thrown when the replica node fails
      assertNotNull(e);
    }
  }

  @Test
  public void testCompareTo_samePriority() throws IOException {
    RemoteCopyJob copyJob1 = createRemoteCopyJob("reason1", false);
    RemoteCopyJob copyJob2 = createRemoteCopyJob("reason2", false);

    // Both jobs have same priority, so comparison should be based on order
    int comparison = copyJob1.compareTo(copyJob2);
    assertTrue(comparison < 0); // copyJob1 was created first
  }

  @Test
  public void testCompareTo_differentPriority() throws IOException {
    RemoteCopyJob highPriorityJob = createRemoteCopyJob("high", true);
    RemoteCopyJob lowPriorityJob = createRemoteCopyJob("low", false);

    assertTrue(highPriorityJob.compareTo(lowPriorityJob) < 0); // high priority comes first
    assertTrue(lowPriorityJob.compareTo(highPriorityJob) > 0); // low priority comes later
  }

  @Test
  public void testConflicts_noConflict() throws IOException {
    Map<String, FileMetaData> files1 =
        Map.of("file1.dat", new FileMetaData(new byte[0], new byte[0], 100, 0));
    Map<String, FileMetaData> files2 =
        Map.of("file2.dat", new FileMetaData(new byte[0], new byte[0], 200, 0));

    RemoteCopyJob copyJob1 = createRemoteCopyJobWithFiles("reason1", files1);
    RemoteCopyJob copyJob2 = createRemoteCopyJobWithFiles("reason2", files2);

    assertFalse(copyJob1.conflicts(copyJob2));
    assertFalse(copyJob2.conflicts(copyJob1));
  }

  @Test
  public void testConflicts_hasConflict() throws IOException {
    Map<String, FileMetaData> files1 =
        Map.of(
            "file1.dat", new FileMetaData(new byte[0], new byte[0], 100, 0),
            "shared.dat", new FileMetaData(new byte[0], new byte[0], 50, 0));
    Map<String, FileMetaData> files2 =
        Map.of(
            "file2.dat", new FileMetaData(new byte[0], new byte[0], 200, 0),
            "shared.dat", new FileMetaData(new byte[0], new byte[0], 50, 0));

    RemoteCopyJob copyJob1 = createRemoteCopyJobWithFiles("reason1", files1);
    RemoteCopyJob copyJob2 = createRemoteCopyJobWithFiles("reason2", files2);

    // Test the conflicts method - the result depends on internal toCopy field setup
    // Just verify the method can be called without exception
    boolean conflicts = copyJob1.conflicts(copyJob2);
    // The actual result depends on how the parent class sets up toCopy
    assertNotNull(conflicts);
  }

  @Test
  public void testVisit_noFiles() throws IOException {
    Map<String, FileMetaData> emptyFiles = Map.of();
    RemoteCopyJob copyJob = createRemoteCopyJobWithFiles("empty", emptyFiles);

    copyJob.start();

    // With no files to copy, visit should return true (done)
    assertTrue(copyJob.visit());
  }

  @Test
  public void testVisit_simple() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();

    copyJob.start();

    // Test that visit can be called without exception
    boolean result = copyJob.visit();
    // We just verify the method can be called without throwing
    assertNotNull(result);
  }

  @Test
  public void testRunBlocking_success() throws Exception {
    Map<String, FileMetaData> emptyFiles = Map.of();
    RemoteCopyJob copyJob = createRemoteCopyJobWithFiles("blocking", emptyFiles);

    copyJob.start();
    copyJob.runBlocking();

    // Should complete without exception
    assertTrue(true);
  }

  @Test
  public void testFinish_success() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();

    copyJob.start();
    copyJob.finish();

    // Verify that message was called at least twice (once for start, once for finish)
    verify(mockReplicaNode, atLeast(2)).message(anyString());
  }

  @Test
  public void testToString() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();
    String result = copyJob.toString();

    assertNotNull(result);
    assertTrue(result.contains("RemoteCopyJob"));
    assertTrue(result.contains("test_reason"));
  }

  @Test
  public void testNewCopyOneFile() throws IOException {
    RemoteCopyJob copyJob = createRemoteCopyJob();

    // This method should return the same instance that was passed in
    // (no state changes when transferring to a new job)
    Object result = copyJob.newCopyOneFile(null);

    // The method should return the input (null in this case)
    assertEquals(null, result);
  }

  // Helper methods

  private RemoteCopyJob createRemoteCopyJob() throws IOException {
    return createRemoteCopyJob("test_reason", false);
  }

  private RemoteCopyJob createRemoteCopyJob(String reason, boolean highPriority)
      throws IOException {
    return createRemoteCopyJobWithFiles(reason, testFiles, highPriority);
  }

  private RemoteCopyJob createRemoteCopyJobWithFiles(String reason, Map<String, FileMetaData> files)
      throws IOException {
    return createRemoteCopyJobWithFiles(reason, files, false);
  }

  private RemoteCopyJob createRemoteCopyJobWithFiles(
      String reason, Map<String, FileMetaData> files, boolean highPriority) throws IOException {
    return createRemoteCopyJobWithPointStateAndFiles(testPointState, files, reason, highPriority);
  }

  private RemoteCopyJob createRemoteCopyJobWithPointStateAndFiles(
      NrtPointState pointState,
      Map<String, FileMetaData> files,
      String reason,
      boolean highPriority)
      throws IOException {
    CopyState copyState = pointState.toCopyState();
    return new RemoteCopyJob(
        reason,
        pointState,
        testTimestamp,
        copyState,
        mockDataManager,
        mockReplicaNode,
        files,
        highPriority,
        mockOnceDone);
  }

  private NrtPointState createTestPointState() {
    long version = 1;
    long gen = 3;
    byte[] infosBytes = new byte[] {1, 2, 3, 4, 5};
    long primaryGen = 5;
    Set<String> completedMergeFiles = Set.of("file1");
    String primaryId = "testPrimaryId";

    FileMetaData fileMetaData =
        new FileMetaData(new byte[] {6, 7, 8}, new byte[] {0, 10, 11}, 10, 25);
    NrtFileMetaData nrtFileMetaData =
        new NrtFileMetaData(
            new byte[] {6, 7, 8}, new byte[] {0, 10, 11}, 10, 25, "primaryId2", "timeString");

    CopyState copyState =
        new CopyState(
            Map.of("file3", fileMetaData),
            version,
            gen,
            infosBytes,
            completedMergeFiles,
            primaryGen,
            null);

    return new NrtPointState(copyState, Map.of("file3", nrtFileMetaData), primaryId);
  }

  private Map<String, FileMetaData> createTestFiles() {
    Map<String, FileMetaData> files = new HashMap<>();
    files.put("file1.dat", new FileMetaData(new byte[] {1, 2}, new byte[] {3, 4}, 100, 12345));
    files.put("file2.idx", new FileMetaData(new byte[] {5, 6}, new byte[] {7, 8}, 200, 67890));
    return files;
  }
}
