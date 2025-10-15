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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.nrt.NRTReplicaNode;
import com.yelp.nrtsearch.server.nrt.NrtDataManager;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RemoteCopyJobManagerTest {

  @Mock private NrtDataManager mockDataManager;
  @Mock private NRTReplicaNode mockReplicaNode;
  @Mock private CopyJob.OnceDone mockOnceDone;

  private RemoteCopyJobManager copyJobManager;
  private static final int POLLING_INTERVAL = 1; // 1 second for fast tests

  @Before
  public void setUp() {
    copyJobManager =
        new RemoteCopyJobManager(POLLING_INTERVAL, mockDataManager, mockReplicaNode, null);
  }

  @After
  public void tearDown() throws IOException {
    if (copyJobManager != null) {
      copyJobManager.close();
    }
  }

  @Test
  public void testConstructor() {
    assertNotNull(copyJobManager);
  }

  @Test
  public void testNewCopyJob_withFiles() throws IOException {
    Map<String, FileMetaData> files =
        Map.of("file1", new FileMetaData(new byte[0], new byte[0], 1, 0));

    try {
      copyJobManager.newCopyJob("test", files, null, false, mockOnceDone);
      fail("Expected IllegalArgumentException when files is not null");
    } catch (IllegalArgumentException e) {
      assertEquals("RemoteCopyJobManager does not support merge precopy", e.getMessage());
    }
  }

  @Test
  public void testNewCopyJob_success() throws IOException {
    // Create test data
    NrtPointState pointState = createTestPointState();
    Instant timestamp = Instant.now();
    NrtDataManager.PointStateWithTimestamp pointStateWithTimestamp =
        new NrtDataManager.PointStateWithTimestamp(pointState, timestamp);

    when(mockDataManager.getTargetPointState(null)).thenReturn(pointStateWithTimestamp);

    // Test the method
    CopyJob copyJob = copyJobManager.newCopyJob("test_reason", null, null, true, mockOnceDone);

    // Verify results
    assertNotNull(copyJob);
    assertTrue(copyJob instanceof RemoteCopyJob);

    RemoteCopyJob remoteCopyJob = (RemoteCopyJob) copyJob;
    assertEquals(pointState, remoteCopyJob.getPointState());
    assertEquals(timestamp, remoteCopyJob.getPointStateTimestamp());

    verify(mockDataManager).getTargetPointState(null);
  }

  @Test
  public void testFinishNRTCopy_success() throws IOException {
    // Create a mock RemoteCopyJob
    RemoteCopyJob mockRemoteCopyJob = mock(RemoteCopyJob.class);
    NrtPointState pointState = createTestPointState();
    Instant timestamp = Instant.now();

    when(mockRemoteCopyJob.getFailed()).thenReturn(false);
    when(mockRemoteCopyJob.getPointState()).thenReturn(pointState);
    when(mockRemoteCopyJob.getPointStateTimestamp()).thenReturn(timestamp);

    // Test the method
    copyJobManager.finishNRTCopy(mockRemoteCopyJob);

    // Verify that setLastPointState was called
    verify(mockDataManager).setLastPointState(pointState, timestamp);
  }

  @Test
  public void testFinishNRTCopy_failed() throws IOException {
    // Create a mock RemoteCopyJob that failed
    RemoteCopyJob mockRemoteCopyJob = mock(RemoteCopyJob.class);
    when(mockRemoteCopyJob.getFailed()).thenReturn(true);

    // Test the method
    copyJobManager.finishNRTCopy(mockRemoteCopyJob);

    // Verify that setLastPointState was NOT called
    verify(mockDataManager, never()).setLastPointState(any(), any());
  }

  @Test
  public void testFinishNRTCopy_wrongCopyJobType() throws IOException {
    // Create a mock CopyJob that is not a RemoteCopyJob
    CopyJob mockCopyJob = mock(CopyJob.class);
    when(mockCopyJob.getFailed()).thenReturn(false);

    try {
      copyJobManager.finishNRTCopy(mockCopyJob);
      fail("Expected IllegalArgumentException for wrong copy job type");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Expected copyJob to be instance of RemoteCopyJob"));
    }
  }

  @Test
  public void testStart() throws IOException {
    copyJobManager.start();

    // The thread should be started - we can't easily test the thread directly
    // but we can verify it doesn't throw an exception
    assertTrue(true);
  }

  @Test
  public void testClose() throws IOException {
    copyJobManager.start();
    copyJobManager.close();

    // Verify the manager can be closed without errors
    assertTrue(true);
  }

  @Test
  public void testUpdateTask_newPointAvailable() throws Exception {
    // Set up test data
    NrtPointState targetPointState = createTestPointState();
    NrtDataManager.PointStateWithTimestamp pointStateWithTimestamp =
        new NrtDataManager.PointStateWithTimestamp(targetPointState, Instant.now());

    when(mockDataManager.getTargetPointState(null)).thenReturn(pointStateWithTimestamp);
    when(mockReplicaNode.getCurrentSearchingVersion()).thenReturn(0L); // Lower than target version

    // Create a copy job manager with very short polling interval for testing
    RemoteCopyJobManager shortIntervalManager =
        new RemoteCopyJobManager(1, mockDataManager, mockReplicaNode, null);

    try {
      shortIntervalManager.start();

      // Wait a bit for the update task to run at least once
      Thread.sleep(1200); // Wait 1.2 seconds

      // Verify newNRTPoint was called at least once (it may be called multiple times due to
      // polling)
      verify(mockReplicaNode, atLeast(1))
          .newNRTPoint(targetPointState.primaryGen, targetPointState.version);

    } finally {
      shortIntervalManager.close();
    }
  }

  @Test
  public void testUpdateTask_noNewPoint() throws Exception {
    // Set up test data where current version is already up to date
    NrtPointState targetPointState = createTestPointState();
    NrtDataManager.PointStateWithTimestamp pointStateWithTimestamp =
        new NrtDataManager.PointStateWithTimestamp(targetPointState, Instant.now());

    when(mockDataManager.getTargetPointState(null)).thenReturn(pointStateWithTimestamp);
    when(mockReplicaNode.getCurrentSearchingVersion())
        .thenReturn(targetPointState.version); // Same as target

    // Create a copy job manager with very short polling interval for testing
    RemoteCopyJobManager shortIntervalManager =
        new RemoteCopyJobManager(1, mockDataManager, mockReplicaNode, null);

    try {
      shortIntervalManager.start();

      // Wait a bit for the update task to run
      Thread.sleep(1500); // Wait 1.5 seconds

      // Verify newNRTPoint was NOT called
      verify(mockReplicaNode, never()).newNRTPoint(anyLong(), anyLong());

    } finally {
      shortIntervalManager.close();
    }
  }

  @Test
  public void testUpdateTask_exceptionHandling() throws Exception {
    // Set up test data to throw an exception
    when(mockDataManager.getTargetPointState(null))
        .thenThrow(new RuntimeException("Test exception"));

    // Create a copy job manager with very short polling interval for testing
    RemoteCopyJobManager shortIntervalManager =
        new RemoteCopyJobManager(1, mockDataManager, mockReplicaNode, null);

    try {
      shortIntervalManager.start();

      // Wait a bit for the update task to run
      Thread.sleep(1500); // Wait 1.5 seconds

      // The thread should continue running despite the exception
      // We can't easily verify this, but at least it shouldn't crash the test
      assertTrue(true);

    } finally {
      shortIntervalManager.close();
    }
  }

  @Test
  public void testUpdateTask_interrupted() throws Exception {
    // Create a copy job manager
    RemoteCopyJobManager shortIntervalManager =
        new RemoteCopyJobManager(1, mockDataManager, mockReplicaNode, null);

    try {
      shortIntervalManager.start();

      // Wait a short time then close (which interrupts the thread)
      Thread.sleep(500);
      shortIntervalManager.close();

      // Verify it completes without hanging
      assertTrue(true);

    } catch (Exception e) {
      // Should not throw any exceptions
      fail("Unexpected exception: " + e.getMessage());
    }
  }

  /** Create a test NrtPointState for testing purposes. */
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
}
