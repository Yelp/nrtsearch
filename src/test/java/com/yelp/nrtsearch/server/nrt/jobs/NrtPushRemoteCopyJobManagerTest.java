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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NrtPushRemoteCopyJobManagerTest {

  @Mock private NrtDataManager mockDataManager;
  @Mock private NRTReplicaNode mockReplicaNode;
  @Mock private CopyJob.OnceDone mockOnceDone;

  private NrtPushRemoteCopyJobManager copyJobManager;

  @Before
  public void setUp() {
    copyJobManager = new NrtPushRemoteCopyJobManager(mockDataManager, mockReplicaNode);
  }

  @Test
  public void testNewCopyJob_withFiles() throws IOException {
    Map<String, FileMetaData> files =
        Map.of("file1", new FileMetaData(new byte[0], new byte[0], 1, 0));

    try {
      copyJobManager.newCopyJob("test", files, null, false, mockOnceDone);
      fail("Expected IllegalArgumentException when files is not null");
    } catch (IllegalArgumentException e) {
      assertEquals("NrtPushRemoteCopyJobManager does not support merge precopy", e.getMessage());
    }
  }

  @Test
  public void testNewCopyJob_success() throws IOException {
    NrtPointState pointState = createTestPointState();
    Instant timestamp = Instant.now();
    NrtDataManager.PointStateWithTimestamp pointStateWithTimestamp =
        new NrtDataManager.PointStateWithTimestamp(pointState, timestamp);

    when(mockDataManager.getTargetPointState(null)).thenReturn(pointStateWithTimestamp);

    CopyJob copyJob = copyJobManager.newCopyJob("test_reason", null, null, true, mockOnceDone);

    assertNotNull(copyJob);
    assertTrue(copyJob instanceof RemoteCopyJob);

    RemoteCopyJob remoteCopyJob = (RemoteCopyJob) copyJob;
    assertEquals(pointState, remoteCopyJob.getPointState());
    assertEquals(timestamp, remoteCopyJob.getPointStateTimestamp());

    verify(mockDataManager).getTargetPointState(null);
  }

  @Test
  public void testNewCopyJob_nullPointState() throws IOException {
    NrtDataManager.PointStateWithTimestamp pointStateWithTimestamp =
        new NrtDataManager.PointStateWithTimestamp(null, null);

    when(mockDataManager.getTargetPointState(null)).thenReturn(pointStateWithTimestamp);

    try {
      copyJobManager.newCopyJob("test_reason", null, null, true, mockOnceDone);
      fail("Expected IOException when point state is null");
    } catch (IOException e) {
      assertEquals("No point state available from S3", e.getMessage());
    }
  }

  @Test
  public void testFinishNRTCopy_success() throws IOException {
    RemoteCopyJob mockRemoteCopyJob = mock(RemoteCopyJob.class);
    NrtPointState pointState = createTestPointState();
    Instant timestamp = Instant.now();

    when(mockRemoteCopyJob.getFailed()).thenReturn(false);
    when(mockRemoteCopyJob.getPointState()).thenReturn(pointState);
    when(mockRemoteCopyJob.getPointStateTimestamp()).thenReturn(timestamp);

    copyJobManager.finishNRTCopy(mockRemoteCopyJob);

    verify(mockDataManager).setLastPointState(pointState, timestamp);
  }

  @Test
  public void testFinishNRTCopy_failed() throws IOException {
    RemoteCopyJob mockRemoteCopyJob = mock(RemoteCopyJob.class);
    when(mockRemoteCopyJob.getFailed()).thenReturn(true);

    copyJobManager.finishNRTCopy(mockRemoteCopyJob);

    verify(mockDataManager, never()).setLastPointState(any(), any());
  }

  @Test
  public void testFinishNRTCopy_wrongCopyJobType() throws IOException {
    CopyJob mockCopyJob = mock(CopyJob.class);
    when(mockCopyJob.getFailed()).thenReturn(false);

    try {
      copyJobManager.finishNRTCopy(mockCopyJob);
      fail("Expected IllegalArgumentException for wrong copy job type");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Expected copyJob to be instance of RemoteCopyJob"));
    }
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
}
