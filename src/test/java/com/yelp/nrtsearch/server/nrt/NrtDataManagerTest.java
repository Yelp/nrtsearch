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
package com.yelp.nrtsearch.server.nrt;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.grpc.RestoreIndex;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.RemoteUtils;
import com.yelp.nrtsearch.server.utils.TimeStringUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

public class NrtDataManagerTest {
  private static final String SERVICE_NAME = "test_service";
  private static final String INDEX_NAME = "test_index";
  private static final String PRIMARY_ID = "primayId";

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testHasRestoreData_noRestoreIndex() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, false);
    assertFalse(nrtDataManager.hasRestoreData());
  }

  @Test
  public void testHasRestoreData_notExist() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    when(mockRemoteBackend.exists(
            SERVICE_NAME, INDEX_NAME, RemoteBackend.IndexResourceType.POINT_STATE))
        .thenReturn(false);
    RestoreIndex restoreIndex =
        RestoreIndex.newBuilder().setServiceName(SERVICE_NAME).setResourceName(INDEX_NAME).build();
    NrtDataManager nrtDataManager =
        new NrtDataManager(
            SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, restoreIndex, false);
    assertFalse(nrtDataManager.hasRestoreData());
  }

  @Test
  public void testHasRestoreData_exist() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    when(mockRemoteBackend.exists(
            SERVICE_NAME, INDEX_NAME, RemoteBackend.IndexResourceType.POINT_STATE))
        .thenReturn(true);
    RestoreIndex restoreIndex =
        RestoreIndex.newBuilder().setServiceName(SERVICE_NAME).setResourceName(INDEX_NAME).build();
    NrtDataManager nrtDataManager =
        new NrtDataManager(
            SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, restoreIndex, false);
    assertTrue(nrtDataManager.hasRestoreData());
  }

  @Test
  public void testDoRemoteCommit_true() {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    assertTrue(nrtDataManager.doRemoteCommit());
  }

  @Test
  public void testDoRemoteCommit_false() {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, false);
    assertFalse(nrtDataManager.doRemoteCommit());
  }

  @Test
  public void testAlreadyStarted() {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    nrtDataManager.startUploadManager(mock(NRTPrimaryNode.class), folder.getRoot().toPath());

    try {
      nrtDataManager.startUploadManager(mock(NRTPrimaryNode.class), folder.getRoot().toPath());
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Upload manager already started", e.getMessage());
    }
  }

  @Test
  public void testRestoreIfNeeded_noRestoreIndex() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    folder.newFile("test_file");
    nrtDataManager.restoreIfNeeded(folder.getRoot().toPath());
    assertArrayEquals(new String[] {"test_file"}, folder.getRoot().list());
    assertNull(nrtDataManager.getLastPointState());
  }

  @Test
  public void testRestoreIfNeeded_notExist_deleteExisting() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    when(mockRemoteBackend.exists(
            SERVICE_NAME, INDEX_NAME, RemoteBackend.IndexResourceType.POINT_STATE))
        .thenReturn(false);
    RestoreIndex restoreIndex =
        RestoreIndex.newBuilder()
            .setServiceName(SERVICE_NAME)
            .setResourceName(INDEX_NAME)
            .setDeleteExistingData(true)
            .build();
    NrtDataManager nrtDataManager =
        new NrtDataManager(
            SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, restoreIndex, true);
    folder.newFile("test_file");
    nrtDataManager.restoreIfNeeded(folder.getRoot().toPath());
    assertArrayEquals(new String[0], folder.getRoot().list());
    assertNull(nrtDataManager.getLastPointState());
  }

  @Test
  public void testRestoreIfNeeded_notExist_keepExisting() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    when(mockRemoteBackend.exists(
            SERVICE_NAME, INDEX_NAME, RemoteBackend.IndexResourceType.POINT_STATE))
        .thenReturn(false);
    RestoreIndex restoreIndex =
        RestoreIndex.newBuilder()
            .setServiceName(SERVICE_NAME)
            .setResourceName(INDEX_NAME)
            .setDeleteExistingData(false)
            .build();
    NrtDataManager nrtDataManager =
        new NrtDataManager(
            SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, restoreIndex, true);
    folder.newFile("test_file");
    nrtDataManager.restoreIfNeeded(folder.getRoot().toPath());
    assertArrayEquals(new String[] {"test_file"}, folder.getRoot().list());
    assertNull(nrtDataManager.getLastPointState());
  }

  @Test
  public void testRestoreIfNeeded_exist_deleteExisting() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    when(mockRemoteBackend.exists(
            SERVICE_NAME, INDEX_NAME, RemoteBackend.IndexResourceType.POINT_STATE))
        .thenReturn(true);

    FileMetaData fileMetaData =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 15, 16);
    CopyState copyState =
        new CopyState(
            Map.of("file1", fileMetaData),
            5,
            6,
            new byte[] {1, 2, 3},
            Set.of("merged_file"),
            7,
            null);
    NrtFileMetaData nrtFileMetaData = new NrtFileMetaData(fileMetaData, PRIMARY_ID, "timestamp");
    Map<String, NrtFileMetaData> nrtFileMetaDataMap = Map.of("file1", nrtFileMetaData);
    NrtPointState nrtPointState = new NrtPointState(copyState, nrtFileMetaDataMap, PRIMARY_ID);
    byte[] pointStateBytes = RemoteUtils.pointStateToUtf8(nrtPointState);
    when(mockRemoteBackend.downloadPointState(SERVICE_NAME, INDEX_NAME))
        .thenReturn(
            new RemoteBackend.InputStreamWithTimestamp(
                new ByteArrayInputStream(pointStateBytes), Instant.now()));

    RestoreIndex restoreIndex =
        RestoreIndex.newBuilder()
            .setServiceName(SERVICE_NAME)
            .setResourceName(INDEX_NAME)
            .setDeleteExistingData(true)
            .build();
    NrtDataManager nrtDataManager =
        new NrtDataManager(
            SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, restoreIndex, true);
    folder.newFile("test_file");
    nrtDataManager.restoreIfNeeded(folder.getRoot().toPath());
    assertArrayEquals(new String[] {"segments_6"}, folder.getRoot().list());
    assertEquals(nrtPointState, nrtDataManager.getLastPointState());

    verify(mockRemoteBackend, times(1)).downloadPointState(SERVICE_NAME, INDEX_NAME);
    verify(mockRemoteBackend, times(1))
        .downloadIndexFiles(
            SERVICE_NAME, INDEX_NAME, folder.getRoot().toPath(), nrtFileMetaDataMap);
  }

  @Test
  public void testRestoreIfNeeded_exist_keepExisting() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    when(mockRemoteBackend.exists(
            SERVICE_NAME, INDEX_NAME, RemoteBackend.IndexResourceType.POINT_STATE))
        .thenReturn(true);

    FileMetaData fileMetaData =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 15, 16);
    CopyState copyState =
        new CopyState(
            Map.of("file1", fileMetaData),
            5,
            6,
            new byte[] {1, 2, 3},
            Set.of("merged_file"),
            7,
            null);
    NrtFileMetaData nrtFileMetaData = new NrtFileMetaData(fileMetaData, PRIMARY_ID, "timestamp");
    Map<String, NrtFileMetaData> nrtFileMetaDataMap = Map.of("file1", nrtFileMetaData);
    NrtPointState nrtPointState = new NrtPointState(copyState, nrtFileMetaDataMap, PRIMARY_ID);
    byte[] pointStateBytes = RemoteUtils.pointStateToUtf8(nrtPointState);
    Instant nrtPointTimestamp = Instant.now();
    when(mockRemoteBackend.downloadPointState(SERVICE_NAME, INDEX_NAME))
        .thenReturn(
            new RemoteBackend.InputStreamWithTimestamp(
                new ByteArrayInputStream(pointStateBytes), nrtPointTimestamp));

    RestoreIndex restoreIndex =
        RestoreIndex.newBuilder()
            .setServiceName(SERVICE_NAME)
            .setResourceName(INDEX_NAME)
            .setDeleteExistingData(false)
            .build();
    NrtDataManager nrtDataManager =
        new NrtDataManager(
            SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, restoreIndex, true);
    folder.newFile("test_file");
    nrtDataManager.restoreIfNeeded(folder.getRoot().toPath());
    Set<String> expectedFiles = Set.of("test_file", "segments_6");
    Set<String> actualFiles = Set.of(folder.getRoot().list());
    assertEquals(expectedFiles, actualFiles);
    assertEquals(nrtPointState, nrtDataManager.getLastPointState());
    assertEquals(nrtPointTimestamp, nrtDataManager.getLastPointTimestamp());

    verify(mockRemoteBackend, times(1)).downloadPointState(SERVICE_NAME, INDEX_NAME);
    verify(mockRemoteBackend, times(1))
        .downloadIndexFiles(
            SERVICE_NAME, INDEX_NAME, folder.getRoot().toPath(), nrtFileMetaDataMap);
  }

  @Test
  public void testWriteSegmentsFile() throws IOException {
    NrtDataManager.writeSegmentsFile(new byte[] {1, 2, 3, 4, 5}, 14, folder.getRoot().toPath());
    assertArrayEquals(new String[] {"segments_e"}, folder.getRoot().list());
    byte[] fileBytes = Files.readAllBytes(folder.getRoot().toPath().resolve("segments_e"));
    assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, fileBytes);
  }

  @Test
  public void testEnqueueUpload_closed() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    nrtDataManager.close();

    try {
      nrtDataManager.enqueueUpload(null, List.of());
      fail();
    } catch (IllegalStateException e) {
      assertEquals("NrtDataManager is closed", e.getMessage());
    }
  }

  @Test
  public void testEnqueueUpload_noCurrentTask() {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    CopyState mockCopyState = mock(CopyState.class);
    List<RefreshUploadFuture> refreshUploadFutures = List.of(mock(RefreshUploadFuture.class));
    nrtDataManager.enqueueUpload(mockCopyState, refreshUploadFutures);

    assertSame(mockCopyState, nrtDataManager.getCurrentUploadTask().copyState());
    assertSame(refreshUploadFutures, nrtDataManager.getCurrentUploadTask().watchers());
    assertNull(nrtDataManager.getNextUploadTask());
  }

  @Test
  public void testEnqueueUpload_noNextTask() {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    CopyState mockCopyState = mock(CopyState.class);
    List<RefreshUploadFuture> refreshUploadFutures = List.of(mock(RefreshUploadFuture.class));
    nrtDataManager.enqueueUpload(mockCopyState, refreshUploadFutures);

    CopyState mockCopyState2 = mock(CopyState.class);
    List<RefreshUploadFuture> refreshUploadFutures2 = List.of(mock(RefreshUploadFuture.class));
    nrtDataManager.enqueueUpload(mockCopyState2, refreshUploadFutures2);

    assertSame(mockCopyState, nrtDataManager.getCurrentUploadTask().copyState());
    assertSame(refreshUploadFutures, nrtDataManager.getCurrentUploadTask().watchers());
    assertSame(mockCopyState2, nrtDataManager.getNextUploadTask().copyState());
    assertSame(refreshUploadFutures2, nrtDataManager.getNextUploadTask().watchers());
  }

  @Test
  public void testEnqueueUpload_mergeTasks() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    nrtDataManager.startWithoutThread(mockPrimaryNode, folder.getRoot().toPath());

    CopyState mockCopyState = mock(CopyState.class);
    List<RefreshUploadFuture> refreshUploadFutures = List.of(mock(RefreshUploadFuture.class));
    nrtDataManager.enqueueUpload(mockCopyState, refreshUploadFutures);

    CopyState mockCopyState2 = mock(CopyState.class);
    RefreshUploadFuture mockRefreshUploadFuture = mock(RefreshUploadFuture.class);
    List<RefreshUploadFuture> refreshUploadFutures2 = List.of(mockRefreshUploadFuture);
    nrtDataManager.enqueueUpload(mockCopyState2, refreshUploadFutures2);

    CopyState mockCopyState3 = mock(CopyState.class);
    RefreshUploadFuture mockRefreshUploadFuture2 = mock(RefreshUploadFuture.class);
    List<RefreshUploadFuture> refreshUploadFutures3 = List.of(mockRefreshUploadFuture2);
    nrtDataManager.enqueueUpload(mockCopyState3, refreshUploadFutures3);

    assertSame(mockCopyState, nrtDataManager.getCurrentUploadTask().copyState());
    assertSame(refreshUploadFutures, nrtDataManager.getCurrentUploadTask().watchers());
    assertSame(mockCopyState3, nrtDataManager.getNextUploadTask().copyState());
    assertEquals(
        List.of(mockRefreshUploadFuture, mockRefreshUploadFuture2),
        nrtDataManager.getNextUploadTask().watchers());

    verify(mockPrimaryNode, times(1)).releaseCopyState(mockCopyState2);
    verifyNoMoreInteractions(mockPrimaryNode);
  }

  @Test
  public void testMergeTasks_greaterVersion() throws IOException {
    CopyState copyState = new CopyState(Map.of(), 5, 6, new byte[0], Set.of(), 7, null);
    CopyState copyState2 = new CopyState(Map.of(), 6, 6, new byte[0], Set.of(), 7, null);
    List<RefreshUploadFuture> refreshUploadFutures = List.of(mock(RefreshUploadFuture.class));
    List<RefreshUploadFuture> refreshUploadFutures2 = List.of(mock(RefreshUploadFuture.class));
    NrtDataManager.UploadTask uploadTask =
        new NrtDataManager.UploadTask(copyState, refreshUploadFutures);
    NrtDataManager.UploadTask uploadTask2 =
        new NrtDataManager.UploadTask(copyState2, refreshUploadFutures2);

    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    NrtDataManager.UploadTask mergedTask =
        NrtDataManager.mergeTasks(uploadTask, uploadTask2, mockPrimaryNode);
    assertSame(copyState2, mergedTask.copyState());
    assertEquals(
        List.of(refreshUploadFutures.getFirst(), refreshUploadFutures2.getFirst()),
        mergedTask.watchers());

    verify(mockPrimaryNode, times(1)).releaseCopyState(copyState);
    verifyNoMoreInteractions(mockPrimaryNode);
  }

  @Test
  public void testMergeTasks_sameVersion() throws IOException {
    CopyState copyState = new CopyState(Map.of(), 6, 6, new byte[0], Set.of(), 7, null);
    CopyState copyState2 = new CopyState(Map.of(), 6, 6, new byte[0], Set.of(), 7, null);
    List<RefreshUploadFuture> refreshUploadFutures = List.of(mock(RefreshUploadFuture.class));
    List<RefreshUploadFuture> refreshUploadFutures2 = List.of(mock(RefreshUploadFuture.class));
    NrtDataManager.UploadTask uploadTask =
        new NrtDataManager.UploadTask(copyState, refreshUploadFutures);
    NrtDataManager.UploadTask uploadTask2 =
        new NrtDataManager.UploadTask(copyState2, refreshUploadFutures2);

    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    NrtDataManager.UploadTask mergedTask =
        NrtDataManager.mergeTasks(uploadTask, uploadTask2, mockPrimaryNode);
    assertSame(copyState2, mergedTask.copyState());
    assertEquals(
        List.of(refreshUploadFutures.getFirst(), refreshUploadFutures2.getFirst()),
        mergedTask.watchers());

    verify(mockPrimaryNode, times(1)).releaseCopyState(copyState);
    verifyNoMoreInteractions(mockPrimaryNode);
  }

  @Test
  public void testMergeTasks_lowerVersion() throws IOException {
    CopyState copyState = new CopyState(Map.of(), 6, 6, new byte[0], Set.of(), 7, null);
    CopyState copyState2 = new CopyState(Map.of(), 5, 6, new byte[0], Set.of(), 7, null);
    List<RefreshUploadFuture> refreshUploadFutures = List.of(mock(RefreshUploadFuture.class));
    List<RefreshUploadFuture> refreshUploadFutures2 = List.of(mock(RefreshUploadFuture.class));
    NrtDataManager.UploadTask uploadTask =
        new NrtDataManager.UploadTask(copyState, refreshUploadFutures);
    NrtDataManager.UploadTask uploadTask2 =
        new NrtDataManager.UploadTask(copyState2, refreshUploadFutures2);

    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    NrtDataManager.UploadTask mergedTask =
        NrtDataManager.mergeTasks(uploadTask, uploadTask2, mockPrimaryNode);
    assertSame(copyState, mergedTask.copyState());
    assertEquals(
        List.of(refreshUploadFutures.getFirst(), refreshUploadFutures2.getFirst()),
        mergedTask.watchers());

    verify(mockPrimaryNode, times(1)).releaseCopyState(copyState2);
    verifyNoMoreInteractions(mockPrimaryNode);
  }

  @Test
  public void testClose_noThread() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    nrtDataManager.startWithoutThread(mockPrimaryNode, folder.getRoot().toPath());
    nrtDataManager.close();

    verifyNoInteractions(mockRemoteBackend, mockPrimaryNode);
  }

  @Test
  public void testClose_withThread() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    nrtDataManager.startUploadManager(mockPrimaryNode, folder.getRoot().toPath());
    nrtDataManager.close();

    verifyNoInteractions(mockRemoteBackend, mockPrimaryNode);
  }

  @Test
  public void testClose_cleansUpTasks() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    nrtDataManager.startWithoutThread(mockPrimaryNode, folder.getRoot().toPath());

    CopyState mockCopyState = mock(CopyState.class);
    RefreshUploadFuture mockRefreshUploadFuture = mock(RefreshUploadFuture.class);
    List<RefreshUploadFuture> refreshUploadFutures = List.of(mockRefreshUploadFuture);
    nrtDataManager.enqueueUpload(mockCopyState, refreshUploadFutures);

    CopyState mockCopyState2 = mock(CopyState.class);
    RefreshUploadFuture mockRefreshUploadFuture2 = mock(RefreshUploadFuture.class);
    List<RefreshUploadFuture> refreshUploadFutures2 = List.of(mockRefreshUploadFuture2);
    nrtDataManager.enqueueUpload(mockCopyState2, refreshUploadFutures2);
    nrtDataManager.close();

    verify(mockPrimaryNode, times(1)).releaseCopyState(mockCopyState);
    verify(mockPrimaryNode, times(1)).releaseCopyState(mockCopyState2);
    verify(mockRefreshUploadFuture, times(1)).setDone(any(IllegalStateException.class));
    verify(mockRefreshUploadFuture2, times(1)).setDone(any(IllegalStateException.class));
    verifyNoMoreInteractions(mockPrimaryNode, mockRefreshUploadFuture, mockRefreshUploadFuture2);
    verifyNoInteractions(mockRemoteBackend);
  }

  @Test
  public void testUploadManagerThread_processTask()
      throws ExecutionException, InterruptedException, TimeoutException, IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    nrtDataManager.startUploadManager(mockPrimaryNode, folder.getRoot().toPath());

    FileMetaData fileMetaData =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 15, 16);
    CopyState copyState =
        new CopyState(
            Map.of("file1", fileMetaData),
            5,
            6,
            new byte[] {1, 2, 3},
            Set.of("merged_file"),
            7,
            null);
    RefreshUploadFuture refreshUploadFuture = new RefreshUploadFuture();
    nrtDataManager.enqueueUpload(copyState, List.of(refreshUploadFuture));

    refreshUploadFuture.get(30, java.util.concurrent.TimeUnit.SECONDS);
    waitUntilDone(nrtDataManager);

    NrtFileMetaData nrtFileMetaData = new NrtFileMetaData(fileMetaData, PRIMARY_ID, "timestamp");
    NrtPointState nrtPointState =
        new NrtPointState(copyState, Map.of("file1", nrtFileMetaData), PRIMARY_ID);
    verifyPointStates(nrtPointState, nrtDataManager.getLastPointState());
    byte[] pointStateBytes = RemoteUtils.pointStateToUtf8(nrtDataManager.getLastPointState());

    verify(mockPrimaryNode, times(1)).releaseCopyState(copyState);
    verify(mockRemoteBackend, times(1))
        .uploadPointState(
            eq(SERVICE_NAME),
            eq(INDEX_NAME),
            argThat(
                arg -> {
                  verifyPointStates(nrtPointState, arg);
                  return true;
                }),
            eq(pointStateBytes));
    verify(mockRemoteBackend, times(1))
        .uploadIndexFiles(
            eq(SERVICE_NAME),
            eq(INDEX_NAME),
            eq(folder.getRoot().toPath()),
            argThat(
                arg -> {
                  verifyFilesMetaData(Map.of("file1", nrtFileMetaData), arg);
                  return true;
                }));
    verifyNoMoreInteractions(mockPrimaryNode, mockRemoteBackend);
  }

  @Test
  public void testUploadManagerThread_processMultipleTasks()
      throws ExecutionException, InterruptedException, TimeoutException, IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    nrtDataManager.startUploadManager(mockPrimaryNode, folder.getRoot().toPath());

    FileMetaData fileMetaData1 =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 15, 16);
    FileMetaData fileMetaData2 =
        new FileMetaData(new byte[] {7, 8, 9}, new byte[] {10, 11, 12}, 25, 26);
    FileMetaData fileMetaData3 =
        new FileMetaData(new byte[] {13, 14, 15}, new byte[] {16, 17, 18}, 35, 36);
    CopyState copyState1 =
        new CopyState(
            Map.of("file1", fileMetaData1, "file2", fileMetaData2),
            5,
            6,
            new byte[] {1, 2, 3},
            Set.of("merged_file1"),
            7,
            null);
    CopyState copyState2 =
        new CopyState(
            Map.of("file2", fileMetaData2, "file3", fileMetaData3),
            6,
            6,
            new byte[] {4, 5, 6},
            Set.of("merged_file2"),
            7,
            null);
    RefreshUploadFuture refreshUploadFuture = new RefreshUploadFuture();
    RefreshUploadFuture refreshUploadFuture2 = new RefreshUploadFuture();
    nrtDataManager.enqueueUpload(copyState1, List.of(refreshUploadFuture));
    nrtDataManager.enqueueUpload(copyState2, List.of(refreshUploadFuture2));

    refreshUploadFuture.get(30, java.util.concurrent.TimeUnit.SECONDS);
    refreshUploadFuture2.get(30, java.util.concurrent.TimeUnit.SECONDS);
    waitUntilDone(nrtDataManager);

    NrtFileMetaData nrtFileMetaData1 = new NrtFileMetaData(fileMetaData1, PRIMARY_ID, "timestamp");
    NrtFileMetaData nrtFileMetaData2 = new NrtFileMetaData(fileMetaData2, PRIMARY_ID, "timestamp");
    NrtFileMetaData nrtFileMetaData3 = new NrtFileMetaData(fileMetaData3, PRIMARY_ID, "timestamp");
    NrtPointState nrtPointState1 =
        new NrtPointState(
            copyState1, Map.of("file1", nrtFileMetaData1, "file2", nrtFileMetaData2), PRIMARY_ID);
    NrtPointState nrtPointState2 =
        new NrtPointState(
            copyState2, Map.of("file2", nrtFileMetaData2, "file3", nrtFileMetaData3), PRIMARY_ID);
    verifyPointStates(nrtPointState2, nrtDataManager.getLastPointState());

    verify(mockPrimaryNode, times(1)).releaseCopyState(copyState1);
    verify(mockPrimaryNode, times(1)).releaseCopyState(copyState2);

    ArgumentCaptor<NrtPointState> pointStateCaptor = ArgumentCaptor.forClass(NrtPointState.class);
    ArgumentCaptor<byte[]> pointStateBytesCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(mockRemoteBackend, times(2))
        .uploadPointState(
            eq(SERVICE_NAME),
            eq(INDEX_NAME),
            pointStateCaptor.capture(),
            pointStateBytesCaptor.capture());
    List<NrtPointState> pointStates = pointStateCaptor.getAllValues();
    verifyPointStates(nrtPointState1, pointStates.get(0));
    assertArrayEquals(
        RemoteUtils.pointStateToUtf8(pointStates.get(0)),
        pointStateBytesCaptor.getAllValues().get(0));
    verifyPointStates(nrtPointState2, pointStates.get(1));
    assertArrayEquals(
        RemoteUtils.pointStateToUtf8(pointStates.get(1)),
        pointStateBytesCaptor.getAllValues().get(1));

    ArgumentCaptor<Map<String, NrtFileMetaData>> filesCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockRemoteBackend, times(2))
        .uploadIndexFiles(
            eq(SERVICE_NAME), eq(INDEX_NAME), eq(folder.getRoot().toPath()), filesCaptor.capture());
    List<Map<String, NrtFileMetaData>> files = filesCaptor.getAllValues();
    verifyFilesMetaData(Map.of("file1", nrtFileMetaData1, "file2", nrtFileMetaData2), files.get(0));
    verifyFilesMetaData(Map.of("file3", nrtFileMetaData3), files.get(1));
    verifyNoMoreInteractions(mockPrimaryNode, mockRemoteBackend);
  }

  @Test
  public void testUploadManagerThread_sameVersion()
      throws ExecutionException, InterruptedException, TimeoutException, IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    nrtDataManager.startUploadManager(mockPrimaryNode, folder.getRoot().toPath());

    FileMetaData fileMetaData =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 15, 16);
    CopyState copyState =
        new CopyState(
            Map.of("file1", fileMetaData),
            5,
            6,
            new byte[] {1, 2, 3},
            Set.of("merged_file"),
            7,
            null);
    RefreshUploadFuture refreshUploadFuture = new RefreshUploadFuture();
    RefreshUploadFuture refreshUploadFuture2 = new RefreshUploadFuture();
    nrtDataManager.enqueueUpload(copyState, List.of(refreshUploadFuture));
    nrtDataManager.enqueueUpload(copyState, List.of(refreshUploadFuture2));

    refreshUploadFuture.get(30, java.util.concurrent.TimeUnit.SECONDS);
    refreshUploadFuture2.get(30, java.util.concurrent.TimeUnit.SECONDS);
    waitUntilDone(nrtDataManager);

    NrtFileMetaData nrtFileMetaData = new NrtFileMetaData(fileMetaData, PRIMARY_ID, "timestamp");
    NrtPointState nrtPointState =
        new NrtPointState(copyState, Map.of("file1", nrtFileMetaData), PRIMARY_ID);
    verifyPointStates(nrtPointState, nrtDataManager.getLastPointState());
    byte[] pointStateBytes = RemoteUtils.pointStateToUtf8(nrtDataManager.getLastPointState());

    verify(mockPrimaryNode, times(2)).releaseCopyState(copyState);
    verify(mockRemoteBackend, times(1))
        .uploadPointState(
            eq(SERVICE_NAME),
            eq(INDEX_NAME),
            argThat(
                arg -> {
                  verifyPointStates(nrtPointState, arg);
                  return true;
                }),
            eq(pointStateBytes));
    verify(mockRemoteBackend, times(1))
        .uploadIndexFiles(
            eq(SERVICE_NAME),
            eq(INDEX_NAME),
            eq(folder.getRoot().toPath()),
            argThat(
                arg -> {
                  verifyFilesMetaData(Map.of("file1", nrtFileMetaData), arg);
                  return true;
                }));
    verifyNoMoreInteractions(mockPrimaryNode, mockRemoteBackend);
  }

  @Test
  public void testUploadManagerThread_lowerVersion()
      throws ExecutionException, InterruptedException, TimeoutException, IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    nrtDataManager.startUploadManager(mockPrimaryNode, folder.getRoot().toPath());

    FileMetaData fileMetaData =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 15, 16);
    CopyState copyState =
        new CopyState(
            Map.of("file1", fileMetaData),
            5,
            6,
            new byte[] {1, 2, 3},
            Set.of("merged_file"),
            7,
            null);
    FileMetaData fileMetaData2 =
        new FileMetaData(new byte[] {7, 8, 9}, new byte[] {10, 11, 12}, 25, 26);
    CopyState copyState2 =
        new CopyState(
            Map.of("file0", fileMetaData2),
            4,
            6,
            new byte[] {0, 1, 2},
            Set.of("merged_file"),
            7,
            null);
    RefreshUploadFuture refreshUploadFuture = new RefreshUploadFuture();
    RefreshUploadFuture refreshUploadFuture2 = new RefreshUploadFuture();
    nrtDataManager.enqueueUpload(copyState, List.of(refreshUploadFuture));
    nrtDataManager.enqueueUpload(copyState2, List.of(refreshUploadFuture2));

    refreshUploadFuture.get(30, java.util.concurrent.TimeUnit.SECONDS);
    refreshUploadFuture2.get(30, java.util.concurrent.TimeUnit.SECONDS);
    waitUntilDone(nrtDataManager);

    NrtFileMetaData nrtFileMetaData = new NrtFileMetaData(fileMetaData, PRIMARY_ID, "timestamp");
    NrtPointState nrtPointState =
        new NrtPointState(copyState, Map.of("file1", nrtFileMetaData), PRIMARY_ID);
    verifyPointStates(nrtPointState, nrtDataManager.getLastPointState());
    byte[] pointStateBytes = RemoteUtils.pointStateToUtf8(nrtDataManager.getLastPointState());

    verify(mockPrimaryNode, times(1)).releaseCopyState(copyState);
    verify(mockPrimaryNode, times(1)).releaseCopyState(copyState2);
    verify(mockRemoteBackend, times(1))
        .uploadPointState(
            eq(SERVICE_NAME),
            eq(INDEX_NAME),
            argThat(
                arg -> {
                  verifyPointStates(nrtPointState, arg);
                  return true;
                }),
            eq(pointStateBytes));
    verify(mockRemoteBackend, times(1))
        .uploadIndexFiles(
            eq(SERVICE_NAME),
            eq(INDEX_NAME),
            eq(folder.getRoot().toPath()),
            argThat(
                arg -> {
                  verifyFilesMetaData(Map.of("file1", nrtFileMetaData), arg);
                  return true;
                }));
    verifyNoMoreInteractions(mockPrimaryNode, mockRemoteBackend);
  }

  @Test
  public void testUploadManagerThread_error()
      throws ExecutionException, InterruptedException, TimeoutException, IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    doThrow(new IOException("error"))
        .when(mockRemoteBackend)
        .uploadPointState(
            eq(SERVICE_NAME), eq(INDEX_NAME), any(NrtPointState.class), any(byte[].class));
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    nrtDataManager.startUploadManager(mockPrimaryNode, folder.getRoot().toPath());

    FileMetaData fileMetaData =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 15, 16);
    CopyState copyState =
        new CopyState(
            Map.of("file1", fileMetaData),
            5,
            6,
            new byte[] {1, 2, 3},
            Set.of("merged_file"),
            7,
            null);
    RefreshUploadFuture refreshUploadFuture = new RefreshUploadFuture();
    nrtDataManager.enqueueUpload(copyState, List.of(refreshUploadFuture));

    try {
      refreshUploadFuture.get(30, java.util.concurrent.TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException e) {
      assertEquals("java.io.IOException: error", e.getMessage());
    }
    waitUntilDone(nrtDataManager);
    assertNull(nrtDataManager.getLastPointState());

    NrtFileMetaData nrtFileMetaData = new NrtFileMetaData(fileMetaData, PRIMARY_ID, "timestamp");
    NrtPointState nrtPointState =
        new NrtPointState(copyState, Map.of("file1", nrtFileMetaData), PRIMARY_ID);

    verify(mockPrimaryNode, times(1)).releaseCopyState(copyState);
    verify(mockRemoteBackend, times(1))
        .uploadPointState(
            eq(SERVICE_NAME),
            eq(INDEX_NAME),
            argThat(
                arg -> {
                  verifyPointStates(nrtPointState, arg);
                  return true;
                }),
            any(byte[].class));
    verify(mockRemoteBackend, times(1))
        .uploadIndexFiles(
            eq(SERVICE_NAME),
            eq(INDEX_NAME),
            eq(folder.getRoot().toPath()),
            argThat(
                arg -> {
                  verifyFilesMetaData(Map.of("file1", nrtFileMetaData), arg);
                  return true;
                }));
    verifyNoMoreInteractions(mockPrimaryNode, mockRemoteBackend);
  }

  private void waitUntilDone(NrtDataManager nrtDataManager) {
    int count = 0;
    while (nrtDataManager.getCurrentUploadTask() != null) {
      count++;
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (count > 100) {
        fail("Upload tasks are not done");
      }
    }
  }

  @Test
  public void testEnqueueNoRemoteCommit() {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, false);
    try {
      nrtDataManager.enqueueUpload(mock(CopyState.class), List.of());
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Remote commit is not available for this configuration", e.getMessage());
    }
  }

  @Test
  public void testIsSameFile_same() {
    FileMetaData fileMetaData =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 15, 16);
    NrtFileMetaData nrtFileMetaData = new NrtFileMetaData(fileMetaData, PRIMARY_ID, "timestamp");
    assertTrue(NrtDataManager.UploadManagerThread.isSameFile(fileMetaData, nrtFileMetaData));
  }

  @Test
  public void testIsSameFile_differentLength() {
    FileMetaData fileMetaData =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 15, 16);
    NrtFileMetaData nrtFileMetaData = new NrtFileMetaData(fileMetaData, PRIMARY_ID, "timestamp");
    FileMetaData fileMetaData2 =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 10, 16);
    assertFalse(NrtDataManager.UploadManagerThread.isSameFile(fileMetaData2, nrtFileMetaData));
  }

  @Test
  public void testIsSameFile_differentChecksum() {
    FileMetaData fileMetaData =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 15, 16);
    NrtFileMetaData nrtFileMetaData = new NrtFileMetaData(fileMetaData, PRIMARY_ID, "timestamp");
    FileMetaData fileMetaData2 =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 15, 11);
    assertFalse(NrtDataManager.UploadManagerThread.isSameFile(fileMetaData2, nrtFileMetaData));
  }

  @Test
  public void testIsSameFile_differentHeader() {
    FileMetaData fileMetaData =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 15, 16);
    NrtFileMetaData nrtFileMetaData = new NrtFileMetaData(fileMetaData, PRIMARY_ID, "timestamp");
    FileMetaData fileMetaData2 =
        new FileMetaData(new byte[] {2, 4, 6}, new byte[] {4, 5, 6}, 15, 16);
    assertFalse(NrtDataManager.UploadManagerThread.isSameFile(fileMetaData2, nrtFileMetaData));
  }

  @Test
  public void testIsSameFile_differentFooter() {
    FileMetaData fileMetaData =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 15, 16);
    NrtFileMetaData nrtFileMetaData = new NrtFileMetaData(fileMetaData, PRIMARY_ID, "timestamp");
    FileMetaData fileMetaData2 =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {8, 10, 12}, 15, 16);
    assertFalse(NrtDataManager.UploadManagerThread.isSameFile(fileMetaData2, nrtFileMetaData));
  }

  private void verifyPointStates(NrtPointState expected, NrtPointState actual) {
    assertEquals(expected.version, actual.version);
    assertEquals(expected.gen, actual.gen);
    assertEquals(expected.primaryGen, actual.primaryGen);
    assertEquals(expected.primaryId, actual.primaryId);
    assertEquals(expected.completedMergeFiles, actual.completedMergeFiles);
    assertArrayEquals(expected.infosBytes, actual.infosBytes);
    verifyFilesMetaData(expected.files, actual.files);
  }

  private void verifyFilesMetaData(
      Map<String, NrtFileMetaData> expected, Map<String, NrtFileMetaData> actual) {
    assertEquals(expected.size(), actual.size());
    expected.forEach(
        (key, value) -> {
          assertTrue(actual.containsKey(key));
          NrtFileMetaData actualValue = actual.get(key);
          assertEquals(value.length, actualValue.length);
          assertEquals(value.checksum, actualValue.checksum);
          assertEquals(value.primaryId, actualValue.primaryId);
          assertArrayEquals(value.header, actualValue.header);
          assertArrayEquals(value.footer, actualValue.footer);
          assertTrue(TimeStringUtils.isTimeStringSec(actualValue.timeString));
        });
  }

  @Test
  public void testGetTargetPointState_success() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);

    // Create test data
    FileMetaData fileMetaData = new FileMetaData(new byte[] {1, 2}, new byte[] {3, 4}, 100, 12345);
    CopyState copyState =
        new CopyState(
            Map.of("test_file", fileMetaData),
            1,
            3,
            new byte[] {1, 2, 3, 4, 5},
            Set.of("merged_file"),
            7,
            null);
    NrtFileMetaData nrtFileMetaData =
        new NrtFileMetaData(fileMetaData, PRIMARY_ID, TimeStringUtils.generateTimeStringSec());
    Map<String, NrtFileMetaData> nrtFileMetaDataMap = Map.of("test_file", nrtFileMetaData);
    NrtPointState nrtPointState = new NrtPointState(copyState, nrtFileMetaDataMap, PRIMARY_ID);

    byte[] pointStateBytes = RemoteUtils.pointStateToUtf8(nrtPointState);
    Instant testTimestamp = Instant.now();

    when(mockRemoteBackend.downloadPointState(SERVICE_NAME, INDEX_NAME))
        .thenReturn(
            new RemoteBackend.InputStreamWithTimestamp(
                new ByteArrayInputStream(pointStateBytes), testTimestamp));

    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, false);

    // Test the method
    NrtDataManager.PointStateWithTimestamp result = nrtDataManager.getTargetPointState();

    // Verify results
    assertNotNull(result);
    assertEquals(testTimestamp, result.timestamp());
    verifyPointStates(nrtPointState, result.pointState());

    verify(mockRemoteBackend).downloadPointState(SERVICE_NAME, INDEX_NAME);
  }

  @Test
  public void testGetTargetPointState_ioException() throws IOException {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);

    when(mockRemoteBackend.downloadPointState(SERVICE_NAME, INDEX_NAME))
        .thenThrow(new IOException("Download failed"));

    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, false);

    try {
      nrtDataManager.getTargetPointState();
      fail("Expected IOException");
    } catch (IOException e) {
      assertEquals("Download failed", e.getMessage());
    }

    verify(mockRemoteBackend).downloadPointState(SERVICE_NAME, INDEX_NAME);
  }

  @Test
  public void testSetLastPointState_newPointState() {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, false);

    // Create test data
    FileMetaData fileMetaData = new FileMetaData(new byte[] {1, 2}, new byte[] {3, 4}, 100, 12345);
    CopyState copyState =
        new CopyState(
            Map.of("test_file", fileMetaData),
            1,
            3,
            new byte[] {1, 2, 3, 4, 5},
            Set.of("merged_file"),
            7,
            null);
    NrtFileMetaData nrtFileMetaData =
        new NrtFileMetaData(fileMetaData, PRIMARY_ID, TimeStringUtils.generateTimeStringSec());
    Map<String, NrtFileMetaData> nrtFileMetaDataMap = Map.of("test_file", nrtFileMetaData);
    NrtPointState nrtPointState = new NrtPointState(copyState, nrtFileMetaDataMap, PRIMARY_ID);
    Instant testTimestamp = Instant.now();

    // Initially should be null
    assertNull(nrtDataManager.getLastPointState());

    // Set the point state
    nrtDataManager.setLastPointState(nrtPointState, testTimestamp);

    // Verify it was set
    verifyPointStates(nrtPointState, nrtDataManager.getLastPointState());
  }

  @Test
  public void testSetLastPointState_higherVersion() {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, false);

    // Create initial point state with version 1
    FileMetaData fileMetaData1 = new FileMetaData(new byte[] {1, 2}, new byte[] {3, 4}, 100, 12345);
    CopyState copyState1 =
        new CopyState(
            Map.of("file1", fileMetaData1),
            1, // version 1
            3,
            new byte[] {1, 2, 3, 4, 5},
            Set.of("merged_file"),
            7,
            null);
    NrtFileMetaData nrtFileMetaData1 =
        new NrtFileMetaData(fileMetaData1, PRIMARY_ID, TimeStringUtils.generateTimeStringSec());
    NrtPointState pointState1 =
        new NrtPointState(copyState1, Map.of("file1", nrtFileMetaData1), PRIMARY_ID);
    Instant timestamp1 = Instant.now();

    // Create newer point state with version 2
    FileMetaData fileMetaData2 = new FileMetaData(new byte[] {5, 6}, new byte[] {7, 8}, 200, 67890);
    CopyState copyState2 =
        new CopyState(
            Map.of("file2", fileMetaData2),
            2, // version 2
            4,
            new byte[] {6, 7, 8, 9, 10},
            Set.of("merged_file2"),
            8,
            null);
    NrtFileMetaData nrtFileMetaData2 =
        new NrtFileMetaData(fileMetaData2, PRIMARY_ID, TimeStringUtils.generateTimeStringSec());
    NrtPointState pointState2 =
        new NrtPointState(copyState2, Map.of("file2", nrtFileMetaData2), PRIMARY_ID);
    Instant timestamp2 = timestamp1.plusSeconds(10);

    // Set initial state
    nrtDataManager.setLastPointState(pointState1, timestamp1);
    verifyPointStates(pointState1, nrtDataManager.getLastPointState());

    // Set newer state - should update
    nrtDataManager.setLastPointState(pointState2, timestamp2);
    verifyPointStates(pointState2, nrtDataManager.getLastPointState());
  }

  @Test
  public void testSetLastPointState_lowerVersion() {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, false);

    // Create initial point state with version 2
    FileMetaData fileMetaData2 = new FileMetaData(new byte[] {5, 6}, new byte[] {7, 8}, 200, 67890);
    CopyState copyState2 =
        new CopyState(
            Map.of("file2", fileMetaData2),
            2, // version 2
            4,
            new byte[] {6, 7, 8, 9, 10},
            Set.of("merged_file2"),
            8,
            null);
    NrtFileMetaData nrtFileMetaData2 =
        new NrtFileMetaData(fileMetaData2, PRIMARY_ID, TimeStringUtils.generateTimeStringSec());
    NrtPointState pointState2 =
        new NrtPointState(copyState2, Map.of("file2", nrtFileMetaData2), PRIMARY_ID);
    Instant timestamp2 = Instant.now();

    // Create older point state with version 1
    FileMetaData fileMetaData1 = new FileMetaData(new byte[] {1, 2}, new byte[] {3, 4}, 100, 12345);
    CopyState copyState1 =
        new CopyState(
            Map.of("file1", fileMetaData1),
            1, // version 1 (lower)
            3,
            new byte[] {1, 2, 3, 4, 5},
            Set.of("merged_file"),
            7,
            null);
    NrtFileMetaData nrtFileMetaData1 =
        new NrtFileMetaData(fileMetaData1, PRIMARY_ID, TimeStringUtils.generateTimeStringSec());
    NrtPointState pointState1 =
        new NrtPointState(copyState1, Map.of("file1", nrtFileMetaData1), PRIMARY_ID);
    Instant timestamp1 = timestamp2.minusSeconds(10);

    // Set newer state first
    nrtDataManager.setLastPointState(pointState2, timestamp2);
    verifyPointStates(pointState2, nrtDataManager.getLastPointState());

    // Try to set older state - should NOT update
    nrtDataManager.setLastPointState(pointState1, timestamp1);
    verifyPointStates(
        pointState2, nrtDataManager.getLastPointState()); // Should still be the newer one
  }

  @Test
  public void testSetLastPointState_sameVersion() {
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, false);

    // Create first point state with version 1
    FileMetaData fileMetaData1 = new FileMetaData(new byte[] {1, 2}, new byte[] {3, 4}, 100, 12345);
    CopyState copyState1 =
        new CopyState(
            Map.of("file1", fileMetaData1),
            1, // version 1
            3,
            new byte[] {1, 2, 3, 4, 5},
            Set.of("merged_file"),
            7,
            null);
    NrtFileMetaData nrtFileMetaData1 =
        new NrtFileMetaData(fileMetaData1, PRIMARY_ID, TimeStringUtils.generateTimeStringSec());
    NrtPointState pointState1 =
        new NrtPointState(copyState1, Map.of("file1", nrtFileMetaData1), PRIMARY_ID);
    Instant timestamp1 = Instant.now();

    // Create second point state with same version 1
    FileMetaData fileMetaData2 = new FileMetaData(new byte[] {5, 6}, new byte[] {7, 8}, 200, 67890);
    CopyState copyState2 =
        new CopyState(
            Map.of("file2", fileMetaData2),
            1, // same version 1
            4,
            new byte[] {6, 7, 8, 9, 10},
            Set.of("merged_file2"),
            8,
            null);
    NrtFileMetaData nrtFileMetaData2 =
        new NrtFileMetaData(fileMetaData2, PRIMARY_ID, TimeStringUtils.generateTimeStringSec());
    NrtPointState pointState2 =
        new NrtPointState(copyState2, Map.of("file2", nrtFileMetaData2), PRIMARY_ID);
    Instant timestamp2 = timestamp1.plusSeconds(10);

    // Set first state
    nrtDataManager.setLastPointState(pointState1, timestamp1);
    verifyPointStates(pointState1, nrtDataManager.getLastPointState());

    // Try to set second state with same version - should NOT update
    nrtDataManager.setLastPointState(pointState2, timestamp2);
    verifyPointStates(
        pointState1, nrtDataManager.getLastPointState()); // Should still be the first one
  }

  @Test
  public void testDownloadIndexFile() throws IOException {
    // Create mock RemoteBackend
    RemoteBackend mockRemoteBackend = mock(RemoteBackend.class);
    NrtDataManager nrtDataManager =
        new NrtDataManager(SERVICE_NAME, INDEX_NAME, PRIMARY_ID, mockRemoteBackend, null, true);

    // Create test data
    String fileName = "test_file.dat";
    FileMetaData fileMetaData =
        new FileMetaData(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, 100, 12345);
    NrtFileMetaData nrtFileMetaData = new NrtFileMetaData(fileMetaData, PRIMARY_ID, "timestamp");

    // Set up mock behavior
    byte[] expectedData = "test data content".getBytes();
    InputStream expectedInputStream = new ByteArrayInputStream(expectedData);
    when(mockRemoteBackend.downloadIndexFile(
            eq(SERVICE_NAME), eq(INDEX_NAME), eq(fileName), eq(nrtFileMetaData)))
        .thenReturn(expectedInputStream);

    // Call the method under test
    InputStream result = nrtDataManager.downloadIndexFile(fileName, nrtFileMetaData);

    // Verify the result
    assertSame(expectedInputStream, result);

    // Verify interactions with the mock
    verify(mockRemoteBackend)
        .downloadIndexFile(SERVICE_NAME, INDEX_NAME, fileName, nrtFileMetaData);
    verifyNoMoreInteractions(mockRemoteBackend);
  }
}
