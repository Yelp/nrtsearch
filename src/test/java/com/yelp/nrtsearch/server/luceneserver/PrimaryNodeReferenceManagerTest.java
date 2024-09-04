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
package com.yelp.nrtsearch.server.luceneserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.luceneserver.nrt.NrtDataManager;
import com.yelp.nrtsearch.server.luceneserver.nrt.RefreshUploadFuture;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.junit.Test;

public class PrimaryNodeReferenceManagerTest {
  @Test
  public void testRefreshIfNeeded() throws IOException {
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    ReferenceManager<IndexSearcher> mockReferenceManager = mock(ReferenceManager.class);
    IndexSearcher mockSearcher = mock(IndexSearcher.class);
    IndexReader mockIndexReader = mock(IndexReader.class);
    when(mockSearcher.getIndexReader()).thenReturn(mockIndexReader);
    when(mockReferenceManager.acquire()).thenReturn(mockSearcher);
    when(mockPrimaryNode.getSearcherManager()).thenReturn(mockReferenceManager);
    SearcherFactory mockSearcherFactory = mock(SearcherFactory.class);
    when(mockSearcherFactory.newSearcher(eq(mockIndexReader), any())).thenReturn(mockSearcher);

    when(mockPrimaryNode.flushAndRefresh()).thenReturn(true);

    NRTPrimaryNode.PrimaryNodeReferenceManager primaryNodeReferenceManager =
        new NRTPrimaryNode.PrimaryNodeReferenceManager(mockPrimaryNode, mockSearcherFactory);
    IndexSearcher indexSearcher = primaryNodeReferenceManager.refreshIfNeeded(mockSearcher);
    assertEquals(mockSearcher, indexSearcher);

    verify(mockPrimaryNode, times(2)).getSearcherManager();
    verify(mockPrimaryNode, times(1)).flushAndRefresh();
    verify(mockPrimaryNode, times(1)).sendNewNRTPointToReplicas();
    verify(mockReferenceManager, times(2)).acquire();
    verify(mockSearcherFactory, times(1)).newSearcher(mockIndexReader, null);
    verify(mockSearcherFactory, times(1)).newSearcher(mockIndexReader, mockIndexReader);
    verify(mockSearcher, times(5)).getIndexReader();
    verifyNoMoreInteractions(
        mockPrimaryNode, mockReferenceManager, mockSearcher, mockIndexReader, mockSearcherFactory);
  }

  @Test
  public void testRefreshIfNeeded_noopFlush() throws IOException {
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    ReferenceManager<IndexSearcher> mockReferenceManager = mock(ReferenceManager.class);
    IndexSearcher mockSearcher = mock(IndexSearcher.class);
    IndexReader mockIndexReader = mock(IndexReader.class);
    when(mockSearcher.getIndexReader()).thenReturn(mockIndexReader);
    when(mockReferenceManager.acquire()).thenReturn(mockSearcher);
    when(mockPrimaryNode.getSearcherManager()).thenReturn(mockReferenceManager);
    SearcherFactory mockSearcherFactory = mock(SearcherFactory.class);
    when(mockSearcherFactory.newSearcher(eq(mockIndexReader), any())).thenReturn(mockSearcher);

    when(mockPrimaryNode.flushAndRefresh()).thenReturn(false);

    NRTPrimaryNode.PrimaryNodeReferenceManager primaryNodeReferenceManager =
        new NRTPrimaryNode.PrimaryNodeReferenceManager(mockPrimaryNode, mockSearcherFactory);
    IndexSearcher indexSearcher = primaryNodeReferenceManager.refreshIfNeeded(mockSearcher);
    assertNull(indexSearcher);

    verify(mockPrimaryNode, times(1)).getSearcherManager();
    verify(mockPrimaryNode, times(1)).flushAndRefresh();
    verify(mockReferenceManager, times(1)).acquire();
    verify(mockSearcherFactory, times(1)).newSearcher(mockIndexReader, null);
    verify(mockSearcher, times(2)).getIndexReader();
    verifyNoMoreInteractions(
        mockPrimaryNode, mockReferenceManager, mockSearcher, mockIndexReader, mockSearcherFactory);
  }

  @Test
  public void testRefreshIfNeededWatcher() throws IOException {
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    ReferenceManager<IndexSearcher> mockReferenceManager = mock(ReferenceManager.class);
    IndexSearcher mockSearcher = mock(IndexSearcher.class);
    IndexReader mockIndexReader = mock(IndexReader.class);
    when(mockSearcher.getIndexReader()).thenReturn(mockIndexReader);
    when(mockReferenceManager.acquire()).thenReturn(mockSearcher);
    when(mockPrimaryNode.getSearcherManager()).thenReturn(mockReferenceManager);
    SearcherFactory mockSearcherFactory = mock(SearcherFactory.class);
    when(mockSearcherFactory.newSearcher(eq(mockIndexReader), any())).thenReturn(mockSearcher);

    CopyState mockCopyState = mock(CopyState.class);
    NrtDataManager mockNrtDataManager = mock(NrtDataManager.class);
    when(mockPrimaryNode.getNrtDataManager()).thenReturn(mockNrtDataManager);
    when(mockPrimaryNode.getCopyState()).thenReturn(mockCopyState);
    when(mockPrimaryNode.flushAndRefresh()).thenReturn(true);

    NRTPrimaryNode.PrimaryNodeReferenceManager primaryNodeReferenceManager =
        new NRTPrimaryNode.PrimaryNodeReferenceManager(mockPrimaryNode, mockSearcherFactory);
    RefreshUploadFuture watcher =
        (RefreshUploadFuture) primaryNodeReferenceManager.nextRefreshDurable();
    IndexSearcher indexSearcher = primaryNodeReferenceManager.refreshIfNeeded(mockSearcher);
    assertEquals(mockSearcher, indexSearcher);

    verify(mockPrimaryNode, times(2)).getSearcherManager();
    verify(mockPrimaryNode, times(1)).flushAndRefresh();
    verify(mockPrimaryNode, times(1)).sendNewNRTPointToReplicas();
    verify(mockPrimaryNode, times(1)).getNrtDataManager();
    verify(mockPrimaryNode, times(1)).getCopyState();
    verify(mockNrtDataManager, times(1)).enqueueUpload(mockCopyState, List.of(watcher));
    verify(mockReferenceManager, times(2)).acquire();
    verify(mockSearcherFactory, times(1)).newSearcher(mockIndexReader, null);
    verify(mockSearcherFactory, times(1)).newSearcher(mockIndexReader, mockIndexReader);
    verify(mockSearcher, times(5)).getIndexReader();
    verifyNoMoreInteractions(
        mockPrimaryNode,
        mockReferenceManager,
        mockSearcher,
        mockIndexReader,
        mockSearcherFactory,
        mockNrtDataManager,
        mockCopyState);
  }

  @Test
  public void testRefreshIfNeededWatcher_noopFlush() throws IOException {
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    ReferenceManager<IndexSearcher> mockReferenceManager = mock(ReferenceManager.class);
    IndexSearcher mockSearcher = mock(IndexSearcher.class);
    IndexReader mockIndexReader = mock(IndexReader.class);
    when(mockSearcher.getIndexReader()).thenReturn(mockIndexReader);
    when(mockReferenceManager.acquire()).thenReturn(mockSearcher);
    when(mockPrimaryNode.getSearcherManager()).thenReturn(mockReferenceManager);
    SearcherFactory mockSearcherFactory = mock(SearcherFactory.class);
    when(mockSearcherFactory.newSearcher(eq(mockIndexReader), any())).thenReturn(mockSearcher);

    CopyState mockCopyState = mock(CopyState.class);
    NrtDataManager mockNrtDataManager = mock(NrtDataManager.class);
    when(mockPrimaryNode.getNrtDataManager()).thenReturn(mockNrtDataManager);
    when(mockPrimaryNode.getCopyState()).thenReturn(mockCopyState);
    when(mockPrimaryNode.flushAndRefresh()).thenReturn(false);

    NRTPrimaryNode.PrimaryNodeReferenceManager primaryNodeReferenceManager =
        new NRTPrimaryNode.PrimaryNodeReferenceManager(mockPrimaryNode, mockSearcherFactory);
    RefreshUploadFuture watcher =
        (RefreshUploadFuture) primaryNodeReferenceManager.nextRefreshDurable();
    IndexSearcher indexSearcher = primaryNodeReferenceManager.refreshIfNeeded(mockSearcher);
    assertNull(indexSearcher);

    verify(mockPrimaryNode, times(1)).getSearcherManager();
    verify(mockPrimaryNode, times(1)).flushAndRefresh();
    verify(mockPrimaryNode, times(1)).getNrtDataManager();
    verify(mockPrimaryNode, times(1)).getCopyState();
    verify(mockNrtDataManager, times(1)).enqueueUpload(mockCopyState, List.of(watcher));
    verify(mockReferenceManager, times(1)).acquire();
    verify(mockSearcherFactory, times(1)).newSearcher(mockIndexReader, null);
    verify(mockSearcher, times(2)).getIndexReader();
    verifyNoMoreInteractions(
        mockPrimaryNode,
        mockReferenceManager,
        mockSearcher,
        mockIndexReader,
        mockSearcherFactory,
        mockNrtDataManager,
        mockCopyState);
  }

  @Test
  public void testRefreshIfNeededWatcher_errorBeforeEnqueue()
      throws IOException, InterruptedException, TimeoutException {
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    ReferenceManager<IndexSearcher> mockReferenceManager = mock(ReferenceManager.class);
    IndexSearcher mockSearcher = mock(IndexSearcher.class);
    IndexReader mockIndexReader = mock(IndexReader.class);
    when(mockSearcher.getIndexReader()).thenReturn(mockIndexReader);
    when(mockReferenceManager.acquire()).thenReturn(mockSearcher);
    when(mockPrimaryNode.getSearcherManager()).thenReturn(mockReferenceManager);
    SearcherFactory mockSearcherFactory = mock(SearcherFactory.class);
    when(mockSearcherFactory.newSearcher(eq(mockIndexReader), any())).thenReturn(mockSearcher);

    when(mockPrimaryNode.flushAndRefresh()).thenThrow(new RuntimeException("error"));

    NRTPrimaryNode.PrimaryNodeReferenceManager primaryNodeReferenceManager =
        new NRTPrimaryNode.PrimaryNodeReferenceManager(mockPrimaryNode, mockSearcherFactory);
    RefreshUploadFuture watcher =
        (RefreshUploadFuture) primaryNodeReferenceManager.nextRefreshDurable();
    try {
      primaryNodeReferenceManager.refreshIfNeeded(mockSearcher);
      fail();
    } catch (RuntimeException e) {
      assertEquals("error", e.getMessage());
    }
    assertTrue(watcher.isDone());
    try {
      watcher.get(10, java.util.concurrent.TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException e) {
      assertEquals("error", e.getCause().getMessage());
    }

    verify(mockPrimaryNode, times(1)).getSearcherManager();
    verify(mockPrimaryNode, times(1)).flushAndRefresh();
    verify(mockReferenceManager, times(1)).acquire();
    verify(mockSearcherFactory, times(1)).newSearcher(mockIndexReader, null);
    verify(mockSearcher, times(2)).getIndexReader();
    verifyNoMoreInteractions(
        mockPrimaryNode, mockReferenceManager, mockSearcher, mockIndexReader, mockSearcherFactory);
  }

  @Test
  public void testRefreshIfNeededWatcher_errorAfterEnqueue() throws IOException {
    NRTPrimaryNode mockPrimaryNode = mock(NRTPrimaryNode.class);
    ReferenceManager<IndexSearcher> mockReferenceManager = mock(ReferenceManager.class);
    IndexSearcher mockSearcher = mock(IndexSearcher.class);
    IndexReader mockIndexReader = mock(IndexReader.class);
    when(mockSearcher.getIndexReader()).thenReturn(mockIndexReader);
    when(mockReferenceManager.acquire()).thenReturn(mockSearcher);
    when(mockPrimaryNode.getSearcherManager()).thenReturn(mockReferenceManager);
    SearcherFactory mockSearcherFactory = mock(SearcherFactory.class);
    when(mockSearcherFactory.newSearcher(eq(mockIndexReader), any())).thenReturn(mockSearcher);

    CopyState mockCopyState = mock(CopyState.class);
    NrtDataManager mockNrtDataManager = mock(NrtDataManager.class);
    when(mockPrimaryNode.getNrtDataManager()).thenReturn(mockNrtDataManager);
    when(mockPrimaryNode.getCopyState()).thenReturn(mockCopyState);
    when(mockPrimaryNode.flushAndRefresh()).thenReturn(true);
    doThrow(new RuntimeException("error")).when(mockPrimaryNode).sendNewNRTPointToReplicas();

    NRTPrimaryNode.PrimaryNodeReferenceManager primaryNodeReferenceManager =
        new NRTPrimaryNode.PrimaryNodeReferenceManager(mockPrimaryNode, mockSearcherFactory);
    RefreshUploadFuture watcher =
        (RefreshUploadFuture) primaryNodeReferenceManager.nextRefreshDurable();
    try {
      primaryNodeReferenceManager.refreshIfNeeded(mockSearcher);
      fail();
    } catch (RuntimeException e) {
      assertEquals("error", e.getMessage());
    }
    assertFalse(watcher.isDone());

    verify(mockPrimaryNode, times(1)).getSearcherManager();
    verify(mockPrimaryNode, times(1)).flushAndRefresh();
    verify(mockPrimaryNode, times(1)).sendNewNRTPointToReplicas();
    verify(mockPrimaryNode, times(1)).getNrtDataManager();
    verify(mockPrimaryNode, times(1)).getCopyState();
    verify(mockNrtDataManager, times(1)).enqueueUpload(mockCopyState, List.of(watcher));
    verify(mockReferenceManager, times(1)).acquire();
    verify(mockSearcherFactory, times(1)).newSearcher(mockIndexReader, null);
    verify(mockSearcher, times(2)).getIndexReader();
    verifyNoMoreInteractions(
        mockPrimaryNode,
        mockReferenceManager,
        mockSearcher,
        mockIndexReader,
        mockSearcherFactory,
        mockNrtDataManager,
        mockCopyState);
  }
}
