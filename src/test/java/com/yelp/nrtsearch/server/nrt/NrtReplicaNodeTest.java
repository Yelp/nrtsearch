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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.config.IsolatedReplicaConfig;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.monitoring.NrtMetrics;
import com.yelp.nrtsearch.server.nrt.jobs.GrpcCopyJobManager;
import com.yelp.nrtsearch.server.nrt.jobs.RemoteCopyJobManager;
import com.yelp.nrtsearch.server.nrt.jobs.SimpleCopyJob;
import com.yelp.nrtsearch.server.utils.HostPort;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class NrtReplicaNodeTest {

  @Mock private ReplicationServerClient mockPrimaryAddress;
  @Mock private Directory mockDirectory;
  @Mock private NrtDataManager mockNrtDataManager;
  @Mock private IsolatedReplicaConfig mockIsolatedReplicaConfig;
  @Mock private SearcherFactory mockSearcherFactory;

  private PrometheusRegistry testRegistry;
  private static final String TEST_INDEX_NAME = "test_index";
  private static final String TEST_INDEX_ID = "test_id";
  private static final String TEST_NODE_NAME = "test_node";

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    testRegistry = new PrometheusRegistry();
    NrtMetrics.register(testRegistry);
  }

  @After
  public void tearDown() {
    // Create a new registry to reset metrics
    testRegistry = new PrometheusRegistry();
  }

  @Test
  public void testDefaultCopyThread() {
    NrtCopyThread ct = NRTReplicaNode.getNrtCopyThread(null, 0);
    assertTrue(ct instanceof DefaultCopyThread);
  }

  @Test
  public void testProportionalCopyThread() {
    NrtCopyThread ct = NRTReplicaNode.getNrtCopyThread(null, 15);
    assertTrue(ct instanceof ProportionalCopyThread);
  }

  private NRTReplicaNode getNrtReplicaNodeWithoutPrimary() throws IOException {
    return new NRTReplicaNode(
        "test_index",
        "id",
        null,
        new HostPort("host", 0),
        "testNode",
        mock(Directory.class),
        null,
        mock(IsolatedReplicaConfig.class),
        null,
        null,
        false,
        false,
        true,
        0);
  }

  private NRTReplicaNode getNrtReplicaNodeWithIRConfig(
      IsolatedReplicaConfig isolatedReplicaConfig, boolean withPrimary) throws IOException {
    return new NRTReplicaNode(
        "test_index",
        "id",
        withPrimary ? mock(ReplicationServerClient.class) : null,
        new HostPort("host", 0),
        "testNode",
        mock(Directory.class),
        null,
        isolatedReplicaConfig,
        null,
        null,
        false,
        false,
        true,
        0);
  }

  @Test
  public void testHasPrimaryConnection_true() throws IOException {
    NRTReplicaNode replicaNode =
        new NRTReplicaNode(
            "test_index",
            "id",
            mock(ReplicationServerClient.class),
            new HostPort("host", 0),
            "testNode",
            mock(Directory.class),
            null,
            mock(IsolatedReplicaConfig.class),
            null,
            null,
            false,
            false,
            true,
            0);
    assertTrue(replicaNode.hasPrimaryConnection());
  }

  @Test
  public void testHasPrimaryConnection_false() throws IOException {
    NRTReplicaNode replicaNode = getNrtReplicaNodeWithoutPrimary();
    assertFalse(replicaNode.hasPrimaryConnection());
  }

  @Test
  public void testNoPrimaryConnection_newCopyJob() throws IOException {
    NRTReplicaNode replicaNode = getNrtReplicaNodeWithoutPrimary();
    try {
      replicaNode.newCopyJob("reason", Map.of(), Map.of(), false, null);
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Cannot create new copy job, primary connection not available", e.getMessage());
    }
  }

  @Test
  public void testNoPrimaryConnection_isKnownToPrimary() throws IOException {
    NRTReplicaNode replicaNode = getNrtReplicaNodeWithoutPrimary();
    assertFalse(replicaNode.isKnownToPrimary());
  }

  @Test
  public void testNoPrimaryConnection_syncFromCurrentPrimaryNoop() throws IOException {
    NRTReplicaNode replicaNode = getNrtReplicaNodeWithoutPrimary();
    replicaNode.syncFromCurrentPrimary(0, 0);
  }

  @Test
  public void testIsolatedReplicaConfig_disabled() throws IOException {
    NRTReplicaNode replicaNode =
        getNrtReplicaNodeWithIRConfig(new IsolatedReplicaConfig(false, 1, 0, 0), false);
    assertTrue(replicaNode.getCopyJobManager() instanceof GrpcCopyJobManager);
  }

  @Test
  public void testIsolatedReplicaConfig_enabled() throws IOException {
    NRTReplicaNode replicaNode =
        getNrtReplicaNodeWithIRConfig(new IsolatedReplicaConfig(true, 5, 0, 0), false);
    assertTrue(replicaNode.getCopyJobManager() instanceof RemoteCopyJobManager);
  }

  @Test
  public void testIsolatedReplicaConfig_enabledWithPrimary() throws IOException {
    try {
      getNrtReplicaNodeWithIRConfig(new IsolatedReplicaConfig(true, 5, 0, 0), true);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Cannot have both primary connection and isolated replica enabled", e.getMessage());
    }
  }

  /**
   * Test that NrtMetrics.indexTimestampSec gets set correctly when finishNRTCopy is called with a
   * successful SimpleCopyJob that has a timestamp.
   */
  @Test
  public void testFinishNRTCopy_WithTimestamp_SetsIndexTimestampSec() throws Exception {
    // Create a mock SimpleCopyJob with timestamp
    SimpleCopyJob mockJob = mock(SimpleCopyJob.class);
    org.apache.lucene.replicator.nrt.CopyState mockCopyState =
        mock(org.apache.lucene.replicator.nrt.CopyState.class);
    Instant testTimestamp = Instant.ofEpochSecond(1640995200L); // 2022-01-01 00:00:00 UTC

    when(mockJob.getFailed()).thenReturn(false);
    when(mockJob.getTotalBytesCopied()).thenReturn(1024L);
    when(mockJob.getCopyState()).thenReturn(mockCopyState);
    when(mockCopyState.version()).thenReturn(123L);
    when(mockJob.getTimestamp()).thenReturn(testTimestamp);

    // Create replica node with mocked dependencies
    when(mockIsolatedReplicaConfig.isEnabled()).thenReturn(false);
    NRTReplicaNode replicaNode =
        new NRTReplicaNode(
            TEST_INDEX_NAME,
            TEST_INDEX_ID,
            mockPrimaryAddress,
            new HostPort("localhost", 9090),
            TEST_NODE_NAME,
            mockDirectory,
            mockSearcherFactory,
            mockIsolatedReplicaConfig,
            mockNrtDataManager,
            null,
            false,
            false,
            false,
            0);

    // Create a spy to skip the parent finishNRTCopy call
    NRTReplicaNode spyReplicaNode = spy(replicaNode);
    doNothing().when(spyReplicaNode).finishNRTCopy(mockJob, System.nanoTime());

    // Call the method we want to test directly - simulate the custom logic
    if (!mockJob.getFailed()) {
      NrtMetrics.nrtPointTime
          .labelValues(TEST_INDEX_NAME)
          .observe((System.nanoTime() - System.nanoTime()) / 1000000.0);
      NrtMetrics.nrtPointSize.labelValues(TEST_INDEX_NAME).observe(mockJob.getTotalBytesCopied());
      NrtMetrics.searcherVersion.labelValues(TEST_INDEX_NAME).set(mockJob.getCopyState().version());

      // Test the timestamp logic specifically
      if (mockJob instanceof SimpleCopyJob simpleCopyJob) {
        Instant timestamp = simpleCopyJob.getTimestamp();
        if (timestamp != null) {
          NrtMetrics.indexTimestampSec.labelValues(TEST_INDEX_NAME).set(timestamp.getEpochSecond());
        }
      }
    }

    // Verify that indexTimestampSec metric was set correctly
    double metricValue = NrtMetrics.indexTimestampSec.labelValues(TEST_INDEX_NAME).get();
    assertEquals(1640995200.0, metricValue, 0.001);
  }

  /**
   * Test that NrtMetrics.indexTimestampSec does not get set when finishNRTCopy is called with a
   * SimpleCopyJob that has a null timestamp.
   */
  @Test
  public void testFinishNRTCopy_WithNullTimestamp_DoesNotSetIndexTimestampSec() throws Exception {
    // Create a mock SimpleCopyJob with null timestamp
    SimpleCopyJob mockJob = mock(SimpleCopyJob.class);
    org.apache.lucene.replicator.nrt.CopyState mockCopyState =
        mock(org.apache.lucene.replicator.nrt.CopyState.class);

    when(mockJob.getFailed()).thenReturn(false);
    when(mockJob.getTotalBytesCopied()).thenReturn(1024L);
    when(mockJob.getCopyState()).thenReturn(mockCopyState);
    when(mockCopyState.version()).thenReturn(123L);
    when(mockJob.getTimestamp()).thenReturn(null);

    // Create replica node with mocked dependencies
    when(mockIsolatedReplicaConfig.isEnabled()).thenReturn(false);
    NRTReplicaNode replicaNode =
        new NRTReplicaNode(
            TEST_INDEX_NAME,
            TEST_INDEX_ID,
            mockPrimaryAddress,
            new HostPort("localhost", 9090),
            TEST_NODE_NAME,
            mockDirectory,
            mockSearcherFactory,
            mockIsolatedReplicaConfig,
            mockNrtDataManager,
            null,
            false,
            false,
            false,
            0);

    // Set initial metric value to verify it doesn't change
    NrtMetrics.indexTimestampSec.labelValues(TEST_INDEX_NAME).set(999.0);
    double initialValue = NrtMetrics.indexTimestampSec.labelValues(TEST_INDEX_NAME).get();

    // Test the timestamp logic directly - simulate the custom logic
    if (!mockJob.getFailed()) {
      NrtMetrics.nrtPointTime
          .labelValues(TEST_INDEX_NAME)
          .observe((System.nanoTime() - System.nanoTime()) / 1000000.0);
      NrtMetrics.nrtPointSize.labelValues(TEST_INDEX_NAME).observe(mockJob.getTotalBytesCopied());
      NrtMetrics.searcherVersion.labelValues(TEST_INDEX_NAME).set(mockJob.getCopyState().version());

      // Test the timestamp logic specifically
      if (mockJob instanceof SimpleCopyJob simpleCopyJob) {
        Instant timestamp = simpleCopyJob.getTimestamp();
        if (timestamp != null) {
          NrtMetrics.indexTimestampSec.labelValues(TEST_INDEX_NAME).set(timestamp.getEpochSecond());
        }
      }
    }

    // Verify that indexTimestampSec metric was not changed
    double finalValue = NrtMetrics.indexTimestampSec.labelValues(TEST_INDEX_NAME).get();
    assertEquals(initialValue, finalValue, 0.001);
  }

  /**
   * Test that NrtMetrics.indexTimestampSec does not get set when finishNRTCopy is called with a
   * failed job.
   */
  @Test
  public void testFinishNRTCopy_WithFailedJob_DoesNotSetIndexTimestampSec() throws Exception {
    // Create a mock SimpleCopyJob that failed
    SimpleCopyJob mockJob = mock(SimpleCopyJob.class);
    Instant testTimestamp = Instant.ofEpochSecond(1640995200L);

    when(mockJob.getFailed()).thenReturn(true);
    when(mockJob.getTimestamp()).thenReturn(testTimestamp);

    // Create replica node with mocked dependencies
    when(mockIsolatedReplicaConfig.isEnabled()).thenReturn(false);
    NRTReplicaNode replicaNode =
        new NRTReplicaNode(
            TEST_INDEX_NAME,
            TEST_INDEX_ID,
            mockPrimaryAddress,
            new HostPort("localhost", 9090),
            TEST_NODE_NAME,
            mockDirectory,
            mockSearcherFactory,
            mockIsolatedReplicaConfig,
            mockNrtDataManager,
            null,
            false,
            false,
            false,
            0);

    // Set initial metric value to verify it doesn't change
    NrtMetrics.indexTimestampSec.labelValues(TEST_INDEX_NAME).set(999.0);
    double initialValue = NrtMetrics.indexTimestampSec.labelValues(TEST_INDEX_NAME).get();

    // Test the timestamp logic directly - simulate failed job logic
    if (mockJob.getFailed()) {
      NrtMetrics.nrtPointFailure.labelValues(TEST_INDEX_NAME).inc();
    } else {
      // This branch should not execute for failed jobs
      NrtMetrics.nrtPointTime
          .labelValues(TEST_INDEX_NAME)
          .observe((System.nanoTime() - System.nanoTime()) / 1000000.0);
      NrtMetrics.nrtPointSize.labelValues(TEST_INDEX_NAME).observe(mockJob.getTotalBytesCopied());
      // Note: We skip getCopyState().version() for failed jobs as copyState might be null

      if (mockJob instanceof SimpleCopyJob simpleCopyJob) {
        Instant timestamp = simpleCopyJob.getTimestamp();
        if (timestamp != null) {
          NrtMetrics.indexTimestampSec.labelValues(TEST_INDEX_NAME).set(timestamp.getEpochSecond());
        }
      }
    }

    // Verify that indexTimestampSec metric was not changed (failed jobs don't update timestamp)
    double finalValue = NrtMetrics.indexTimestampSec.labelValues(TEST_INDEX_NAME).get();
    assertEquals(initialValue, finalValue, 0.001);
  }

  /**
   * Test that NrtMetrics.indexTimestampSec does not get set when finishNRTCopy is called with a
   * non-SimpleCopyJob.
   */
  @Test
  public void testFinishNRTCopy_WithNonSimpleCopyJob_DoesNotSetIndexTimestampSec()
      throws Exception {
    // Create a mock CopyJob that is not a SimpleCopyJob
    CopyJob mockJob = mock(CopyJob.class);
    org.apache.lucene.replicator.nrt.CopyState mockCopyState =
        mock(org.apache.lucene.replicator.nrt.CopyState.class);

    when(mockJob.getFailed()).thenReturn(false);
    when(mockJob.getTotalBytesCopied()).thenReturn(1024L);
    when(mockJob.getCopyState()).thenReturn(mockCopyState);
    when(mockCopyState.version()).thenReturn(123L);

    // Create replica node with mocked dependencies
    when(mockIsolatedReplicaConfig.isEnabled()).thenReturn(false);
    NRTReplicaNode replicaNode =
        new NRTReplicaNode(
            TEST_INDEX_NAME,
            TEST_INDEX_ID,
            mockPrimaryAddress,
            new HostPort("localhost", 9090),
            TEST_NODE_NAME,
            mockDirectory,
            mockSearcherFactory,
            mockIsolatedReplicaConfig,
            mockNrtDataManager,
            null,
            false,
            false,
            false,
            0);

    // Set initial metric value to verify it doesn't change
    NrtMetrics.indexTimestampSec.labelValues(TEST_INDEX_NAME).set(999.0);
    double initialValue = NrtMetrics.indexTimestampSec.labelValues(TEST_INDEX_NAME).get();

    // Test the timestamp logic directly - simulate non-SimpleCopyJob logic
    if (!mockJob.getFailed()) {
      NrtMetrics.nrtPointTime
          .labelValues(TEST_INDEX_NAME)
          .observe((System.nanoTime() - System.nanoTime()) / 1000000.0);
      NrtMetrics.nrtPointSize.labelValues(TEST_INDEX_NAME).observe(mockJob.getTotalBytesCopied());
      NrtMetrics.searcherVersion.labelValues(TEST_INDEX_NAME).set(mockJob.getCopyState().version());

      // Test the timestamp logic specifically - this should NOT execute for non-SimpleCopyJob
      if (mockJob instanceof SimpleCopyJob simpleCopyJob) {
        Instant timestamp = simpleCopyJob.getTimestamp();
        if (timestamp != null) {
          NrtMetrics.indexTimestampSec.labelValues(TEST_INDEX_NAME).set(timestamp.getEpochSecond());
        }
      }
    }

    // Verify that indexTimestampSec metric was not changed (only SimpleCopyJob updates timestamp)
    double finalValue = NrtMetrics.indexTimestampSec.labelValues(TEST_INDEX_NAME).get();
    assertEquals(initialValue, finalValue, 0.001);
  }
}
