/*
 * Copyright 2020 Yelp Inc.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.ingestion.AbstractIngestor;
import com.yelp.nrtsearch.server.ingestion.Ingestor;
import com.yelp.nrtsearch.server.leader.LeaderManager;
import com.yelp.nrtsearch.server.plugins.IngestionPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.test_utils.ManualElectorPlugin;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Ingestion plugins with multi-primary leader election. */
public class MultiPrimaryIngestionTest {
  private static final String INDEX = "test_index";
  private static final String LEADER_CONFIG =
      String.join(
          "\n",
          "s3RefreshUpload: true",
          "s3RefreshNotifyAfterUpload: true",
          "leaderElection:",
          "  type: " + ManualElectorPlugin.TYPE,
          "  allowLoopbackHost: true");

  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private ManualElectorPlugin.Cluster cluster;

  /** Records starts and stops; the test drives indexing through it. */
  static class RecordingIngestor extends AbstractIngestor {
    final AtomicInteger starts = new AtomicInteger();
    final AtomicInteger stops = new AtomicInteger();
    volatile boolean running = false;
    private final boolean supported;

    RecordingIngestor(NrtsearchConfig config, boolean supported) {
      super(config);
      this.supported = supported;
    }

    @Override
    public boolean supportsLeaderElection() {
      return supported;
    }

    @Override
    public void start() {
      running = true;
      starts.incrementAndGet();
    }

    @Override
    public void stop() {
      running = false;
      stops.incrementAndGet();
    }
  }

  static class RecordingIngestionPlugin extends Plugin implements IngestionPlugin {
    private final boolean supported;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private RecordingIngestor ingestor;

    RecordingIngestionPlugin(boolean supported) {
      this.supported = supported;
    }

    @Override
    public synchronized Ingestor getIngestor() {
      if (ingestor == null) {
        ingestor = new RecordingIngestor(null, supported);
      }
      return ingestor;
    }

    @Override
    public ExecutorService getIngestionExecutor() {
      return executor;
    }

    RecordingIngestor ingestor() {
      return (RecordingIngestor) getIngestor();
    }
  }

  @Before
  public void setUp() {
    cluster = new ManualElectorPlugin.Cluster();
  }

  @After
  public void cleanup() {
    TestServer.cleanupServers();
  }

  @AfterClass
  public static void cleanupClass() {
    TestServer.cleanupAll();
  }

  private TestServer primaryServer(Plugin ingestionPlugin) throws IOException {
    return TestServer.builder(folder)
        .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
        .withRemoteStateBackend(false)
        .withAdditionalConfig(LEADER_CONFIG)
        .withPlugins(List.of(new ManualElectorPlugin(cluster), ingestionPlugin))
        .build();
  }

  private static LeaderManager leaderManager(TestServer server) {
    return server.getGlobalState().getLeaderManager().orElseThrow();
  }

  private long elect(TestServer server) throws Exception {
    long epoch = cluster.elect(server.getGlobalState().getNodeName());
    leaderManager(server).awaitTransitions();
    return epoch;
  }

  /** Ingestors start on their own thread, so wait for a started ingestor. */
  private static void awaitRunning(RecordingIngestor ingestor, boolean running) throws Exception {
    long deadline = System.currentTimeMillis() + 10_000;
    while (ingestor.running != running) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("ingestor running is not " + running);
      }
      Thread.sleep(10);
    }
  }

  private static void refreshAndWait(TestServer server) throws Exception {
    server.reloadState();
    leaderManager(server).awaitTransitions();
  }

  @Test
  public void testIngestionFollowsLeadershipAndCheckpoint() throws Exception {
    RecordingIngestionPlugin pluginA = new RecordingIngestionPlugin(true);
    RecordingIngestionPlugin pluginB = new RecordingIngestionPlugin(true);
    TestServer a = primaryServer(pluginA);
    TestServer b = primaryServer(pluginB);
    // standbys do not ingest
    assertEquals(0, pluginA.ingestor().starts.get());
    assertEquals(0, pluginB.ingestor().starts.get());

    elect(a);
    awaitRunning(pluginA.ingestor(), true);
    assertFalse(pluginB.ingestor().running);
    a.createSimpleIndex(INDEX);
    a.startIndexV2(StartIndexV2Request.newBuilder().setIndexName(INDEX).build());
    assertTrue(pluginA.ingestor().getCommittedCheckpoint(INDEX).isEmpty());

    // a ingests and records its position with the commit
    a.addSimpleDocs(INDEX, 1, 2, 3);
    pluginA.ingestor().commit(INDEX, Map.of("source.p0", "3"));
    // more data made durable by a refresh upload, without a checkpoint commit: the data survives a
    // failover, but the checkpoint stays behind it (replay, never a gap)
    a.addSimpleDocs(INDEX, 4);
    a.refresh(INDEX);
    refreshAndWait(b);

    // failover: a stops ingesting before demotion, b starts and resumes from a's checkpoint
    elect(b);
    assertFalse(pluginA.ingestor().running);
    assertEquals(1, pluginA.ingestor().stops.get());
    awaitRunning(pluginB.ingestor(), true);
    assertEquals(Map.of("source.p0", "3"), pluginB.ingestor().getCommittedCheckpoint(INDEX));
    b.verifySimpleDocIds(INDEX, 1, 2, 3, 4);

    // b continues; other keys are kept
    pluginB.ingestor().commit(INDEX, Map.of("source.p1", "7"));
    assertEquals(
        Map.of("source.p0", "3", "source.p1", "7"),
        pluginB.ingestor().getCommittedCheckpoint(INDEX));

    // fail back to a: it starts a new term from b's checkpoint
    refreshAndWait(a);
    elect(a);
    assertEquals(2, pluginA.ingestor().starts.get());
    assertEquals(
        Map.of("source.p0", "3", "source.p1", "7"),
        pluginA.ingestor().getCommittedCheckpoint(INDEX));
  }

  /**
   * A paused old leader that still believes it leads can not make its checkpoint durable once the
   * new leader has claimed the index state: its commit is fenced.
   */
  @Test
  public void testStaleLeaderCheckpointFenced() throws Exception {
    RecordingIngestionPlugin pluginA = new RecordingIngestionPlugin(true);
    RecordingIngestionPlugin pluginB = new RecordingIngestionPlugin(true);
    TestServer a = primaryServer(pluginA);
    TestServer b = primaryServer(pluginB);
    elect(a);
    a.createSimpleIndex(INDEX);
    a.startIndexV2(StartIndexV2Request.newBuilder().setIndexName(INDEX).build());
    a.addSimpleDocs(INDEX, 1);
    pluginA.ingestor().commit(INDEX, Map.of("pos", "1"));
    refreshAndWait(b);

    // b takes over without a being told
    cluster.electWithoutRevoking(b.getGlobalState().getNodeName());
    leaderManager(b).awaitTransitions();
    assertTrue(leaderManager(b).isActive());

    // a's next checkpoint commit fails, so it is not durable
    a.addSimpleDocs(INDEX, 2);
    assertThrows(Exception.class, () -> pluginA.ingestor().commit(INDEX, Map.of("pos", "2")));
    leaderManager(a).awaitTransitions();
    assertFalse(pluginA.ingestor().running);

    assertEquals(Map.of("pos", "1"), pluginB.ingestor().getCommittedCheckpoint(INDEX));
    b.verifySimpleDocIds(INDEX, 1);
  }

  /** Ingestion writes on a standby are rejected. */
  @Test
  public void testStandbyIngestionWritesRejected() throws Exception {
    RecordingIngestionPlugin pluginA = new RecordingIngestionPlugin(true);
    RecordingIngestionPlugin pluginB = new RecordingIngestionPlugin(true);
    TestServer a = primaryServer(pluginA);
    TestServer b = primaryServer(pluginB);
    elect(a);
    a.createSimpleIndex(INDEX);
    a.startIndexV2(StartIndexV2Request.newBuilder().setIndexName(INDEX).build());
    refreshAndWait(b);

    RecordingIngestor standby = pluginB.ingestor();
    assertThrows(Exception.class, () -> standby.commit(INDEX));
    assertThrows(Exception.class, () -> standby.commit(INDEX, Map.of("pos", "1")));
    assertThrows(
        Exception.class,
        () ->
            standby.deleteByQuery(
                List.of(
                    Query.newBuilder()
                        .setMatchAllQuery(MatchAllQuery.newBuilder().build())
                        .build()),
                INDEX));
  }

  @Test
  public void testUnsupportedIngestorFailsStartup() {
    Exception e =
        assertThrows(Exception.class, () -> primaryServer(new RecordingIngestionPlugin(false)));
    Throwable cause = e;
    while (cause != null && !cause.getMessage().contains("does not support leader election")) {
      cause = cause.getCause();
    }
    assertTrue(e.toString(), cause != null);
  }

  /** On shutdown, the active primary stops ingestion. */
  @Test
  public void testShutdownStopsIngestion() throws Exception {
    RecordingIngestionPlugin pluginA = new RecordingIngestionPlugin(true);
    TestServer a = primaryServer(pluginA);
    elect(a);
    awaitRunning(pluginA.ingestor(), true);
    a.stop();
    assertFalse(pluginA.ingestor().running);
  }
}
