/*
 * Copyright 2022 Yelp Inc.
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

import static com.yelp.nrtsearch.server.grpc.TestServer.SERVICE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.monitoring.BootstrapMetrics;
import com.yelp.nrtsearch.server.monitoring.WarmingMetrics;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WarmingQueriesTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupServers();
  }

  @AfterClass
  public static void cleanupClass() {
    TestServer.cleanupAll();
  }

  @Test
  public void testCreateWarmingQueries() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);
    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");

    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.LOCAL)
            .withWarming(10, 1, false)
            .build();

    SearchRequest searchRequest =
        SearchRequest.newBuilder()
            .setIndexName("test_index")
            .setTopHits(10)
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(TermQuery.newBuilder().setField("id").setTextValue("2").build())
                    .build())
            .addAllRetrieveFields(Arrays.asList("id", "field1", "field2"))
            .build();
    replica.getClient().getBlockingStub().search(searchRequest);
    replica
        .getClient()
        .getBlockingStub()
        .backupWarmingQueries(
            BackupWarmingQueriesRequest.newBuilder()
                .setIndex("test_index")
                .setUptimeMinutesThreshold(0)
                .setServiceName(SERVICE_NAME)
                .setNumQueriesThreshold(0)
                .build());

    InputStream queriesStream =
        replica
            .getRemoteBackend()
            .downloadWarmingQueries(
                SERVICE_NAME, server.getGlobalState().getDataResourceForIndex("test_index"));
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(queriesStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    }
    assertEquals(1, lines.size());
    assertEquals(
        JsonFormat.printer().omittingInsignificantWhitespace().print(searchRequest), lines.get(0));
  }

  @Test
  public void testWarmOnStartup() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);
    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");

    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.LOCAL)
            .withWarming(10, 1, true)
            .build();

    SearchRequest searchRequest =
        SearchRequest.newBuilder()
            .setIndexName("test_index")
            .setTopHits(10)
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(TermQuery.newBuilder().setField("id").setTextValue("2").build())
                    .build())
            .addAllRetrieveFields(Arrays.asList("id", "field1", "field2"))
            .build();
    replica.getClient().getBlockingStub().search(searchRequest);
    replica
        .getClient()
        .getBlockingStub()
        .backupWarmingQueries(
            BackupWarmingQueriesRequest.newBuilder()
                .setIndex("test_index")
                .setUptimeMinutesThreshold(0)
                .setServiceName(SERVICE_NAME)
                .setNumQueriesThreshold(0)
                .build());
    replica.restart();
  }

  @Test
  public void testCollectionFilter() throws IOException {
    TestServer server = startPrimaryWithDocs();
    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.LOCAL)
            .withWarming(10, 1, false)
            .withAdditionalWarmingConfig("filterMatchAllQueries: true", "maxQueryTopHits: 100")
            .build();

    SearchRequest termRequest = termSearchRequest(10);
    SearchRequest matchAllRequest =
        SearchRequest.newBuilder()
            .setIndexName("test_index")
            .setTopHits(10)
            .addAllRetrieveFields(Arrays.asList("id", "field1", "field2"))
            .build();
    SearchRequest largeTopHitsRequest = termSearchRequest(1000);
    replica.getClient().getBlockingStub().search(termRequest);
    replica.getClient().getBlockingStub().search(matchAllRequest);
    replica.getClient().getBlockingStub().search(largeTopHitsRequest);
    backupWarmingQueries(replica);

    List<String> lines = downloadWarmingQueries(server, replica);
    assertEquals(1, lines.size());
    assertEquals(
        JsonFormat.printer().omittingInsignificantWhitespace().print(termRequest), lines.get(0));
  }

  @Test
  public void testWarmOnStartupWithGuardrails() throws IOException {
    TestServer server = startPrimaryWithDocs();
    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.LOCAL)
            .withWarming(10, 2, true)
            .withAdditionalWarmingConfig("queryTimeoutMs: 5000", "maxWarmingDurationMs: 30000")
            .build();

    replica.getClient().getBlockingStub().search(termSearchRequest(10));
    backupWarmingQueries(replica);
    replica.restart();

    SearchResponse response = replica.getClient().getBlockingStub().search(termSearchRequest(10));
    assertEquals(1, response.getHitsCount());
  }

  private TestServer startPrimaryWithDocs() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);
    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");
    return server;
  }

  private static SearchRequest termSearchRequest(int topHits) {
    return SearchRequest.newBuilder()
        .setIndexName("test_index")
        .setTopHits(topHits)
        .setQuery(
            Query.newBuilder()
                .setTermQuery(TermQuery.newBuilder().setField("id").setTextValue("2").build())
                .build())
        .addAllRetrieveFields(Arrays.asList("id", "field1", "field2"))
        .build();
  }

  private static void backupWarmingQueries(TestServer replica) {
    replica
        .getClient()
        .getBlockingStub()
        .backupWarmingQueries(
            BackupWarmingQueriesRequest.newBuilder()
                .setIndex("test_index")
                .setUptimeMinutesThreshold(0)
                .setServiceName(SERVICE_NAME)
                .setNumQueriesThreshold(0)
                .build());
  }

  private static List<String> downloadWarmingQueries(TestServer server, TestServer replica)
      throws IOException {
    InputStream queriesStream =
        replica
            .getRemoteBackend()
            .downloadWarmingQueries(
                SERVICE_NAME, server.getGlobalState().getDataResourceForIndex("test_index"));
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(queriesStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    }
    return lines;
  }

  @Test
  public void testBackupWarmingQueriesMetrics() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);
    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");

    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.LOCAL)
            .withWarming(10, 1, false)
            .build();
    replica
        .getClient()
        .getBlockingStub()
        .search(SearchRequest.newBuilder().setIndexName("test_index").setTopHits(10).build());

    double successBefore = backupSuccessCount();
    double belowUptimeBefore = backupFailureCount("below_uptime_threshold");
    double belowQueriesBefore = backupFailureCount("below_query_threshold");

    // replica just started, so a one day uptime threshold is not met
    assertThrows(StatusRuntimeException.class, () -> backupWarmingQueries(replica, 24 * 60, 0));
    // only one query has been collected
    assertThrows(StatusRuntimeException.class, () -> backupWarmingQueries(replica, 0, 100));
    backupWarmingQueries(replica, 0, 0);

    assertEquals(1, backupFailureCount("below_uptime_threshold") - belowUptimeBefore, 0);
    assertEquals(1, backupFailureCount("below_query_threshold") - belowQueriesBefore, 0);
    assertEquals(1, backupSuccessCount() - successBefore, 0);
  }

  private static void backupWarmingQueries(
      TestServer replica, int uptimeMinutesThreshold, int numQueriesThreshold) {
    replica
        .getClient()
        .getBlockingStub()
        .backupWarmingQueries(
            BackupWarmingQueriesRequest.newBuilder()
                .setIndex("test_index")
                .setUptimeMinutesThreshold(uptimeMinutesThreshold)
                .setServiceName(SERVICE_NAME)
                .setNumQueriesThreshold(numQueriesThreshold)
                .build());
  }

  private static double backupSuccessCount() {
    return WarmingMetrics.warmingBackupSuccessCount.labelValues("test_index").get();
  }

  private static double backupFailureCount(String reason) {
    return WarmingMetrics.warmingBackupFailureCount.labelValues("test_index", reason).get();
  }

  @Test
  public void testWarmOnStartupFromSchema() throws IOException {
    TestServer server = startPrimaryWithDocs();
    // no warming queries are backed up, schema warming must not need them
    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.LOCAL)
            .withWarming(10, 2, true)
            .withWarmingStrategy("schema")
            .build();
    String indexId = server.getGlobalState().getDataResourceForIndex("test_index");
    assertEquals(1, BootstrapMetrics.warmingStrategyInfo.labelValues(indexId, "schema").get(), 0);

    SearchResponse response = replica.getClient().getBlockingStub().search(termSearchRequest(10));
    assertEquals(1, response.getHitsCount());
  }

  @Test
  public void testWarmOnStartupFromSchemaWithGuardrails() throws IOException {
    TestServer server = startPrimaryWithDocs();
    String indexId = server.getGlobalState().getDataResourceForIndex("test_index");
    double schemaSuccessBefore = schemaQueryCount(indexId, "success");
    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.LOCAL)
            .withWarming(10, 2, true)
            .withWarmingStrategy("schema")
            .withAdditionalWarmingConfig("queryTimeoutMs: 5000", "maxWarmingDurationMs: 30000")
            .build();

    // generated queries are replayed through the guarded path and counted as type schema
    assertTrue(schemaQueryCount(indexId, "success") > schemaSuccessBefore);
    assertEquals(0, schemaQueryCount(indexId, "error"), 0);
    SearchResponse response = replica.getClient().getBlockingStub().search(termSearchRequest(10));
    assertEquals(1, response.getHitsCount());
  }

  private TestServer peerWarmingReplica(TestServer primary, String... extraConfig)
      throws IOException {
    List<String> config = new ArrayList<>(List.of("peerMinUptimeMinutes: 0"));
    config.addAll(List.of(extraConfig));
    return TestServer.builder(folder)
        .withAutoStartConfig(
            true, Mode.REPLICA, primary.getReplicationPort(), IndexDataLocationType.LOCAL)
        .withWarming(10, 1, true)
        .withWarmingStrategy("peer")
        .withAdditionalWarmingConfig(config.toArray(new String[0]))
        .build();
  }

  private static double peerFetchCount(String indexId, String outcome) {
    return WarmingMetrics.warmingPeerFetchCount.labelValues(indexId, outcome).get();
  }

  private static double fullQueryCount(String indexId) {
    return WarmingMetrics.warmingQueryCount.labelValues(indexId, "full", "success").get();
  }

  @Test
  public void testWarmOnStartupFromPeer() throws IOException {
    TestServer server = startPrimaryWithDocs();
    String indexId = server.getGlobalState().getDataResourceForIndex("test_index");
    // the first replica has no peer, and nothing in S3
    TestServer replicaA =
        peerWarmingReplica(server, "peerMinQueries: 2", "peerFallbackStrategy: none");
    replicaA.getClient().getBlockingStub().search(termSearchRequest(10));
    replicaA.getClient().getBlockingStub().search(termSearchRequest(5));

    double successBefore = peerFetchCount(indexId, "success");
    double fullBefore = fullQueryCount(indexId);
    TestServer replicaB =
        peerWarmingReplica(server, "peerMinQueries: 2", "peerFallbackStrategy: none");

    // B warmed with A's two sampled queries, without anything in S3
    assertEquals(1, peerFetchCount(indexId, "success") - successBefore, 0);
    assertEquals(2, fullQueryCount(indexId) - fullBefore, 0);
    assertEquals(1, BootstrapMetrics.warmingStrategyInfo.labelValues(indexId, "peer").get(), 0);
    SearchResponse response = replicaB.getClient().getBlockingStub().search(termSearchRequest(10));
    assertEquals(1, response.getHitsCount());
  }

  @Test
  public void testWarmOnStartupFromPeerFallback() throws IOException {
    TestServer server = startPrimaryWithDocs();
    String indexId = server.getGlobalState().getDataResourceForIndex("test_index");
    peerWarmingReplica(server, "peerFallbackStrategy: none");

    // the running replica has served no queries, so it does not qualify
    double fallbackBefore = peerFetchCount(indexId, "fallback");
    double successBefore = peerFetchCount(indexId, "success");
    TestServer replicaB =
        peerWarmingReplica(server, "peerMinQueries: 1", "peerFallbackStrategy: schema");

    assertEquals(1, peerFetchCount(indexId, "fallback") - fallbackBefore, 0);
    assertEquals(0, peerFetchCount(indexId, "success") - successBefore, 0);
    assertEquals(1, BootstrapMetrics.warmingStrategyInfo.labelValues(indexId, "schema").get(), 0);
    SearchResponse response = replicaB.getClient().getBlockingStub().search(termSearchRequest(10));
    assertEquals(1, response.getHitsCount());
  }

  @Test
  public void testGetWarmingQueries() throws IOException {
    TestServer server = startPrimaryWithDocs();
    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.LOCAL)
            .withWarming(10, 1, false)
            .build();
    SearchRequest first = termSearchRequest(10);
    SearchRequest second = termSearchRequest(5);
    replica.getClient().getBlockingStub().search(first);
    replica.getClient().getBlockingStub().search(second);
    String firstJson = JsonFormat.printer().omittingInsignificantWhitespace().print(first);
    String secondJson = JsonFormat.printer().omittingInsignificantWhitespace().print(second);

    try (ReplicationServerClient client =
        new ReplicationServerClient("localhost", replica.getReplicationPort())) {
      String all =
          new String(
              client.getWarmingQueries("test_index", 0, 0, 2, 0, 10_000), StandardCharsets.UTF_8);
      assertEquals(firstJson + "\n" + secondJson + "\n", all);

      String limited =
          new String(
              client.getWarmingQueries("test_index", 1, 0, 0, 0, 10_000), StandardCharsets.UTF_8);
      assertEquals(firstJson + "\n", limited);

      // only whole queries are sent
      String byteLimited =
          new String(
              client.getWarmingQueries("test_index", 0, firstJson.length() + 5, 0, 0, 10_000),
              StandardCharsets.UTF_8);
      assertEquals(firstJson + "\n", byteLimited);

      StatusRuntimeException belowQueries =
          assertThrows(
              StatusRuntimeException.class,
              () -> client.getWarmingQueries("test_index", 0, 0, 3, 0, 10_000));
      assertEquals(Status.Code.FAILED_PRECONDITION, belowQueries.getStatus().getCode());

      StatusRuntimeException belowUptime =
          assertThrows(
              StatusRuntimeException.class,
              () -> client.getWarmingQueries("test_index", 0, 0, 0, 24 * 60, 10_000));
      assertEquals(Status.Code.FAILED_PRECONDITION, belowUptime.getStatus().getCode());

      StatusRuntimeException unknownIndex =
          assertThrows(
              StatusRuntimeException.class,
              () -> client.getWarmingQueries("missing_index", 0, 0, 0, 0, 10_000));
      assertEquals(Status.Code.NOT_FOUND, unknownIndex.getStatus().getCode());
    }
  }

  private static double schemaQueryCount(String indexId, String outcome) {
    return WarmingMetrics.warmingQueryCount.labelValues(indexId, "schema", outcome).get();
  }
}
