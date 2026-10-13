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

import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
}
