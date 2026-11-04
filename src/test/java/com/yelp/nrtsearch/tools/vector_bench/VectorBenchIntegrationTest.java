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
package com.yelp.nrtsearch.tools.vector_bench;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.server.monitoring.LazyRemoteCacheCollector;
import com.yelp.nrtsearch.vectors.IndexConfig;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

/** Runs every vector_bench command against a primary and a lazy isolated replica on S3Mock. */
public class VectorBenchIntegrationTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void tearDown() {
    TestServer.cleanupServers();
  }

  @AfterClass
  public static void cleanupClass() {
    TestServer.cleanupAll();
  }

  /** Run a command, returning its stdout. Fails if the exit code isn't 0. */
  private static String run(String... args) {
    PrintStream original = System.out;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    int exit;
    try (PrintStream out = new PrintStream(captured, true, StandardCharsets.UTF_8)) {
      System.setOut(out);
      exit = new CommandLine(new VectorBenchCommand()).execute(args);
    } finally {
      System.setOut(original);
    }
    String output = captured.toString(StandardCharsets.UTF_8);
    original.print(output);
    assertEquals("exit code of " + String.join(" ", args) + "\n" + output, 0, exit);
    return output;
  }

  private static double number(String output, String regex) {
    Matcher m = Pattern.compile(regex).matcher(output);
    assertTrue("'" + regex + "' not in:\n" + output, m.find());
    return Double.parseDouble(m.group(1).replace(",", ""));
  }

  @Test
  public void testAllCommands() throws Exception {
    TestServer primary =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withLocalStateBackend()
            .build();
    Path registry = folder.getRoot().toPath().resolve("registry.json");
    Path queries = folder.getRoot().toPath().resolve("queries.bin");
    List<String> dataset =
        List.of("--count", "3000", "--dim", "32", "--clusters", "20", "--seed", "5");
    List<String> primaryTarget =
        List.of("--port", String.valueOf(primary.getPort()), "--registryFile", registry.toString());

    run(args("setup", primaryTarget, dataset));
    String load = run(args("load", primaryTarget, dataset, List.of("--threads", "4")));
    assertTrue(load, load.contains("Loaded 3,000 vectors"));

    run(
        args(
            "groundtruth",
            dataset,
            List.of(
                "--out",
                queries.toString(),
                "--queries",
                "50",
                "--k",
                "20",
                "--filteredPct",
                "30")));

    // the primary refreshes periodically; queries see all vectors once it has
    String nrtIndex = IndexConfig.nrtsearchIndexName("bench", "vectors");
    primary.refresh(nrtIndex);
    String info = run(args("info", primaryTarget, List.of()));
    assertTrue(info, info.contains("3,000 vectors, 0 deleted not yet merged"));
    assertTrue(info, number(info, "searcher version ([0-9]+)") > 0);

    String primaryQuery =
        run(
            args(
                "query",
                primaryTarget,
                List.of(
                    "--queries",
                    queries.toString(),
                    "--duration",
                    "3",
                    "--warmup",
                    "1",
                    "--concurrency",
                    "4")));
    assertTrue("recall", number(primaryQuery, "Recall@10:\\s+([0-9.]+)") >= 0.9);
    assertTrue(primaryQuery, primaryQuery.contains("no lazy remote cache on this node"));

    // a lazy isolated replica restores from S3 and reads .vec files by range
    primary.commit(nrtIndex);
    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, primary.getReplicationPort(), IndexDataLocationType.REMOTE)
            .withoutPrimary()
            .withLocalStateBackend()
            .withAdditionalConfig(
                String.join(
                    "\n",
                    "isolatedReplicaConfig:",
                    "  enabled: true",
                    "  pollingIntervalSeconds: 1",
                    "lazyRemoteData:",
                    "  enabled: true",
                    "  blockSizeKB: 4"))
            .build();
    replica
        .getPrometheusRegistry()
        .register(new LazyRemoteCacheCollector(replica.getGlobalState()));
    List<String> replicaTarget =
        List.of("--port", String.valueOf(replica.getPort()), "--registryFile", registry.toString());
    String replicaQuery =
        run(
            args(
                "query",
                replicaTarget,
                List.of(
                    "--queries",
                    queries.toString(),
                    "--duration",
                    "3",
                    "--warmup",
                    "0",
                    "--qps",
                    "100")));
    assertTrue("replica recall", number(replicaQuery, "Recall@10:\\s+([0-9.]+)") >= 0.9);
    assertTrue(
        replicaQuery,
        number(replicaQuery, "S3 cache:\\s+hit rate [0-9.]+% \\([0-9,]+ hits, ([0-9,]+) misses")
            > 0);
    assertTrue(
        "about 100 qps", Math.abs(number(replicaQuery, "Throughput: ([0-9.]+) qps") - 100) < 20);
    // the target rate is met; cold cache misses may queue a few queries, which is reported
    assertTrue(replicaQuery, !replicaQuery.contains("Warning:"));

    // a stream of writes to the primary, visible on the replica after it polls
    String stream =
        run(
            args(
                "stream",
                primaryTarget,
                dataset,
                List.of(
                    "--rate",
                    "500",
                    "--duration",
                    "3",
                    "--batch",
                    "10",
                    "--insertPct",
                    "60",
                    "--deletePct",
                    "20")));
    double inserted = number(stream, "([0-9,]+) inserted");
    assertTrue(stream, inserted > 0);
    assertTrue(stream, number(stream, "Wrote [0-9,]+ vectors in [0-9.]+s \\(([0-9.]+)/s\\)") > 300);
    primary.refresh(nrtIndex);
    primary.commit(nrtIndex);
    String replicaInfo = "";
    for (int i = 0; i < 50; i++) {
      replicaInfo = run(args("info", replicaTarget, List.of()));
      if (number(replicaInfo, "([0-9,]+) vectors,") > 3000) {
        break;
      }
      Thread.sleep(200);
    }
    assertTrue(replicaInfo, number(replicaInfo, "([0-9,]+) vectors,") > 3000);
    // replicas don't report merge stats
    assertTrue(replicaInfo, !replicaInfo.contains("deleted not yet merged"));
    assertTrue(replicaInfo, replicaInfo.contains("lazy cache"));
  }

  /**
   * The order that broke on a real cluster: the replica starts after setup but before load, so it
   * has no metadata fields, and filtered queries fail until its state is reloaded.
   */
  @Test
  public void testReplicaStartedBeforeLoadIsReloaded() throws Exception {
    TestServer primary =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withRemoteStateBackend(false)
            .build();
    Path registry = folder.getRoot().toPath().resolve("registry.json");
    Path queries = folder.getRoot().toPath().resolve("queries.bin");
    List<String> dataset =
        List.of("--count", "2000", "--dim", "16", "--clusters", "10", "--seed", "9");
    List<String> primaryTarget =
        List.of("--port", String.valueOf(primary.getPort()), "--registryFile", registry.toString());
    run(args("setup", primaryTarget, dataset));
    String nrtIndex = IndexConfig.nrtsearchIndexName("bench", "vectors");
    primary.commit(nrtIndex);

    // replica starts now, before any metadata field exists
    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, primary.getReplicationPort(), IndexDataLocationType.REMOTE)
            .withoutPrimary()
            .withRemoteStateBackend(true)
            .withAdditionalConfig(
                String.join(
                    "\n",
                    "isolatedReplicaConfig:",
                    "  enabled: true",
                    "  pollingIntervalSeconds: 1",
                    "lazyRemoteData:",
                    "  enabled: true"))
            .build();
    assertTrue(replica.isStarted(nrtIndex));

    run(args("load", primaryTarget, dataset));
    primary.refresh(nrtIndex);
    primary.commit(nrtIndex);
    run(
        args(
            "groundtruth",
            dataset,
            List.of("--out", queries.toString(), "--queries", "20", "--filteredPct", "50")));
    List<String> replicaTarget =
        List.of("--port", String.valueOf(replica.getPort()), "--registryFile", registry.toString());
    // wait for the replica to copy the loaded data
    for (int i = 0; i < 50; i++) {
      if (number(run(args("info", replicaTarget, List.of())), "([0-9,]+) vectors,") >= 2000) {
        break;
      }
      Thread.sleep(200);
    }

    String output =
        run(
            args(
                "query",
                replicaTarget,
                List.of("--queries", queries.toString(), "--duration", "2", "--warmup", "0")));
    assertTrue(output, output.contains("is missing fields (field \"m_category\" is unknown)"));
    assertTrue(output, output.contains("err=0"));
    assertTrue("recall", number(output, "Recall@10:\\s+([0-9.]+)") >= 0.9);
  }

  @SafeVarargs
  private static String[] args(String command, List<String>... parts) {
    List<String> all = new ArrayList<>();
    all.add(command);
    for (List<String> part : parts) {
      all.addAll(part);
    }
    return all.toArray(new String[0]);
  }
}
