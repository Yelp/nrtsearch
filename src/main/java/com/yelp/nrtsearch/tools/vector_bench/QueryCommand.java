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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import picocli.CommandLine;

@CommandLine.Command(
    name = "query",
    description =
        "Run QueryVectors load from a query set and report latency, recall and, for nodes with"
            + " lazy remote data, what was read from S3.")
public class QueryCommand implements Callable<Integer> {
  @CommandLine.Mixin BenchOptions.Target target;

  @CommandLine.Option(
      names = {"--queries"},
      description = "Query set from groundtruth (default: ${DEFAULT-VALUE})",
      defaultValue = "queries.bin")
  Path queriesFile;

  @CommandLine.Option(
      names = {"--topK"},
      description = "topK for each query, at most the query set's k (default: ${DEFAULT-VALUE})",
      defaultValue = "10")
  int topK;

  @CommandLine.Option(
      names = {"--qps"},
      description =
          "Target queries per second, or 0 to send as fast as --concurrency allows"
              + " (default: ${DEFAULT-VALUE})",
      defaultValue = "0")
  double qps;

  @CommandLine.Option(
      names = {"--concurrency"},
      description = "Queries in flight at once (default: ${DEFAULT-VALUE})",
      defaultValue = "8")
  int concurrency;

  @CommandLine.Option(
      names = {"--duration"},
      description = "Seconds to measure for (default: ${DEFAULT-VALUE})",
      defaultValue = "60")
  long durationSeconds;

  @CommandLine.Option(
      names = {"--warmup"},
      description = "Seconds to run before measuring (default: ${DEFAULT-VALUE})",
      defaultValue = "10")
  long warmupSeconds;

  @CommandLine.Option(
      names = {"--metricsHost"},
      description =
          "nrtsearch node to read cache metrics from (default: --host, needed with --gateway)")
  String metricsHost;

  @CommandLine.Option(
      names = {"--metricsPort"},
      description = "gRPC port of the metrics node (default: --port)",
      defaultValue = "0")
  int metricsPort;

  @CommandLine.Option(
      names = {"--noRecall"},
      description = "Don't check results against the exact neighbors")
  boolean noRecall;

  @Override
  public Integer call() throws Exception {
    QuerySet set = QuerySet.load(queriesFile);
    if (topK > set.k()) {
      throw new CommandLine.ParameterException(
          new CommandLine(this),
          "--topK " + topK + " is more than the query set's k of " + set.k());
    }
    List<QuerySet.Query> queries = set.queries();
    long filtered = queries.stream().filter(q -> q.category() != null).count();
    String mHost =
        metricsHost != null ? metricsHost : target.gatewayUrl == null ? target.host : null;
    int mPort = metricsPort > 0 ? metricsPort : target.port;

    System.out.printf(
        Locale.ROOT,
        "Querying %s/%s: %,d queries (%,d filtered), topK %d, %s, %ds warmup + %ds%n",
        target.bucket,
        target.index,
        queries.size(),
        filtered,
        topK,
        qps > 0 ? String.format(Locale.ROOT, "%.0f qps target", qps) : concurrency + " in flight",
        warmupSeconds,
        durationSeconds);

    LatencyRecorder latency = new LatencyRecorder();
    DoubleAdder recallSum = new DoubleAdder();
    AtomicLong recallCount = new AtomicLong();
    AtomicLong nextQuery = new AtomicLong();
    AtomicBoolean measuring = new AtomicBoolean(warmupSeconds == 0);
    AtomicBoolean stop = new AtomicBoolean();
    RateLimiter limiter = new RateLimiter(qps);

    try (BenchClient client = target.client(concurrency, true)) {
      ExecutorService executor = Executors.newFixedThreadPool(concurrency);
      CacheMetrics.Snapshot before = null;
      long measureStart = 0;
      try {
        Future<?>[] futures = new Future<?>[concurrency];
        for (int t = 0; t < concurrency; t++) {
          futures[t] =
              executor.submit(
                  () -> {
                    while (!stop.get()) {
                      long scheduled = limiter.acquire();
                      QuerySet.Query query =
                          queries.get((int) (nextQuery.getAndIncrement() % queries.size()));
                      ObjectNode request = target.request();
                      request.put("topK", topK);
                      ArrayNode data = request.putObject("queryVector").putArray("float32");
                      for (float f : query.vector()) {
                        data.add(f);
                      }
                      if (query.category() != null) {
                        request.putObject("filter").put("category", query.category());
                      }
                      try {
                        JsonNode response = client.call("QueryVectors", request);
                        long nanos = System.nanoTime() - scheduled;
                        if (measuring.get()) {
                          latency.record(nanos);
                          if (!noRecall) {
                            recallSum.add(recall(query, response));
                            recallCount.incrementAndGet();
                          }
                        }
                      } catch (Exception e) {
                        if (measuring.get()) {
                          latency.recordError();
                        }
                        System.err.println("Query failed: " + e.getMessage());
                      }
                    }
                    return null;
                  });
        }
        if (warmupSeconds > 0) {
          Thread.sleep(TimeUnit.SECONDS.toMillis(warmupSeconds));
          limiter.resetLateCount();
          measuring.set(true);
        }
        before = mHost != null ? CacheMetrics.scrape(mHost, mPort) : null;
        measureStart = System.nanoTime();
        long lastReport = measureStart;
        long end = measureStart + TimeUnit.SECONDS.toNanos(durationSeconds);
        while (System.nanoTime() < end) {
          Thread.sleep(200);
          if (System.nanoTime() - lastReport > TimeUnit.SECONDS.toNanos(10)) {
            lastReport = System.nanoTime();
            LatencyRecorder.Summary s = latency.summary();
            System.out.printf(
                Locale.ROOT,
                "  %.0fs: %.0f qps, p50 %.1fms, p99 %.1fms%n",
                (lastReport - measureStart) / 1e9,
                s.count() / ((lastReport - measureStart) / 1e9),
                s.percentileMs(50),
                s.percentileMs(99));
          }
        }
        measuring.set(false);
        stop.set(true);
        for (Future<?> f : futures) {
          f.get();
        }
      } finally {
        stop.set(true);
        executor.shutdownNow();
      }
      double seconds = (System.nanoTime() - measureStart) / 1e9;
      LatencyRecorder.Summary summary = latency.summary();
      System.out.println();
      System.out.printf(
          Locale.ROOT, "Throughput: %.1f qps over %.1fs%n", summary.count() / seconds, seconds);
      System.out.println("Latency:    " + summary.format());
      if (!noRecall && recallCount.get() > 0) {
        System.out.printf(
            Locale.ROOT,
            "Recall@%d:  %.4f (exact neighbors from %s; only valid while the index matches it)%n",
            topK,
            recallSum.sum() / recallCount.get(),
            queriesFile);
      }
      if (limiter.isLimited()) {
        long late = limiter.lateCount();
        double achieved = summary.count() / seconds;
        if (achieved < qps * 0.95) {
          System.out.printf(
              Locale.ROOT,
              "Warning:    reached %.1f of the %.0f qps target. %,d queries were queued because"
                  + " all %d senders were busy, and their latency includes the queueing. Raise"
                  + " --concurrency, or the server is at capacity.%n",
              achieved,
              qps,
              late,
              concurrency);
        } else if (late > 0) {
          System.out.printf(
              Locale.ROOT,
              "Queued:     %,d queries waited for a free sender (latency includes the wait)%n",
              late);
        }
      }
      if (before != null) {
        CacheMetrics.Snapshot after = CacheMetrics.scrape(mHost, mPort);
        System.out.println("S3 cache:   " + after.formatDelta(before, seconds));
      }
    }
    return 0;
  }

  /** Fraction of the exact topK neighbors that are in the response. */
  double recall(QuerySet.Query query, JsonNode response) {
    return recall(query.neighbors(), response, topK);
  }

  /**
   * Fraction of the first {@code topK} exact neighbors found in the response. If fewer than topK
   * vectors match (a selective filter), recall is over the neighbors that exist.
   */
  static double recall(long[] exactNeighbors, JsonNode response, int topK) {
    int expectedCount = Math.min(topK, exactNeighbors.length);
    if (expectedCount == 0) {
      return 1.0;
    }
    Set<Long> expected = new HashSet<>();
    for (int i = 0; i < expectedCount; i++) {
      expected.add(exactNeighbors[i]);
    }
    int found = 0;
    for (JsonNode vector : response.get("vectors")) {
      if (expected.contains(BenchOptions.id(vector.get("key").asText()))) {
        found++;
      }
    }
    return (double) found / expectedCount;
  }
}
