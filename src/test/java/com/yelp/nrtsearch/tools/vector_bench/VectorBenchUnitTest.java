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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class VectorBenchUnitTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testSyntheticIsDeterministicAndClustered() {
    VectorSource.Synthetic a = new VectorSource.Synthetic(16, 1000, 5, 0.1f, 1);
    VectorSource.Synthetic b = new VectorSource.Synthetic(16, 1000, 5, 0.1f, 1);
    VectorSource.Synthetic other = new VectorSource.Synthetic(16, 1000, 5, 0.1f, 2);
    assertArrayEquals(a.vector(123), b.vector(123), 0f);
    assertFalse(Arrays.equals(a.vector(123), other.vector(123)));
    assertFalse(Arrays.equals(a.vector(1), a.vector(2)));
    // queries are not dataset vectors
    assertFalse(Arrays.equals(a.query(0), a.vector(0)));

    // with tight clusters, each vector's nearest neighbor is much closer than an average vector
    double near = 0;
    double all = 0;
    for (int i = 0; i < 50; i++) {
      float[] v = a.vector(i);
      double best = Double.MAX_VALUE;
      double sum = 0;
      for (int j = 0; j < 1000; j++) {
        if (j == i) {
          continue;
        }
        double d = QuerySet.squaredL2(v, a.vector(j));
        best = Math.min(best, d);
        sum += d;
      }
      near += best;
      all += sum / 999;
    }
    assertTrue("nearest " + near + " vs average " + all, near * 10 < all);
  }

  private Path writeFvecs(float[][] vectors) throws IOException {
    Path path = folder.newFile("data.fvecs").toPath();
    try (OutputStream out = Files.newOutputStream(path)) {
      for (float[] v : vectors) {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 * v.length).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(v.length);
        for (float f : v) {
          buffer.putFloat(f);
        }
        out.write(buffer.array());
      }
    }
    return path;
  }

  @Test
  public void testFvecs() throws Exception {
    float[][] vectors = {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}};
    Path path = writeFvecs(vectors);
    try (VectorSource source = new VectorSource.Fvecs(path, 0)) {
      assertEquals(3, source.dimension());
      assertEquals(3, source.size());
      assertArrayEquals(vectors[1], source.vector(1), 0f);
      assertArrayEquals(vectors[2], source.vector(2), 0f);
      assertThrows(IllegalArgumentException.class, () -> source.vector(3));
    }
    try (VectorSource limited = new VectorSource.Fvecs(path, 2)) {
      assertEquals(2, limited.size());
    }
  }

  @Test
  public void testFvecsRejectsBadFile() throws Exception {
    Path path = folder.newFile("bad.fvecs").toPath();
    Files.write(path, new byte[] {3, 0, 0, 0, 1, 2});
    assertThrows(IOException.class, () -> new VectorSource.Fvecs(path, 0));
  }

  @Test
  public void testIvecs() throws Exception {
    Path path = folder.newFile("gt.ivecs").toPath();
    ByteBuffer buffer = ByteBuffer.allocate(4 * 7).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(2).putInt(5).putInt(9).putInt(3).putInt(1).putInt(2).putInt(3);
    Files.write(path, buffer.array());
    int[][] rows = VectorSource.readIvecs(path, 0);
    assertEquals(2, rows.length);
    assertArrayEquals(new int[] {5, 9}, rows[0]);
    assertArrayEquals(new int[] {1, 2, 3}, rows[1]);
    assertEquals(1, VectorSource.readIvecs(path, 1).length);
  }

  @Test
  public void testGroundTruthMatchesSortedDistances() throws Exception {
    VectorSource.Synthetic source = new VectorSource.Synthetic(8, 500, 10, 0.3f, 3);
    List<float[]> queries = List.of(source.query(0), source.query(1));
    List<String> categories = Arrays.asList(null, "c3");
    for (String metric : List.of("euclidean", "cosine")) {
      QuerySet set = QuerySet.compute(source, queries, categories, metric, 5, 3, null);
      for (int q = 0; q < 2; q++) {
        float[] query = queries.get(q);
        String category = categories.get(q);
        // expected: sort every allowed id by distance
        List<Long> ids = new ArrayList<>();
        for (long id = 0; id < 500; id++) {
          if (category == null || category.equals("c" + id % 10)) {
            ids.add(id);
          }
        }
        ids.sort(
            (x, y) ->
                Double.compare(
                    distance(metric, query, source.vector(x)),
                    distance(metric, query, source.vector(y))));
        long[] expected = ids.subList(0, 5).stream().mapToLong(Long::longValue).toArray();
        assertArrayEquals(metric + " query " + q, expected, set.queries().get(q).neighbors());
      }
    }
  }

  private static double distance(String metric, float[] a, float[] b) {
    return metric.equals("cosine")
        ? -QuerySet.dot(QuerySet.normalize(a), QuerySet.normalize(b))
        : QuerySet.squaredL2(a, b);
  }

  @Test
  public void testQuerySetRoundTrip() throws Exception {
    QuerySet set =
        new QuerySet(
            "cosine",
            3,
            List.of(
                new QuerySet.Query(new float[] {1, 2}, null, new long[] {4, 5, 6}),
                new QuerySet.Query(new float[] {3, 4}, "c7", new long[] {1})));
    Path path = folder.getRoot().toPath().resolve("q.bin");
    set.save(path);
    QuerySet loaded = QuerySet.load(path);
    assertEquals("cosine", loaded.metric());
    assertEquals(3, loaded.k());
    assertEquals(2, loaded.queries().size());
    assertArrayEquals(new float[] {3, 4}, loaded.queries().get(1).vector(), 0f);
    assertEquals("c7", loaded.queries().get(1).category());
    assertEquals(null, loaded.queries().get(0).category());
    assertArrayEquals(new long[] {4, 5, 6}, loaded.queries().get(0).neighbors());
  }

  @Test
  public void testRecall() throws Exception {
    JsonNode response =
        BenchClient.MAPPER.readTree(
            "{\"vectors\": [{\"key\": \""
                + BenchOptions.key(1)
                + "\"}, {\"key\": \""
                + BenchOptions.key(9)
                + "\"}, {\"key\": \""
                + BenchOptions.key(3)
                + "\"}]}");
    long[] exact = {1, 2, 3, 4};
    assertEquals(2.0 / 3, QueryCommand.recall(exact, response, 3), 1e-9);
    assertEquals(0.5, QueryCommand.recall(exact, response, 4), 1e-9);
    // fewer exact neighbors than topK, as with a selective filter
    assertEquals(1.0, QueryCommand.recall(new long[] {1, 3}, response, 10), 1e-9);
    assertEquals(1.0, QueryCommand.recall(new long[] {}, response, 10), 1e-9);
  }

  @Test
  public void testKeysAndMetadata() {
    assertEquals("v000000000042", BenchOptions.key(42));
    assertEquals(42, BenchOptions.id(BenchOptions.key(42)));
    assertTrue(BenchOptions.key(9).compareTo(BenchOptions.key(10)) < 0);
    assertEquals("c3", BenchOptions.metadata(13).get("category").asText());
  }

  @Test
  public void testLatencyPercentiles() {
    LatencyRecorder recorder = new LatencyRecorder();
    for (int i = 1; i <= 100; i++) {
      recorder.record(i * 1_000_000L);
    }
    recorder.recordError();
    LatencyRecorder.Summary summary = recorder.summary();
    assertEquals(100, summary.count());
    assertEquals(1, summary.errors());
    assertEquals(50.0, summary.percentileMs(50), 0);
    assertEquals(99.0, summary.percentileMs(99), 0);
    assertEquals(100.0, summary.maxMs(), 0);
    assertEquals(50.5, summary.meanMs(), 1e-9);
    recorder.reset();
    assertEquals(0, recorder.count());
  }

  @Test
  public void testRateLimiterSpacing() throws Exception {
    RateLimiter limiter = new RateLimiter(1000);
    // time before the first acquire doesn't make the first starts late
    Thread.sleep(50);
    long first = limiter.acquire();
    assertTrue(System.nanoTime() - first < 20_000_000);
    long second = limiter.acquire();
    long third = limiter.acquire();
    assertEquals(1_000_000, second - first);
    assertEquals(1_000_000, third - second);
    assertTrue(System.nanoTime() >= third);
    assertEquals(0, limiter.lateCount());
    assertFalse(new RateLimiter(0).isLimited());
  }

  @Test
  public void testRateLimiterCountsQueuedSlots() throws Exception {
    RateLimiter limiter = new RateLimiter(1000);
    limiter.acquire();
    // a sender that was busy for 20 intervals finds its next slots already due
    Thread.sleep(20);
    for (int i = 0; i < 10; i++) {
      limiter.acquire();
    }
    assertTrue("late " + limiter.lateCount(), limiter.lateCount() >= 5);
    limiter.resetLateCount();
    assertEquals(0, limiter.lateCount());
  }

  @Test
  public void testCacheMetricsParse() {
    String text =
        String.join(
            "\n",
            "# HELP nrt_lazy_remote_cache_hits_total Block reads served from the cache.",
            "# TYPE nrt_lazy_remote_cache_hits_total counter",
            "nrt_lazy_remote_cache_hits_total{cache_type=\"memory\"} 120.0",
            "nrt_lazy_remote_cache_misses_total{cache_type=\"memory\"} 30.0",
            "nrt_lazy_remote_cache_remote_bytes_total{cache_type=\"memory\"} 3.0E6",
            "nrt_lazy_remote_cache_remote_load_seconds_total{cache_type=\"memory\"} 0.6",
            "nrt_lazy_remote_cache_size_bytes{cache_type=\"memory\"} 1000000.0",
            "nrt_lazy_remote_cache_max_bytes{cache_type=\"memory\"} 2000000.0",
            "other_metric 5");
    CacheMetrics.Snapshot after = CacheMetrics.parse(text);
    assertEquals(120, after.get("hits_total"), 0);
    assertEquals(3e6, after.get("remote_bytes_total"), 0);
    CacheMetrics.Snapshot before =
        CacheMetrics.parse(
            "nrt_lazy_remote_cache_hits_total{cache_type=\"memory\"} 20.0\n"
                + "nrt_lazy_remote_cache_misses_total{cache_type=\"memory\"} 10.0");
    String delta = after.formatDelta(before, 10);
    assertTrue(delta, delta.startsWith("hit rate 83.3% (100 hits, 20 misses)"));
    assertTrue(delta, delta.contains("avg block load 30.0ms"));
    assertTrue(CacheMetrics.parse("other 1").isEmpty());
  }

  /** The cluster.sh config templates, with values filled in, are valid nrtsearch configs. */
  @Test
  public void testClusterConfigTemplatesParse() throws Exception {
    java.util.Map<String, String> values =
        java.util.Map.ofEntries(
            java.util.Map.entry("BENCH_HOME", folder.getRoot().toString()),
            java.util.Map.entry("BENCH_BUCKET", "bucket"),
            java.util.Map.entry("BENCH_SERVICE", "service"),
            java.util.Map.entry("PRIMARY_PORT", "50051"),
            java.util.Map.entry("PRIMARY_REPLICATION_PORT", "50052"),
            java.util.Map.entry("LAZY_PORT", "50061"),
            java.util.Map.entry("LAZY_REPLICATION_PORT", "50062"),
            java.util.Map.entry("LOCAL_PORT", "50071"),
            java.util.Map.entry("LOCAL_REPLICATION_PORT", "50072"),
            java.util.Map.entry("POLL_SECONDS", "5"),
            java.util.Map.entry("CACHE_TYPE", "OFF_HEAP"),
            java.util.Map.entry("CACHE_SIZE_MB", "1024"),
            java.util.Map.entry("BLOCK_SIZE_KB", "64"));
    Path dir = Path.of("scripts/vector_bench/config");
    for (String name : List.of("primary", "replica_lazy", "replica_local")) {
      String yaml = Files.readString(dir.resolve(name + ".yaml"));
      for (java.util.Map.Entry<String, String> e : values.entrySet()) {
        yaml = yaml.replace("${" + e.getKey() + "}", e.getValue());
      }
      // the same placeholder pattern cluster.sh fills in
      assertFalse(
          name + " has unfilled values",
          java.util.regex.Pattern.compile("\\$\\{[A-Z_]+}").matcher(yaml).find());
      com.yelp.nrtsearch.server.config.NrtsearchConfig config =
          new com.yelp.nrtsearch.server.config.NrtsearchConfig(
              new java.io.ByteArrayInputStream(
                  yaml.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
      assertEquals("bucket", config.getBucketName());
      switch (name) {
        case "primary" -> {
          assertEquals(
              com.yelp.nrtsearch.server.grpc.Mode.PRIMARY, config.getIndexStartConfig().getMode());
          assertTrue(config.getS3RefreshUpload());
        }
        case "replica_lazy" -> {
          assertTrue(config.getIsolatedReplicaConfig().isEnabled());
          assertTrue(config.getLazyRemoteDataConfig().isEnabled());
          assertEquals(
              com.yelp.nrtsearch.server.config.LazyRemoteDataConfig.CacheType.OFF_HEAP,
              config.getLazyRemoteDataConfig().getCacheType());
          assertEquals(64 * 1024, config.getLazyRemoteDataConfig().getBlockSizeBytes());
        }
        case "replica_local" -> {
          assertTrue(config.getIsolatedReplicaConfig().isEnabled());
          assertFalse(config.getLazyRemoteDataConfig().isEnabled());
        }
        default -> throw new AssertionError(name);
      }
    }
  }
}
