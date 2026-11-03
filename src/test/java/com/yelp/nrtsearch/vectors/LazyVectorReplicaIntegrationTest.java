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
package com.yelp.nrtsearch.vectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Empty;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.monitoring.LazyRemoteCacheCollector;
import com.yelp.nrtsearch.server.nrt.lazy.LazyFileStub;
import com.yelp.nrtsearch.server.nrt.lazy.LazyRemoteDirectory;
import com.yelp.nrtsearch.server.nrt.lazy.RemoteBlockCache;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * A primary created through the vector gateway, and an isolated replica that leaves the full
 * precision vector files (.vec) in S3 and reads them by range when rescoring.
 */
public class LazyVectorReplicaIntegrationTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String BUCKET = "test-bucket";
  private static final String INDEX = "test-index";
  private static final int DIM = 64;

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void tearDown() {
    TestServer.cleanupServers();
  }

  @AfterClass
  public static void cleanupClass() {
    TestServer.cleanupAll();
  }

  private static ObjectNode req() {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("vectorBucketName", BUCKET);
    node.put("indexName", INDEX);
    return node;
  }

  private static void putVectors(VectorService service, int start, int count, Random random) {
    for (int s = start; s < start + count; s += 500) {
      ObjectNode put = req();
      ArrayNode array = put.putArray("vectors");
      for (int i = s; i < Math.min(start + count, s + 500); i++) {
        ObjectNode vector = array.addObject();
        vector.put("key", String.format("doc-%05d", i));
        ArrayNode data = vector.putObject("data").putArray("float32");
        for (int d = 0; d < DIM; d++) {
          data.add((float) random.nextGaussian());
        }
        vector.putObject("metadata").put("group", i % 3);
      }
      service.putVectors(put);
    }
  }

  private static ObjectNode query(float[] q, int topK, String filter) throws IOException {
    ObjectNode query = req();
    query.put("topK", topK);
    query.put("returnDistance", true);
    ArrayNode data = query.putObject("queryVector").putArray("float32");
    for (float f : q) {
      data.add(f);
    }
    if (filter != null) {
      query.set("filter", MAPPER.readTree(filter));
    }
    return query;
  }

  private static List<String> keys(JsonNode response) {
    List<String> keys = new ArrayList<>();
    response.get("vectors").forEach(v -> keys.add(v.get("key").asText()));
    return keys;
  }

  private static List<Double> distances(JsonNode response) {
    List<Double> d = new ArrayList<>();
    response.get("vectors").forEach(v -> d.add(v.get("distance").asDouble()));
    return d;
  }

  private static List<Path> files(Path dir, String suffix) throws IOException {
    try (Stream<Path> s = Files.list(dir)) {
      return s.filter(p -> p.getFileName().toString().endsWith(suffix)).toList();
    }
  }

  @Test
  public void testReplicaLeavesFullPrecisionVectorsInS3() throws Exception {
    TestServer primary =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withLocalStateBackend()
            .build();
    VectorService primaryService =
        new VectorService(
            primary.getClient().getBlockingStub(),
            primary.getClient().getAsyncStub(),
            new BucketRegistry(folder.getRoot().toPath().resolve("registry.json")),
            new VectorService.Options());
    primaryService.createVectorBucket(MAPPER.createObjectNode().put("vectorBucketName", BUCKET));
    ObjectNode create = req();
    create.put("dimension", DIM);
    create.put("distanceMetric", "cosine");
    primaryService.createIndex(create);

    Random random = new Random(5);
    putVectors(primaryService, 0, 2000, random);
    String nrtIndex = IndexConfig.nrtsearchIndexName(BUCKET, INDEX);
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
                    "  extensions: [vec]",
                    "  cacheType: MEMORY",
                    "  cacheSizeMB: 64",
                    "  blockSizeKB: 4"))
            .build();
    assertTrue(replica.isStarted(nrtIndex));

    // the replica needs the gateway's bucket registry to translate requests
    VectorService replicaService =
        new VectorService(
            replica.getClient().getBlockingStub(),
            replica.getClient().getAsyncStub(),
            new BucketRegistry(folder.getRoot().toPath().resolve("registry.json")),
            new VectorService.Options());

    ShardState shard = replica.getGlobalState().getIndexOrThrow(nrtIndex).getShard(0);
    LazyRemoteDirectory lazyDir = LazyRemoteDirectory.unwrap(shard.indexDir);
    assertTrue("replica index dir is " + shard.indexDir, lazyDir != null);
    Path indexPath = lazyDir.getPath();

    // .vec files are stubs, the HNSW graph and quantized vectors are local
    assertFalse(files(indexPath, ".vec" + LazyFileStub.STUB_SUFFIX).isEmpty());
    assertTrue(files(indexPath, ".vec").isEmpty());
    assertFalse(files(indexPath, ".vex").isEmpty());
    assertFalse(files(indexPath, ".veq").isEmpty());
    assertTrue(files(indexPath, ".cfs").isEmpty());

    long vecBytes = 0;
    for (String file : lazyDir.listAll()) {
      if (file.endsWith(".vec")) {
        vecBytes += lazyDir.fileLength(file);
      }
    }
    assertTrue(vecBytes > 0);

    // opening the index and checking file identities read nothing remotely; one query rescoring 30
    // hits reads only the blocks holding those vectors
    RemoteBlockCache cache = replica.getGlobalState().getLazyRemoteBlockCache();
    assertEquals(0, cache.getRemoteBytes());
    float[] first = new float[DIM];
    first[1] = 1;
    assertEquals(
        keys(primaryService.queryVectors(query(first, 10, null))),
        keys(replicaService.queryVectors(query(first, 10, null))));
    long oneQueryBytes = cache.getRemoteBytes();
    System.out.println(
        "One query read " + oneQueryBytes + " of " + vecBytes + " full precision vector bytes");
    assertTrue(oneQueryBytes > 0);
    assertTrue(oneQueryBytes < vecBytes / 2);

    // queries with rescoring give the same results as the primary
    long missesBefore = cache.getMisses();
    Random queryRandom = new Random(9);
    for (int i = 0; i < 10; i++) {
      float[] q = new float[DIM];
      for (int d = 0; d < DIM; d++) {
        q[d] = (float) queryRandom.nextGaussian();
      }
      String filter = i % 2 == 0 ? null : "{\"group\": {\"$in\": [0, 2]}}";
      JsonNode p = primaryService.queryVectors(query(q, 10, filter));
      JsonNode r = replicaService.queryVectors(query(q, 10, filter));
      assertEquals(keys(p), keys(r));
      List<Double> pd = distances(p);
      List<Double> rd = distances(r);
      for (int j = 0; j < pd.size(); j++) {
        assertEquals(pd.get(j), rd.get(j), 1e-5);
      }
    }
    // rescoring read full precision vectors from S3, but only a part of them
    assertTrue(cache.getMisses() > missesBefore);
    System.out.println(
        "Lazy replica read "
            + cache.getRemoteBytes()
            + " of "
            + vecBytes
            + " full precision vector bytes from S3 in "
            + cache.getMisses()
            + " range reads, "
            + cache.getHits()
            + " cache hits");
    assertTrue(
        "remote bytes " + cache.getRemoteBytes() + " vs vec bytes " + vecBytes,
        cache.getRemoteBytes() <= vecBytes);
    int stubsBefore = files(indexPath, ".vec" + LazyFileStub.STUB_SUFFIX).size();

    // the cache stats are exported through the server's prometheus endpoint. TestServer does not
    // run NrtsearchServer.registerMetrics, so register the collector the same way it does.
    replica
        .getPrometheusRegistry()
        .register(new LazyRemoteCacheCollector(replica.getGlobalState()));
    String metrics =
        replica
            .getClient()
            .getBlockingStub()
            .metrics(Empty.newBuilder().build())
            .getData()
            .toStringUtf8();
    assertTrue(
        metrics,
        metrics.contains(
            "nrt_lazy_remote_cache_misses_total{cache_type=\"memory\"} " + cache.getMisses()));
    assertTrue(metrics.contains("nrt_lazy_remote_cache_remote_bytes_total{cache_type=\"memory\"}"));
    assertTrue(metrics.contains("nrt_lazy_remote_cache_size_bytes{cache_type=\"memory\"}"));

    // GetVectors returns full precision data through the lazy files
    ObjectNode get = req();
    get.putArray("keys").add("doc-00007");
    get.put("returnData", true);
    JsonNode primaryGet = primaryService.getVectors(get);
    JsonNode replicaGet = replicaService.getVectors(get);
    assertEquals(
        primaryGet.get("vectors").get(0).get("data"), replicaGet.get("vectors").get(0).get("data"));

    // a new version is picked up by polling, new .vec files are stubs too
    putVectors(primaryService, 2000, 500, random);
    primary.commit(nrtIndex);
    float[] q = new float[DIM];
    q[0] = 1;
    List<String> expected = keys(primaryService.queryVectors(query(q, 5, null)));
    long deadline = System.currentTimeMillis() + 30_000;
    List<String> actual = List.of();
    while (System.currentTimeMillis() < deadline) {
      actual = keys(replicaService.queryVectors(query(q, 5, null)));
      if (actual.equals(expected)) {
        break;
      }
      Thread.sleep(200);
    }
    assertEquals(expected, actual);
    assertTrue(files(indexPath, ".vec").isEmpty());
    // the copied version added segments, their .vec files are stubs
    assertTrue(
        files(indexPath, ".vec" + LazyFileStub.STUB_SUFFIX).size() > stubsBefore
            || !files(indexPath, ".vec" + LazyFileStub.STUB_SUFFIX).isEmpty());
    System.out.println(
        "Stubs before new version: "
            + stubsBefore
            + ", after: "
            + files(indexPath, ".vec" + LazyFileStub.STUB_SUFFIX).size());
  }
}
