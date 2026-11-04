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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.vectors.IndexConfig.DistanceMetric;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end test: an nrtsearch primary that keeps index data in S3 (S3Mock), with the vector
 * gateway HTTP server in front of it.
 */
public class VectorGatewayIntegrationTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String BUCKET = "test-bucket";
  private static final String INDEX = "test-index";
  private static final int DIM = 32;
  private static final int NUM_VECTORS = 1000;
  private static final String[] GENRES = {"drama", "comedy", "documentary", "horror"};

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private TestServer server;
  private VectorService service;
  private VectorGatewayServer gateway;
  private final HttpClient http = HttpClient.newHttpClient();
  private final Map<String, float[]> vectors = new TreeMap<>();

  @Before
  public void setUp() throws IOException {
    server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withRemoteStateBackend(false)
            .build();
    VectorService.Options options = new VectorService.Options();
    options.idleStopMs = 60_000;
    service =
        new VectorService(
            server.getClient().getBlockingStub(),
            server.getClient().getAsyncStub(),
            new BucketRegistry(folder.getRoot().toPath().resolve("registry.json")),
            options);
    gateway = new VectorGatewayServer(service, 0);
    gateway.start();
  }

  @After
  public void tearDown() {
    if (gateway != null) {
      gateway.stop();
    }
    TestServer.cleanupServers();
  }

  @AfterClass
  public static void cleanupClass() {
    TestServer.cleanupAll();
  }

  private JsonNode call(String operation, Object body) throws Exception {
    HttpResponse<String> response = callRaw(operation, body);
    if (response.statusCode() != 200) {
      throw new AssertionError(
          operation + " failed with " + response.statusCode() + ": " + response.body());
    }
    return MAPPER.readTree(response.body());
  }

  private HttpResponse<String> callRaw(String operation, Object body) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(
                URI.create("http://localhost:" + gateway.getPort() + "/" + operation))
            .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
            .header("Content-Type", "application/json")
            .build();
    return http.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static ObjectNode req() {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("vectorBucketName", BUCKET);
    node.put("indexName", INDEX);
    return node;
  }

  private void createIndex(String metric) throws Exception {
    call("CreateVectorBucket", Map.of("vectorBucketName", BUCKET));
    ObjectNode create = req();
    create.put("dataType", "float32");
    create.put("dimension", DIM);
    create.put("distanceMetric", metric);
    create.putObject("metadataConfiguration").putArray("nonFilterableMetadataKeys").add("text");
    call("CreateIndex", create);
  }

  private void putRandomVectors(int count) throws Exception {
    Random random = new Random(42);
    for (int start = 0; start < count; start += 500) {
      ObjectNode put = req();
      ArrayNode array = put.putArray("vectors");
      for (int i = start; i < Math.min(count, start + 500); i++) {
        String key = String.format("doc-%05d", i);
        float[] v = new float[DIM];
        for (int d = 0; d < DIM; d++) {
          v[d] = (float) random.nextGaussian();
        }
        vectors.put(key, v);
        ObjectNode vector = array.addObject();
        vector.put("key", key);
        ArrayNode data = vector.putObject("data").putArray("float32");
        for (float f : v) {
          data.add(f);
        }
        ObjectNode metadata = vector.putObject("metadata");
        metadata.put("genre", GENRES[i % GENRES.length]);
        metadata.put("year", 2000 + (i % 25));
        metadata.put("text", "body of " + key);
        if (i % 2 == 0) {
          metadata.putArray("tags").add("even").add("t" + (i % 10));
        }
      }
      call("PutVectors", put);
    }
  }

  private ObjectNode queryRequest(float[] q, int topK) {
    ObjectNode query = req();
    query.put("topK", topK);
    ArrayNode data = query.putObject("queryVector").putArray("float32");
    for (float f : q) {
      data.add(f);
    }
    query.put("returnDistance", true);
    query.put("returnMetadata", true);
    return query;
  }

  private List<String> bruteForce(float[] q, int k, DistanceMetric metric, Set<String> allowed) {
    return vectors.entrySet().stream()
        .filter(e -> allowed == null || allowed.contains(e.getKey()))
        .sorted(
            Comparator.comparingDouble(e -> DistanceConverter.distance(q, e.getValue(), metric)))
        .limit(k)
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  private static List<String> keys(JsonNode response) {
    List<String> keys = new ArrayList<>();
    response.get("vectors").forEach(v -> keys.add(v.get("key").asText()));
    return keys;
  }

  private static double recall(List<String> expected, List<String> actual) {
    Set<String> found = new HashSet<>(actual);
    return expected.stream().filter(found::contains).count() / (double) expected.size();
  }

  @Test
  public void testQueryRecallCosine() throws Exception {
    queryRecall("cosine", DistanceMetric.COSINE);
  }

  @Test
  public void testQueryRecallEuclidean() throws Exception {
    queryRecall("euclidean", DistanceMetric.EUCLIDEAN);
  }

  private void queryRecall(String metricName, DistanceMetric metric) throws Exception {
    createIndex(metricName);
    putRandomVectors(NUM_VECTORS);
    Random random = new Random(7);
    double totalRecall = 0;
    int queries = 20;
    for (int i = 0; i < queries; i++) {
      float[] q = new float[DIM];
      for (int d = 0; d < DIM; d++) {
        q[d] = (float) random.nextGaussian();
      }
      JsonNode response = call("QueryVectors", queryRequest(q, 10));
      assertEquals(metricName, response.get("distanceMetric").asText());
      List<String> actual = keys(response);
      assertEquals(10, actual.size());
      totalRecall += recall(bruteForce(q, 10, metric, null), actual);

      // returned distances are sorted and close to the exact distance
      double previous = -1;
      for (JsonNode v : response.get("vectors")) {
        double distance = v.get("distance").asDouble();
        assertTrue(distance >= previous - 1e-4);
        previous = distance;
        float exact = DistanceConverter.distance(q, vectors.get(v.get("key").asText()), metric);
        assertEquals(exact, distance, 0.02 * Math.max(1, exact));
        assertTrue(v.get("metadata").has("genre"));
        assertTrue(v.get("metadata").has("text"));
      }
    }
    double avgRecall = totalRecall / queries;
    assertTrue("recall was " + avgRecall, avgRecall >= 0.95);
  }

  @Test
  public void testFilteredQuery() throws Exception {
    createIndex("cosine");
    putRandomVectors(NUM_VECTORS);
    float[] q = vectors.get("doc-00003");

    ObjectNode query = queryRequest(q, 20);
    query.set(
        "filter",
        MAPPER.readTree(
            "{\"$and\": [{\"genre\": {\"$in\": [\"drama\", \"comedy\"]}}, "
                + "{\"year\": {\"$gte\": 2010, \"$lt\": 2020}}]}"));
    JsonNode response = call("QueryVectors", query);
    Set<String> allowed = new HashSet<>();
    for (int i = 0; i < NUM_VECTORS; i++) {
      int year = 2000 + (i % 25);
      String genre = GENRES[i % GENRES.length];
      if ((genre.equals("drama") || genre.equals("comedy")) && year >= 2010 && year < 2020) {
        allowed.add(String.format("doc-%05d", i));
      }
    }
    List<String> actual = keys(response);
    assertEquals(20, actual.size());
    assertTrue(allowed.containsAll(actual));
    assertTrue(recall(bruteForce(q, 20, DistanceMetric.COSINE, allowed), actual) >= 0.9);

    // very selective filter (one vector) still returns it: pre-filtering
    ObjectNode selective = queryRequest(q, 10);
    selective.set(
        "filter",
        MAPPER.readTree("{\"genre\": \"horror\", \"year\": 2024, \"tags\": {\"$exists\": false}}"));
    List<String> selectiveKeys = keys(call("QueryVectors", selective));
    Set<String> expected = new HashSet<>();
    for (int i = 0; i < NUM_VECTORS; i++) {
      if (i % 4 == 3 && (2000 + i % 25) == 2024 && i % 2 != 0) {
        expected.add(String.format("doc-%05d", i));
      }
    }
    assertEquals(Math.min(10, expected.size()), selectiveKeys.size());
    assertTrue(expected.containsAll(selectiveKeys));

    // list metadata: $eq matches any element; $startsWith; $ne; $or
    ObjectNode listEq = queryRequest(q, 50);
    listEq.set("filter", MAPPER.readTree("{\"tags\": \"t4\"}"));
    for (String key : keys(call("QueryVectors", listEq))) {
      assertEquals(4, Integer.parseInt(key.substring(4)) % 10);
    }
    ObjectNode prefix = queryRequest(q, 50);
    prefix.set("filter", MAPPER.readTree("{\"genre\": {\"$startsWith\": \"doc\"}}"));
    for (JsonNode v : call("QueryVectors", prefix).get("vectors")) {
      assertEquals("documentary", v.get("metadata").get("genre").asText());
    }
    ObjectNode ne = queryRequest(q, 50);
    ne.set(
        "filter",
        MAPPER.readTree(
            "{\"$or\": [{\"genre\": {\"$ne\": \"drama\"}}, {\"year\": {\"$gt\": 2100}}]}"));
    for (JsonNode v : call("QueryVectors", ne).get("vectors")) {
      assertFalse(v.get("metadata").get("genre").asText().equals("drama"));
    }

    // non-filterable key can't be used in a filter
    ObjectNode bad = queryRequest(q, 10);
    bad.set("filter", MAPPER.readTree("{\"text\": \"x\"}"));
    HttpResponse<String> badResponse = callRaw("QueryVectors", bad);
    assertEquals(400, badResponse.statusCode());
    assertTrue(badResponse.body().contains("ValidationException"));
  }

  @Test
  public void testOverwriteDeleteGetConsistency() throws Exception {
    createIndex("cosine");
    putRandomVectors(100);
    float[] target = vectors.get("doc-00050");

    // overwrite doc-00001 with the target vector: immediately the best match
    ObjectNode put = req();
    ObjectNode vector = put.putArray("vectors").addObject();
    vector.put("key", "doc-00001");
    ArrayNode data = vector.putObject("data").putArray("float32");
    for (float f : target) {
      data.add(f);
    }
    vector.putObject("metadata").put("genre", "new");
    call("PutVectors", put);

    List<String> top = keys(call("QueryVectors", queryRequest(target, 2)));
    assertEquals(Set.of("doc-00001", "doc-00050"), new HashSet<>(top));

    // old metadata is gone
    ObjectNode get = req();
    get.putArray("keys").add("doc-00001").add("doc-00002").add("missing");
    get.put("returnData", true);
    get.put("returnMetadata", true);
    JsonNode got = call("GetVectors", get);
    assertEquals(2, got.get("vectors").size());
    for (JsonNode v : got.get("vectors")) {
      if (v.get("key").asText().equals("doc-00001")) {
        assertEquals("new", v.get("metadata").get("genre").asText());
        assertFalse(v.get("metadata").has("year"));
        assertEquals(DIM, v.get("data").get("float32").size());
        assertEquals(target[0], v.get("data").get("float32").get(0).floatValue(), 1e-6);
      }
    }

    // delete: immediately gone from queries and gets
    ObjectNode delete = req();
    delete.putArray("keys").add("doc-00001").add("doc-00050");
    call("DeleteVectors", delete);
    List<String> afterDelete = keys(call("QueryVectors", queryRequest(target, 5)));
    assertFalse(afterDelete.contains("doc-00001"));
    assertFalse(afterDelete.contains("doc-00050"));
    ObjectNode getDeleted = req();
    getDeleted.putArray("keys").add("doc-00001");
    assertEquals(0, call("GetVectors", getDeleted).get("vectors").size());
  }

  @Test
  public void testListVectorsPagination() throws Exception {
    createIndex("cosine");
    putRandomVectors(250);
    List<String> listed = new ArrayList<>();
    String token = null;
    int pages = 0;
    do {
      ObjectNode list = req();
      list.put("maxResults", 100);
      if (token != null) {
        list.put("nextToken", token);
      }
      JsonNode response = call("ListVectors", list);
      listed.addAll(keys(response));
      token = response.has("nextToken") ? response.get("nextToken").asText() : null;
      pages++;
    } while (token != null && pages < 10);
    assertEquals(new ArrayList<>(vectors.keySet()), listed);
    assertTrue(pages >= 3);
  }

  @Test
  public void testValidation() throws Exception {
    createIndex("cosine");
    // wrong dimension
    ObjectNode put = req();
    ObjectNode v = put.putArray("vectors").addObject();
    v.put("key", "a");
    v.putObject("data").putArray("float32").add(1.0f);
    assertEquals(400, callRaw("PutVectors", put).statusCode());

    // type conflict on metadata key
    putRandomVectors(1);
    ObjectNode conflict = req();
    ObjectNode cv = conflict.putArray("vectors").addObject();
    cv.put("key", "b");
    ArrayNode data = cv.putObject("data").putArray("float32");
    for (int i = 0; i < DIM; i++) {
      data.add(1.0f);
    }
    cv.putObject("metadata").put("year", "not a number");
    assertEquals(400, callRaw("PutVectors", conflict).statusCode());

    // unknown index
    ObjectNode missing = req();
    missing.put("indexName", "nope");
    missing.putArray("keys").add("a");
    HttpResponse<String> response = callRaw("GetVectors", missing);
    assertEquals(404, response.statusCode());

    // bucket with indexes can't be deleted
    assertEquals(
        409, callRaw("DeleteVectorBucket", Map.of("vectorBucketName", BUCKET)).statusCode());
    call("DeleteIndex", req());
    call("DeleteVectorBucket", Map.of("vectorBucketName", BUCKET));
    assertEquals(0, call("ListVectorBuckets", Map.of()).get("vectorBuckets").size());
  }

  @Test
  public void testIndexMetadataApis() throws Exception {
    createIndex("euclidean");
    JsonNode index = call("GetIndex", req()).get("index");
    assertEquals(DIM, index.get("dimension").asInt());
    assertEquals("euclidean", index.get("distanceMetric").asText());
    assertEquals(
        "text",
        index.get("metadataConfiguration").get("nonFilterableMetadataKeys").get(0).asText());
    JsonNode list = call("ListIndexes", Map.of("vectorBucketName", BUCKET));
    assertEquals(1, list.get("indexes").size());
    assertEquals(
        BUCKET,
        call("GetVectorBucket", Map.of("vectorBucketName", BUCKET))
            .get("vectorBucket")
            .get("vectorBucketName")
            .asText());
  }

  @Test
  public void testIdleIndexStopsAndRestartsFromS3() throws Exception {
    createIndex("cosine");
    putRandomVectors(200);
    float[] q = vectors.get("doc-00010");
    List<String> before = keys(call("QueryVectors", queryRequest(q, 10)));

    String nrtIndex = IndexConfig.nrtsearchIndexName(BUCKET, INDEX);
    long[] now = {System.currentTimeMillis()};
    service.getTiering().setClock(() -> now[0]);
    now[0] += 120_000;
    assertEquals(List.of(nrtIndex), service.getTiering().stopIdleIndexes());
    assertFalse(server.isStarted(nrtIndex));

    // stopped index is started again, restoring from S3, on the next request
    List<String> after = keys(call("QueryVectors", queryRequest(q, 10)));
    assertTrue(server.isStarted(nrtIndex));
    assertEquals(before, after);

    // writes work after restart too
    ObjectNode delete = req();
    delete.putArray("keys").add("doc-00010");
    call("DeleteVectors", delete);
    assertFalse(keys(call("QueryVectors", queryRequest(q, 10))).contains("doc-00010"));

    // a restarted gateway reloads bucket and schema state from the registry file
    VectorService reloaded =
        new VectorService(
            server.getClient().getBlockingStub(),
            server.getClient().getAsyncStub(),
            new BucketRegistry(folder.getRoot().toPath().resolve("registry.json")),
            new VectorService.Options());
    ObjectNode filtered = queryRequest(q, 5);
    filtered.set("filter", MAPPER.readTree("{\"genre\": \"comedy\"}"));
    JsonNode response = reloaded.queryVectors(filtered);
    assertEquals(5, response.get("vectors").size());
    for (JsonNode v : response.get("vectors")) {
      assertEquals("comedy", v.get("metadata").get("genre").asText());
    }
    assertNull(response.get("nextToken"));
  }

  @Test
  public void testCreateIndexFailuresLeaveNothingBehind() throws Exception {
    createIndex("cosine");
    String nrtIndex = IndexConfig.nrtsearchIndexName(BUCKET, INDEX);
    assertTrue(server.indices().contains(nrtIndex));

    // a duplicate is a conflict, and doesn't touch the existing nrtsearch index
    ObjectNode duplicate = req();
    duplicate.put("dimension", DIM);
    duplicate.put("distanceMetric", "cosine");
    HttpResponse<String> response = callRaw("CreateIndex", duplicate);
    assertEquals(409, response.statusCode());
    assertTrue(server.isStarted(nrtIndex));

    // the index can be deleted and created again
    call("DeleteIndex", req());
    assertFalse(server.indices().contains(nrtIndex));
    call("CreateIndex", duplicate);
    assertTrue(server.indices().contains(nrtIndex));

    // a create that fails after the nrtsearch index exists deletes it again, and leaves no
    // registry entry. Fail startIndexV2 with a client interceptor, after a real createIndex.
    ClientInterceptor failStart =
        new ClientInterceptor() {
          @Override
          public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
              MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            if (method.getFullMethodName().endsWith("/startIndexV2")) {
              throw Status.UNAVAILABLE.withDescription("injected").asRuntimeException();
            }
            return next.newCall(method, callOptions);
          }
        };
    ObjectNode bad = req();
    bad.put("indexName", "bad-index");
    bad.put("dimension", DIM);
    bad.put("distanceMetric", "cosine");
    VectorService badService =
        new VectorService(
            server.getClient().getBlockingStub().withInterceptors(failStart),
            server.getClient().getAsyncStub(),
            service.getRegistry(),
            new VectorService.Options());
    VectorsException e = assertThrows(VectorsException.class, () -> badService.createIndex(bad));
    assertEquals(503, e.getHttpStatus());
    String badNrtIndex = IndexConfig.nrtsearchIndexName(BUCKET, "bad-index");
    assertFalse(server.indices().contains(badNrtIndex));
    assertEquals(1, call("ListIndexes", Map.of("vectorBucketName", BUCKET)).get("indexes").size());
    // the reserved name was released
    VectorService goodService =
        new VectorService(
            server.getClient().getBlockingStub(),
            server.getClient().getAsyncStub(),
            service.getRegistry(),
            new VectorService.Options());
    goodService.createIndex(bad);
    assertTrue(server.isStarted(badNrtIndex));
  }

  @Test
  public void testCreateIndexRegistrySaveFailureCleansUp() throws Exception {
    call("CreateVectorBucket", Map.of("vectorBucketName", BUCKET));
    Path registryFile = folder.getRoot().toPath().resolve("registry.json");
    // make the registry save fail after the nrtsearch index is created and started
    Path tmp = registryFile.resolveSibling("registry.json.tmp");
    Files.createDirectory(tmp);

    ObjectNode create = req();
    create.put("dimension", DIM);
    create.put("distanceMetric", "cosine");
    assertEquals(500, callRaw("CreateIndex", create).statusCode());
    // nothing is left in nrtsearch or the registry
    assertFalse(server.indices().contains(IndexConfig.nrtsearchIndexName(BUCKET, INDEX)));
    assertEquals(0, call("ListIndexes", Map.of("vectorBucketName", BUCKET)).get("indexes").size());
    assertEquals(404, callRaw("GetIndex", req()).statusCode());

    // once saving works the same index can be created
    Files.delete(tmp);
    call("CreateIndex", create);
    assertTrue(server.isStarted(IndexConfig.nrtsearchIndexName(BUCKET, INDEX)));
  }
}
