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
package com.yelp.nrtsearch.tools.nrt_utils.state;

import static com.yelp.nrtsearch.server.grpc.TestServer.S3_ENDPOINT;
import static com.yelp.nrtsearch.server.grpc.TestServer.TEST_BUCKET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexGlobalState;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.server.remote.RemoteBackend.IndexResourceType;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.remote.s3.S3Util;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class GetFenceStateCommandTest {
  private static final String SERVICE = "fence_service";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  @ClassRule public static final TemporaryFolder s3Folder = new TemporaryFolder();

  private final PrintStream originalOut = System.out;
  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private S3Client s3;

  @BeforeClass
  public static void setUpS3() throws Exception {
    TestServer.initS3(s3Folder);
  }

  @Before
  public void setUp() {
    TestServer.resetS3Bucket();
    s3 = AmazonS3Provider.createTestS3Client(S3_ENDPOINT);
    s3.createBucket(CreateBucketRequest.builder().bucket(TEST_BUCKET).build());
    out.reset();
    System.setOut(new PrintStream(out));
  }

  @After
  public void tearDown() {
    System.setOut(originalOut);
  }

  @AfterClass
  public static void cleanupClass() {
    TestServer.cleanupAll();
  }

  private void put(String key, String body) {
    s3.putObject(
        PutObjectRequest.builder().bucket(TEST_BUCKET).key(key).build(),
        RequestBody.fromString(body));
  }

  private List<JsonNode> run() throws Exception {
    GetFenceStateCommand command = new GetFenceStateCommand();
    command.setS3ClientBundle(
        new S3Util.S3ClientBundle(s3, AmazonS3Provider.createTestS3AsyncClient(S3_ENDPOINT)));
    assertEquals(
        0, new CommandLine(command).execute("-s", SERVICE, "-b", TEST_BUCKET, "--region", "x"));
    List<JsonNode> nodes = new ArrayList<>();
    for (String line : out.toString().split(System.lineSeparator())) {
      if (line.startsWith("{")) {
        nodes.add(MAPPER.readTree(line));
      }
    }
    return nodes;
  }

  @Test
  public void testFenceState() throws Exception {
    put(
        SERVICE + "/leader/lease",
        "{\"holder\":{\"nodeName\":\"p1\",\"epoch\":7},\"ttlMs\":10000,\"renewal\":3}");
    // a real global state listing the index, then its pointer rewritten as a fenced pointer
    S3Backend backend =
        new S3Backend(
            TEST_BUCKET,
            false,
            S3Backend.DEFAULT_CONFIG,
            new S3Util.S3ClientBundle(s3, AmazonS3Provider.createTestS3AsyncClient(S3_ENDPOINT)));
    backend.uploadGlobalState(
        SERVICE,
        JsonFormat.printer()
            .print(
                GlobalStateInfo.newBuilder()
                    .setGen(1)
                    .putIndices(
                        "idx", IndexGlobalState.newBuilder().setId("1234").setStarted(true).build())
                    .build())
            .getBytes(StandardCharsets.UTF_8));
    String globalPrefix = S3Backend.getGlobalStateResourcePrefix(SERVICE);
    String globalFile = backend.getCurrentResourceName(globalPrefix);
    put(globalPrefix + "_current", globalFile + "\n#nrt-fence epoch=7 nonce=a");
    String index = "idx-1234";
    put(
        S3Backend.getIndexResourcePrefix(SERVICE, index, IndexResourceType.INDEX_STATE)
            + "_current",
        "state-1\n#nrt-fence epoch=7 nonce=b");
    // tombstone: claimed before the resource existed
    put(
        S3Backend.getIndexResourcePrefix(SERVICE, index, IndexResourceType.POINT_STATE)
            + "_current",
        "\n#nrt-fence epoch=7 nonce=c");
    // unfenced pointer, written without leader election
    put(
        S3Backend.getIndexResourcePrefix(SERVICE, index, IndexResourceType.WARMING_QUERIES)
            + "_current",
        "warm-1");

    List<JsonNode> nodes = run();
    assertEquals(nodes.toString(), 5, nodes.size());
    JsonNode lease = nodes.get(0);
    assertEquals("LEASE", lease.get("type").asText());
    assertEquals("p1", lease.get("lease").get("holder").get("nodeName").asText());
    assertEquals(3, lease.get("lease").get("renewal").asLong());

    JsonNode global = nodes.get(1);
    assertEquals("GLOBAL_STATE", global.get("type").asText());
    assertEquals(globalFile, global.get("value").asText());
    assertEquals(7, global.get("epoch").asLong());
    assertTrue(global.has("etag"));

    for (JsonNode node : nodes.subList(2, 5)) {
      assertEquals(index, node.get("index").asText());
      switch (node.get("type").asText()) {
        case "INDEX_STATE" -> {
          assertEquals("state-1", node.get("value").asText());
          assertEquals(7, node.get("epoch").asLong());
          assertFalse(node.get("tombstone").asBoolean());
        }
        case "POINT_STATE" -> assertTrue(node.get("tombstone").asBoolean());
        case "WARMING_QUERIES" -> {
          assertEquals("warm-1", node.get("value").asText());
          assertEquals(0, node.get("epoch").asLong());
        }
        default -> throw new AssertionError("unexpected " + node);
      }
    }
  }

  @Test
  public void testEmptyService() throws Exception {
    List<JsonNode> nodes = run();
    assertEquals(nodes.toString(), 2, nodes.size());
    assertFalse(nodes.get(0).get("exists").asBoolean());
    assertEquals("GLOBAL_STATE", nodes.get(1).get("type").asText());
    assertFalse(nodes.get(1).get("exists").asBoolean());
  }

  @Test
  public void testMalformedPointer() throws Exception {
    put(S3Backend.getGlobalStateResourcePrefix(SERVICE) + "_current", "gs-1\n#nrt-fence nonce=a");
    JsonNode global = run().get(1);
    assertTrue(global.has("malformed"));
  }
}
