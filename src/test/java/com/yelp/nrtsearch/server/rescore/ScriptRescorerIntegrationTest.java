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
package com.yelp.nrtsearch.server.rescore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.Rescorer;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.ScriptRescorer;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Integration tests for {@link ScriptRescore} wired through the full search stack via {@code
 * scriptRescorer} in a {@link Rescorer} proto.
 *
 * <p>The index has 100 docs with:
 *
 * <ul>
 *   <li>{@code int_score} = (100 - doc_id) — used as a field-based boost signal
 *   <li>{@code int_field} = doc_id — used for range queries
 * </ul>
 */
public class ScriptRescorerIntegrationTest extends ServerTestCase {

  private static final String TEST_INDEX = "test_index";
  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public List<String> getIndices() {
    return Collections.singletonList(TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/rescore/RescoreRegisterFields.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndexOrThrow(name).getShard(0).writer;
    // try to create multiple segments to validate the implement across different leaves
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    List<Integer> idList = new ArrayList<>();
    for (int i = 0; i < NUM_DOCS; ++i) {
      idList.add(i);
    }
    Collections.shuffle(idList);

    List<AddDocumentRequest> requestChunk = new ArrayList<>();
    for (Integer id : idList) {
      requestChunk.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "int_score",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(NUM_DOCS - id))
                      .build())
              .putFields(
                  "int_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .build());

      if (requestChunk.size() == SEGMENT_CHUNK) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }
  }

  // -- helpers --

  private SearchRequest.Builder baseRequest(int lower, int upper) {
    return SearchRequest.newBuilder()
        .setTopHits(10)
        .setStartHit(0)
        .setIndexName(DEFAULT_TEST_INDEX)
        .addRetrieveFields("int_score")
        .setQuery(
            Query.newBuilder()
                .setRangeQuery(
                    RangeQuery.newBuilder()
                        .setField("int_field")
                        .setLower(String.valueOf(lower))
                        .setUpper(String.valueOf(upper))
                        .build())
                .build());
  }

  private Script jsScript(String source) {
    return Script.newBuilder().setLang("js").setSource(source).build();
  }

  // -- tests --

  @Test
  public void testScriptRescoreReplacesScore() {
    // Script always returns 42.0 — all rescored hits should have that score.
    SearchRequest request =
        baseRequest(5, 15)
            .addRescorers(
                Rescorer.newBuilder()
                    .setWindowSize(10)
                    .setScriptRescorer(
                        ScriptRescorer.newBuilder().setScript(jsScript("42.0")).build())
                    .build())
            .build();

    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    assertEquals(10, response.getHitsCount());
    for (SearchResponse.Hit hit : response.getHitsList()) {
      assertEquals(42.0f, hit.getScore(), 0.001f);
    }
  }

  @Test
  public void testScriptRescoreUsingGetScore() {
    // Script doubles the previous score (_score is the Lucene expression variable for query score).
    SearchRequest withoutRescore = baseRequest(10, 20).setTopHits(5).build();
    SearchRequest withRescore =
        baseRequest(10, 20)
            .setTopHits(5)
            .addRescorers(
                Rescorer.newBuilder()
                    .setWindowSize(5)
                    .setScriptRescorer(
                        ScriptRescorer.newBuilder().setScript(jsScript("_score * 2.0")).build())
                    .build())
            .build();

    SearchResponse base = getGrpcServer().getBlockingStub().search(withoutRescore);
    SearchResponse rescored = getGrpcServer().getBlockingStub().search(withRescore);

    assertEquals(base.getHitsCount(), rescored.getHitsCount());
    for (int i = 0; i < base.getHitsCount(); i++) {
      assertEquals(base.getHits(i).getScore() * 2.0f, rescored.getHits(i).getScore(), 0.001f);
    }
  }

  @Test
  public void testScriptRescoreUsingDocValue() {
    // Script returns the int_score doc value; hits should be ranked by int_score descending.
    // int_score = 100 - int_field, so lower int_field → higher int_score.
    SearchRequest request =
        baseRequest(50, 60)
            .addRescorers(
                Rescorer.newBuilder()
                    .setWindowSize(10)
                    .setScriptRescorer(
                        ScriptRescorer.newBuilder()
                            .setScript(jsScript("doc['int_score'].value"))
                            .build())
                    .build())
            .build();

    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    assertTrue(response.getHitsCount() > 0);

    // Scores should be descending and equal to int_score field values.
    float prevScore = Float.MAX_VALUE;
    for (SearchResponse.Hit hit : response.getHitsList()) {
      float score = (float) hit.getScore();
      assertTrue(score <= prevScore);
      int intScore = hit.getFieldsOrThrow("int_score").getFieldValue(0).getIntValue();
      assertEquals((float) intScore, score, 0.001f);
      prevScore = score;
    }
  }

  @Test
  public void testScriptRescoreWithParam() {
    // Script multiplies int_score by a param supplied in the request.
    Script script =
        Script.newBuilder()
            .setLang("js")
            .setSource("doc['int_score'].value * multiplier")
            .putParams("multiplier", Script.ParamValue.newBuilder().setDoubleValue(3.0).build())
            .build();

    SearchRequest request =
        baseRequest(50, 60)
            .addRescorers(
                Rescorer.newBuilder()
                    .setWindowSize(10)
                    .setScriptRescorer(ScriptRescorer.newBuilder().setScript(script).build())
                    .build())
            .build();

    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    assertTrue(response.getHitsCount() > 0);

    for (SearchResponse.Hit hit : response.getHitsList()) {
      int intScore = hit.getFieldsOrThrow("int_score").getFieldValue(0).getIntValue();
      assertEquals(intScore * 3.0f, hit.getScore(), 0.01f);
    }
  }

  @Test
  public void testScriptRescoreWindowSizeLimitsRescored() {
    // Window size 3: only top 3 from first pass are rescored and returned.
    SearchRequest request =
        baseRequest(0, 50)
            .addRescorers(
                Rescorer.newBuilder()
                    .setWindowSize(3)
                    .setScriptRescorer(
                        ScriptRescorer.newBuilder().setScript(jsScript("42.0")).build())
                    .build())
            .build();

    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    assertEquals(3, response.getHitsCount());
    for (SearchResponse.Hit hit : response.getHitsList()) {
      assertEquals(42.0f, hit.getScore(), 0.001f);
    }
  }

  @Test
  public void testInvalidScriptThrows() {
    SearchRequest request =
        baseRequest(0, 10)
            .addRescorers(
                Rescorer.newBuilder()
                    .setWindowSize(5)
                    .setScriptRescorer(
                        ScriptRescorer.newBuilder()
                            .setScript(jsScript("@@@not valid js@@@"))
                            .build())
                    .build())
            .build();

    try {
      getGrpcServer().getBlockingStub().search(request);
      fail("Expected StatusRuntimeException for invalid script");
    } catch (StatusRuntimeException e) {
      // expected
    }
  }
}
