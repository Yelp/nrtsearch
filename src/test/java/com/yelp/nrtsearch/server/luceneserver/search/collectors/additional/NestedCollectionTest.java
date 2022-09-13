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
package com.yelp.nrtsearch.server.luceneserver.search.collectors.additional;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.BucketResult;
import com.yelp.nrtsearch.server.grpc.BucketResult.Bucket;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.CollectorResult.CollectorResultsCase;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.HitsResult;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.grpc.TopHitsCollector;
import com.yelp.nrtsearch.server.grpc.TotalHits.Relation;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.script.js.JsScriptEngine;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;
import org.junit.Test;

public class NestedCollectionTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;
  private static final List<String> ALL_FIELDS =
      Arrays.asList("doc_id", "int_field", "int_field_2", "int_field_3", "value");

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/collection/nested.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    // create a shuffled list of ids
    List<Integer> idList = new ArrayList<>();
    for (int i = 0; i < NUM_DOCS; ++i) {
      idList.add(i);
    }
    Collections.shuffle(idList);

    // add documents one chunk at a time to ensure multiple index segments
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
                  "int_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id % 5))
                      .build())
              .putFields(
                  "int_field_2",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id % 2))
                      .build())
              .putFields(
                  "int_field_3",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id / 50))
                      .build())
              .putFields(
                  "value",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id + 2))
                      .build())
              .build());

      if (requestChunk.size() == SEGMENT_CHUNK) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }
  }

  @Test
  public void testNestedCollectors() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setQuery(
                Query.newBuilder()
                    .setFunctionScoreQuery(
                        FunctionScoreQuery.newBuilder()
                            .setScript(
                                Script.newBuilder()
                                    .setLang(JsScriptEngine.LANG)
                                    .setSource("value*3")
                                    .build())
                            .build())
                    .build())
            .putCollectors(
                "test_collector",
                Collector.newBuilder()
                    .setTerms(
                        TermsCollector.newBuilder().setField("int_field_2").setSize(2).build())
                    .putNestedCollectors(
                        "nested",
                        Collector.newBuilder()
                            .setTopHitsCollector(
                                TopHitsCollector.newBuilder()
                                    .setStartHit(0)
                                    .setTopHits(5)
                                    .addAllRetrieveFields(ALL_FIELDS)
                                    .build())
                            .build())
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    System.out.println(response);

    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, response.getTotalHits().getRelation());
    assertEquals(0, response.getHitsCount());

    assertEquals(1, response.getCollectorResultsCount());
    CollectorResult collectorResult = response.getCollectorResultsOrThrow("test_collector");
    assertEquals(CollectorResultsCase.BUCKETRESULT, collectorResult.getCollectorResultsCase());
    BucketResult bucketResult = collectorResult.getBucketResult();
    assertEquals(2, bucketResult.getBucketsCount());
    Map<String, Bucket> bucketMap =
        bucketResult.getBucketsList().stream().collect(Collectors.toMap(Bucket::getKey, b -> b));
    assertEquals(Set.of("0", "1"), bucketMap.keySet());

    Bucket bucket = bucketMap.get("0");
    assertEquals(50, bucket.getCount());
    assertEquals(1, bucket.getNestedCollectorResultsCount());
    HitsResult hitsResult = bucket.getNestedCollectorResultsOrThrow("nested").getHitsResult();
    assertEquals(50, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 98, 96, 94, 92, 90);
    assertScores(hitsResult, 300, 294, 288, 282, 276);

    bucket = bucketMap.get("1");
    assertEquals(50, bucket.getCount());
    assertEquals(1, bucket.getNestedCollectorResultsCount());
    hitsResult = bucket.getNestedCollectorResultsOrThrow("nested").getHitsResult();
    assertEquals(50, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 99, 97, 95, 93, 91);
    assertScores(hitsResult, 303, 297, 291, 285, 279);
  }

  @Test
  public void testMultiLevelNestedCollectors() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setQuery(
                Query.newBuilder()
                    .setFunctionScoreQuery(
                        FunctionScoreQuery.newBuilder()
                            .setScript(
                                Script.newBuilder()
                                    .setLang(JsScriptEngine.LANG)
                                    .setSource("value*3")
                                    .build())
                            .build())
                    .build())
            .putCollectors(
                "test_collector",
                Collector.newBuilder()
                    .setTerms(
                        TermsCollector.newBuilder().setField("int_field_2").setSize(2).build())
                    .putNestedCollectors(
                        "nested1",
                        Collector.newBuilder()
                            .setTerms(
                                TermsCollector.newBuilder()
                                    .setSize(5)
                                    .setField("int_field")
                                    .build())
                            .putNestedCollectors(
                                "nested2",
                                Collector.newBuilder()
                                    .setTopHitsCollector(
                                        TopHitsCollector.newBuilder()
                                            .setStartHit(0)
                                            .setTopHits(5)
                                            .addAllRetrieveFields(ALL_FIELDS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);

    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, response.getTotalHits().getRelation());
    assertEquals(0, response.getHitsCount());

    assertEquals(1, response.getCollectorResultsCount());
    CollectorResult collectorResult = response.getCollectorResultsOrThrow("test_collector");
    assertEquals(CollectorResultsCase.BUCKETRESULT, collectorResult.getCollectorResultsCase());
    BucketResult bucketResult = collectorResult.getBucketResult();
    assertEquals(2, bucketResult.getBucketsCount());
    Map<String, Bucket> bucketMap =
        bucketResult.getBucketsList().stream().collect(Collectors.toMap(Bucket::getKey, b -> b));
    assertEquals(Set.of("0", "1"), bucketMap.keySet());

    Bucket bucket = bucketMap.get("0");
    assertEquals(50, bucket.getCount());
    assertEquals(1, bucket.getNestedCollectorResultsCount());
    BucketResult nestedBucketResult =
        bucket.getNestedCollectorResultsOrThrow("nested1").getBucketResult();
    assertEquals(5, nestedBucketResult.getBucketsCount());
    Map<String, Bucket> nestedBucketMap =
        nestedBucketResult.getBucketsList().stream()
            .collect(Collectors.toMap(Bucket::getKey, b -> b));
    assertEquals(Set.of("0", "1", "2", "3", "4"), nestedBucketMap.keySet());

    Bucket nestedBucket = nestedBucketMap.get("0");
    HitsResult hitsResult =
        nestedBucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(10, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 90, 80, 70, 60, 50);
    assertScores(hitsResult, 276, 246, 216, 186, 156);

    nestedBucket = nestedBucketMap.get("1");
    hitsResult = nestedBucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(10, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 96, 86, 76, 66, 56);
    assertScores(hitsResult, 294, 264, 234, 204, 174);

    nestedBucket = nestedBucketMap.get("2");
    hitsResult = nestedBucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(10, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 92, 82, 72, 62, 52);
    assertScores(hitsResult, 282, 252, 222, 192, 162);

    nestedBucket = nestedBucketMap.get("3");
    hitsResult = nestedBucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(10, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 98, 88, 78, 68, 58);
    assertScores(hitsResult, 300, 270, 240, 210, 180);

    nestedBucket = nestedBucketMap.get("4");
    hitsResult = nestedBucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(10, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 94, 84, 74, 64, 54);
    assertScores(hitsResult, 288, 258, 228, 198, 168);

    bucket = bucketMap.get("1");
    assertEquals(50, bucket.getCount());
    assertEquals(1, bucket.getNestedCollectorResultsCount());
    nestedBucketResult = bucket.getNestedCollectorResultsOrThrow("nested1").getBucketResult();
    assertEquals(5, nestedBucketResult.getBucketsCount());
    nestedBucketMap =
        nestedBucketResult.getBucketsList().stream()
            .collect(Collectors.toMap(Bucket::getKey, b -> b));
    assertEquals(Set.of("0", "1", "2", "3", "4"), nestedBucketMap.keySet());

    nestedBucket = nestedBucketMap.get("0");
    hitsResult = nestedBucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(10, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 95, 85, 75, 65, 55);
    assertScores(hitsResult, 291, 261, 231, 201, 171);

    nestedBucket = nestedBucketMap.get("1");
    hitsResult = nestedBucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(10, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 91, 81, 71, 61, 51);
    assertScores(hitsResult, 279, 249, 219, 189, 159);

    nestedBucket = nestedBucketMap.get("2");
    hitsResult = nestedBucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(10, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 97, 87, 77, 67, 57);
    assertScores(hitsResult, 297, 267, 237, 207, 177);

    nestedBucket = nestedBucketMap.get("3");
    hitsResult = nestedBucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(10, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 93, 83, 73, 63, 53);
    assertScores(hitsResult, 285, 255, 225, 195, 165);

    nestedBucket = nestedBucketMap.get("4");
    hitsResult = nestedBucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(10, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 99, 89, 79, 69, 59);
    assertScores(hitsResult, 303, 273, 243, 213, 183);
  }

  @Test
  public void testMultiLevelNestedCollectorsReduceDifferentValueSets() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setQuery(
                Query.newBuilder()
                    .setFunctionScoreQuery(
                        FunctionScoreQuery.newBuilder()
                            .setScript(
                                Script.newBuilder()
                                    .setLang(JsScriptEngine.LANG)
                                    .setSource("value*3")
                                    .build())
                            .build())
                    .build())
            .putCollectors(
                "test_collector",
                Collector.newBuilder()
                    .setTerms(
                        TermsCollector.newBuilder().setField("int_field_2").setSize(2).build())
                    .putNestedCollectors(
                        "nested1",
                        Collector.newBuilder()
                            .setTerms(
                                TermsCollector.newBuilder()
                                    .setSize(2)
                                    .setField("int_field_3")
                                    .build())
                            .putNestedCollectors(
                                "nested2",
                                Collector.newBuilder()
                                    .setTopHitsCollector(
                                        TopHitsCollector.newBuilder()
                                            .setStartHit(0)
                                            .setTopHits(5)
                                            .addAllRetrieveFields(ALL_FIELDS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);

    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, response.getTotalHits().getRelation());
    assertEquals(0, response.getHitsCount());

    assertEquals(1, response.getCollectorResultsCount());
    CollectorResult collectorResult = response.getCollectorResultsOrThrow("test_collector");
    assertEquals(CollectorResultsCase.BUCKETRESULT, collectorResult.getCollectorResultsCase());
    BucketResult bucketResult = collectorResult.getBucketResult();
    assertEquals(2, bucketResult.getBucketsCount());
    Map<String, Bucket> bucketMap =
        bucketResult.getBucketsList().stream().collect(Collectors.toMap(Bucket::getKey, b -> b));
    assertEquals(Set.of("0", "1"), bucketMap.keySet());

    Bucket bucket = bucketMap.get("0");
    assertEquals(50, bucket.getCount());
    assertEquals(1, bucket.getNestedCollectorResultsCount());
    BucketResult nestedBucketResult =
        bucket.getNestedCollectorResultsOrThrow("nested1").getBucketResult();
    assertEquals(2, nestedBucketResult.getBucketsCount());
    Map<String, Bucket> nestedBucketMap =
        nestedBucketResult.getBucketsList().stream()
            .collect(Collectors.toMap(Bucket::getKey, b -> b));
    assertEquals(Set.of("0", "1"), nestedBucketMap.keySet());

    Bucket nestedBucket = nestedBucketMap.get("0");
    HitsResult hitsResult =
        nestedBucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(25, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 48, 46, 44, 42, 40);
    assertScores(hitsResult, 150, 144, 138, 132, 126);

    nestedBucket = nestedBucketMap.get("1");
    hitsResult = nestedBucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(25, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 98, 96, 94, 92, 90);
    assertScores(hitsResult, 300, 294, 288, 282, 276);

    bucket = bucketMap.get("1");
    assertEquals(50, bucket.getCount());
    assertEquals(1, bucket.getNestedCollectorResultsCount());
    nestedBucketResult = bucket.getNestedCollectorResultsOrThrow("nested1").getBucketResult();
    assertEquals(2, nestedBucketResult.getBucketsCount());
    nestedBucketMap =
        nestedBucketResult.getBucketsList().stream()
            .collect(Collectors.toMap(Bucket::getKey, b -> b));
    assertEquals(Set.of("0", "1"), nestedBucketMap.keySet());

    nestedBucket = nestedBucketMap.get("0");
    hitsResult = nestedBucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(25, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 49, 47, 45, 43, 41);
    assertScores(hitsResult, 153, 147, 141, 135, 129);

    nestedBucket = nestedBucketMap.get("1");
    hitsResult = nestedBucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(25, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 99, 97, 95, 93, 91);
    assertScores(hitsResult, 303, 297, 291, 285, 279);
  }

  @Test
  public void testNestedMultipleCollectors() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setQuery(
                Query.newBuilder()
                    .setFunctionScoreQuery(
                        FunctionScoreQuery.newBuilder()
                            .setScript(
                                Script.newBuilder()
                                    .setLang(JsScriptEngine.LANG)
                                    .setSource("value*3")
                                    .build())
                            .build())
                    .build())
            .putCollectors(
                "test_collector",
                Collector.newBuilder()
                    .setTerms(
                        TermsCollector.newBuilder().setField("int_field_2").setSize(2).build())
                    .putNestedCollectors(
                        "nested1",
                        Collector.newBuilder()
                            .setTopHitsCollector(
                                TopHitsCollector.newBuilder()
                                    .setStartHit(0)
                                    .setTopHits(5)
                                    .addAllRetrieveFields(ALL_FIELDS)
                                    .build())
                            .build())
                    .putNestedCollectors(
                        "nested2",
                        Collector.newBuilder()
                            .setTopHitsCollector(
                                TopHitsCollector.newBuilder()
                                    .setStartHit(2)
                                    .setTopHits(5)
                                    .addAllRetrieveFields(ALL_FIELDS)
                                    .build())
                            .build())
                    .build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    System.out.println(response);

    assertEquals(100, response.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, response.getTotalHits().getRelation());
    assertEquals(0, response.getHitsCount());

    assertEquals(1, response.getCollectorResultsCount());
    CollectorResult collectorResult = response.getCollectorResultsOrThrow("test_collector");
    assertEquals(CollectorResultsCase.BUCKETRESULT, collectorResult.getCollectorResultsCase());
    BucketResult bucketResult = collectorResult.getBucketResult();
    assertEquals(2, bucketResult.getBucketsCount());
    Map<String, Bucket> bucketMap =
        bucketResult.getBucketsList().stream().collect(Collectors.toMap(Bucket::getKey, b -> b));
    assertEquals(Set.of("0", "1"), bucketMap.keySet());

    Bucket bucket = bucketMap.get("0");
    assertEquals(50, bucket.getCount());
    assertEquals(2, bucket.getNestedCollectorResultsCount());
    HitsResult hitsResult = bucket.getNestedCollectorResultsOrThrow("nested1").getHitsResult();
    assertEquals(50, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 98, 96, 94, 92, 90);
    assertScores(hitsResult, 300, 294, 288, 282, 276);
    hitsResult = bucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(50, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(3, hitsResult.getHitsCount());
    assertHits(hitsResult, 94, 92, 90);
    assertScores(hitsResult, 288, 282, 276);

    bucket = bucketMap.get("1");
    assertEquals(50, bucket.getCount());
    assertEquals(2, bucket.getNestedCollectorResultsCount());
    hitsResult = bucket.getNestedCollectorResultsOrThrow("nested1").getHitsResult();
    assertEquals(50, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(5, hitsResult.getHitsCount());
    assertHits(hitsResult, 99, 97, 95, 93, 91);
    assertScores(hitsResult, 303, 297, 291, 285, 279);
    hitsResult = bucket.getNestedCollectorResultsOrThrow("nested2").getHitsResult();
    assertEquals(50, hitsResult.getTotalHits().getValue());
    assertEquals(Relation.EQUAL_TO, hitsResult.getTotalHits().getRelation());
    assertEquals(3, hitsResult.getHitsCount());
    assertHits(hitsResult, 95, 93, 91);
    assertScores(hitsResult, 291, 285, 279);
  }

  private void assertHits(HitsResult hitsResult, int... ids) {
    assertEquals(hitsResult.getHitsCount(), ids.length);
    for (int i = 0; i < ids.length; ++i) {
      Hit hit = hitsResult.getHits(i);
      assertEquals(
          String.valueOf(ids[i]), hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
      assertEquals(ids[i] % 5, hit.getFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
      assertEquals(ids[i] % 2, hit.getFieldsOrThrow("int_field_2").getFieldValue(0).getIntValue());
      assertEquals(ids[i] / 50, hit.getFieldsOrThrow("int_field_3").getFieldValue(0).getIntValue());
      assertEquals(ids[i] + 2, hit.getFieldsOrThrow("value").getFieldValue(0).getIntValue());
    }
  }

  private void assertScores(HitsResult hitsResult, double... scores) {
    assertEquals(hitsResult.getHitsCount(), scores.length);
    for (int i = 0; i < scores.length; ++i) {
      Hit hit = hitsResult.getHits(i);
      assertEquals(scores[i], hit.getScore(), 0);
    }
  }
}
