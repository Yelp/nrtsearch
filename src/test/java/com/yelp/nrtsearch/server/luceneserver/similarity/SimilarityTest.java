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
package com.yelp.nrtsearch.server.luceneserver.similarity;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.SimilarityPlugin;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.junit.ClassRule;
import org.junit.Test;

public class SimilarityTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  public static class TestSimilarityPlugin extends Plugin implements SimilarityPlugin {
    @Override
    public Map<String, SimilarityProvider<? extends Similarity>> getSimilarities() {
      Map<String, SimilarityProvider<? extends Similarity>> similarityProviderMap = new HashMap<>();
      similarityProviderMap.put("plugin_similarity", params -> new CustomSimilarity());
      return similarityProviderMap;
    }

    public static class CustomSimilarity extends Similarity {

      @Override
      public long computeNorm(FieldInvertState state) {
        return 1;
      }

      @Override
      public SimScorer scorer(
          float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
        return new SimScorer() {
          @Override
          public float score(float freq, long norm) {
            return 11.11F;
          }
        };
      }
    }
  }

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/similarity/registerFieldsSimilarity.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    MultiValuedField firstValue =
        MultiValuedField.newBuilder().addValue("first vendor").addValue("first again").build();
    MultiValuedField secondValue =
        MultiValuedField.newBuilder().addValue("second vendor").addValue("second again").build();
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields("vendor_name", firstValue)
            .putFields("vendor_name_classic", firstValue)
            .putFields("vendor_name_custom", firstValue)
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields("vendor_name", secondValue)
            .putFields("vendor_name_classic", secondValue)
            .putFields("vendor_name_custom", secondValue)
            .build();
    docs.add(request);
    addDocuments(docs.stream());
  }

  @Override
  protected List<Plugin> getPlugins(LuceneServerConfiguration configuration) {
    return Collections.singletonList(new TestSimilarityPlugin());
  }

  @Test
  public void testDefaultSimilarity() {
    SearchResponse response = doSearch("vendor_name");
    assertEquals(2, response.getHitsCount());
    assertEquals(12.12609, response.getHits(0).getScore(), 0.0001);
    assertEquals(11.692873, response.getHits(1).getScore(), 0.0001);
  }

  @Test
  public void testClassicSimilarity() {
    SearchResponse response = doSearch("vendor_name_classic");
    assertEquals(2, response.getHitsCount());
    assertEquals(12.686687, response.getHits(0).getScore(), 0.0001);
    assertEquals(11.692873, response.getHits(1).getScore(), 0.0001);
  }

  @Test
  public void testCustomSimilarity() {
    SearchResponse response = doSearch("vendor_name_custom");
    assertEquals(2, response.getHitsCount());
    assertEquals(22.802873, response.getHits(0).getScore(), 0.0001);
    assertEquals(11.692873, response.getHits(1).getScore(), 0.0001);
  }

  private SearchResponse doSearch(String fieldName) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .setQueryText(fieldName + ":first vendor")
                .build());
  }
}
