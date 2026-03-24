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
package com.yelp.nrtsearch.server.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.KnnRetriever;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.search.collectors.CollectorCreatorContext;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.search.Query;
import org.junit.ClassRule;
import org.junit.Test;

public class KnnRetrieverExecutorTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final String INDEX_NAME = "knn_retriever_test_index";
  private static final int NUM_DOCS = 20;
  private static final int VECTOR_DIM = 3;

  @Override
  protected List<String> getIndices() {
    return Collections.singletonList(INDEX_NAME);
  }

  @Override
  protected FieldDefRequest getIndexDef(String name) throws IOException {
    FieldDefRequest base = getFieldsFromResourceFile("/field/registerFieldsVectorSearch.json");
    return base.toBuilder().setIndexName(name).build();
  }

  @Override
  protected void initIndex(String name) throws Exception {
    Random random = new Random(42);
    List<AddDocumentRequest> docs = new ArrayList<>();
    for (int i = 0; i < NUM_DOCS; i++) {
      docs.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "vector_l2_norm",
                  MultiValuedField.newBuilder()
                      .addValue(randomVectorString(random, VECTOR_DIM))
                      .build())
              .build());
    }
    addDocuments(docs.stream());
  }

  private static String randomVectorString(Random random, int dim) {
    List<String> vals = new ArrayList<>();
    for (int i = 0; i < dim; i++) {
      vals.add(String.valueOf(random.nextFloat()));
    }
    return "[" + String.join(", ", vals) + "]";
  }

  private static Retriever knnRetriever(String name, int k) {
    return Retriever.newBuilder()
        .setName(name)
        .setKnnRetriever(
            KnnRetriever.newBuilder()
                .setKnnQuery(
                    KnnQuery.newBuilder()
                        .setField("vector_l2_norm")
                        .addQueryVector(0.5f)
                        .addQueryVector(0.5f)
                        .addQueryVector(0.5f)
                        .setK(k)
                        .setNumCandidates(10)
                        .build())
                .build())
        .build();
  }

  private SearcherResult runExecutor(
      Retriever retriever,
      SearchResponse.Diagnostics.VectorDiagnostics.Builder vectorDiagnosticsBuilder)
      throws Exception {
    IndexState indexState = getGlobalState().getIndexOrThrow(INDEX_NAME);
    ShardState shardState = indexState.getShard(0);
    SearcherAndTaxonomy s = shardState.acquire();
    try {
      KnnQuery knnQuery = retriever.getKnnRetriever().getKnnQuery();
      Query luceneKnnQuery = KnnUtils.buildKnnQuery(knnQuery, indexState);
      CollectorCreatorContext collectorCreatorContext =
          CollectorCreatorContext.newBuilder(indexState)
              .withShardState(shardState)
              .withSearcherAndTaxonomy(s)
              .withNumHitsToCollect(knnQuery.getK())
              .withTotalHitsThreshold(SearchRequestProcessor.TOTAL_HITS_THRESHOLD)
              .withQueryFields(indexState.getAllFields())
              .build();
      return new KnnRetrieverExecutor(
              retriever, luceneKnnQuery, collectorCreatorContext, vectorDiagnosticsBuilder)
          .call();
    } finally {
      shardState.release(s);
    }
  }

  @Test
  public void testKnnQuery() throws Exception {
    SearchResponse.Diagnostics.VectorDiagnostics.Builder diag =
        SearchResponse.Diagnostics.VectorDiagnostics.newBuilder();
    SearcherResult result = runExecutor(knnRetriever("knn_main", 5), diag);

    assertEquals(5, result.getTopDocs().scoreDocs.length);

    SearcherResult limitedResult =
        runExecutor(
            knnRetriever("knn_limited", 3),
            SearchResponse.Diagnostics.VectorDiagnostics.newBuilder());
    assertEquals(3, limitedResult.getTopDocs().scoreDocs.length);
  }

  @Test
  public void testBoostScalesScores() throws Exception {
    SearchResponse.Diagnostics.VectorDiagnostics.Builder diag =
        SearchResponse.Diagnostics.VectorDiagnostics.newBuilder();
    SearcherResult baseResult = runExecutor(knnRetriever("no_boost", 5), diag);
    SearcherResult boostedResult =
        runExecutor(
            knnRetriever("with_boost", 5).toBuilder().setBoost(2.0f).build(),
            SearchResponse.Diagnostics.VectorDiagnostics.newBuilder());

    float baseScore = baseResult.getTopDocs().scoreDocs[0].score;
    float boostedScore = boostedResult.getTopDocs().scoreDocs[0].score;
    assertEquals(baseScore * 2.0f, boostedScore, 1e-4f);
  }

  @Test
  public void testVectorDiagnosticsPopulated() throws Exception {
    SearchResponse.Diagnostics.VectorDiagnostics.Builder diag =
        SearchResponse.Diagnostics.VectorDiagnostics.newBuilder();
    runExecutor(knnRetriever("knn_diag", 5), diag);

    assertTrue(diag.getSearchTimeMs() >= 0);
  }
}
