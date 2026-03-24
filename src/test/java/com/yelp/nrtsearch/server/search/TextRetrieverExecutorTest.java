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
import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.grpc.TextRetriever;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import io.grpc.testing.GrpcCleanupRule;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.junit.ClassRule;
import org.junit.Test;

public class TextRetrieverExecutorTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      docs.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(i))
                      .build())
              .putFields(
                  "vendor_name",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue("vendor doc " + i)
                      .build())
              .build());
    }
    addDocuments(docs.stream());
  }

  private static Retriever termRetriever(String name, String value, int topHits) {
    return Retriever.newBuilder()
        .setName(name)
        .setTextRetriever(
            TextRetriever.newBuilder()
                .setQuery(
                    Query.newBuilder()
                        .setTermQuery(
                            TermQuery.newBuilder()
                                .setField("vendor_name")
                                .setTextValue(value)
                                .build())
                        .build())
                .setTopHits(topHits)
                .build())
        .build();
  }

  private RetrieverResult runExecutor(Retriever retriever) throws Exception {
    IndexState indexState = getGlobalState().getIndexOrThrow(DEFAULT_TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    SearcherAndTaxonomy s = shardState.acquire();
    try {
      DocLookup docLookup =
          new DocLookup(indexState::getField, () -> indexState.getAllFields().keySet(), s);
      return new TextRetrieverExecutor(
              retriever,
              indexState,
              s.searcher(),
              docLookup,
              SearchRequestProcessor.TOTAL_HITS_THRESHOLD)
          .call();
    } finally {
      shardState.release(s);
    }
  }

  @Test
  public void testTermQuery() throws Exception {
    RetrieverResult result = runExecutor(termRetriever("text_main", "vendor", 10));

    assertEquals("text_main", result.getName());
    assertEquals(5, result.getTopDocs().totalHits.value());
    assertEquals(5, result.getTopDocs().scoreDocs.length);
    assertTrue(result.getTimeTakenMs() >= 0);

    RetrieverResult limitedResult = runExecutor(termRetriever("text_limited", "vendor", 2));

    assertEquals(5, limitedResult.getTopDocs().totalHits.value());
    assertEquals(2, limitedResult.getTopDocs().scoreDocs.length);
  }

  @Test
  public void testBoostScalesScores() throws Exception {
    RetrieverResult baseResult = runExecutor(termRetriever("no_boost", "vendor", 5));
    RetrieverResult boostedResult =
        runExecutor(termRetriever("with_boost", "vendor", 5).toBuilder().setBoost(2.0f).build());

    float baseScore = baseResult.getTopDocs().scoreDocs[0].score;
    float boostedScore = boostedResult.getTopDocs().scoreDocs[0].score;
    assertEquals(baseScore * 2.0f, boostedScore, 1e-4f);
  }

  @Test
  public void testNoMatchingDocuments() throws Exception {
    RetrieverResult result = runExecutor(termRetriever("no_match", "nonexistent", 10));

    assertEquals(0, result.getTopDocs().totalHits.value());
    assertEquals(0, result.getTopDocs().scoreDocs.length);
  }
}
