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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.KnnRetriever;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.MultiRetrieverRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SortFields;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.grpc.TextRetriever;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Test;

public class MultiRetrieverSearchTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/registerFieldsMultiRetriever.json");
  }

  @Override
  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      docs.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id", MultiValuedField.newBuilder().addValue(String.valueOf(i)).build())
              .putFields(
                  "text_field",
                  MultiValuedField.newBuilder().addValue("test document " + i).build())
              .putFields(
                  "vector_field",
                  MultiValuedField.newBuilder()
                      .addValue(String.format("[%f, %f, %f]", i * 0.1f, i * 0.2f, i * 0.3f))
                      .build())
              .build());
    }
    addDocuments(docs.stream());
  }

  private SearchRequest.Builder baseRequest() {
    return SearchRequest.newBuilder()
        .setIndexName(DEFAULT_TEST_INDEX)
        .setStartHit(0)
        .setTopHits(10);
  }

  private Retriever textRetriever(int topHits) {
    return Retriever.newBuilder()
        .setName("text")
        .setTextRetriever(
            TextRetriever.newBuilder()
                .setQuery(
                    Query.newBuilder()
                        .setMatchQuery(
                            MatchQuery.newBuilder().setField("text_field").setQuery("test").build())
                        .build())
                .setTopHits(topHits)
                .build())
        .build();
  }

  private Retriever knnRetriever() {
    return Retriever.newBuilder()
        .setName("knn")
        .setKnnRetriever(
            KnnRetriever.newBuilder()
                .setKnnQuery(
                    KnnQuery.newBuilder()
                        .setField("vector_field")
                        .addAllQueryVector(List.of(0.1f, 0.2f, 0.3f))
                        .setNumCandidates(10)
                        .setK(5)
                        .build())
                .build())
        .build();
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void assertSearchError(SearchRequest request, String expectedMessage) {
    try {
      getGrpcServer().getBlockingStub().search(request);
      fail("Expected error");
    } catch (StatusRuntimeException e) {
      assertTrue(
          "Expected message containing: " + expectedMessage + ", got: " + e.getMessage(),
          e.getMessage().contains(expectedMessage));
    }
  }

  @Test
  public void testEmptyRetrieversList() {
    assertSearchError(
        baseRequest().setMultiRetriever(MultiRetrieverRequest.newBuilder().build()).build(),
        "MultiRetriever request must have at least one retriever");
  }

  @Test
  public void testMultiRetrieverWithQueryParams() {
    MultiRetrieverRequest multiRetriever =
        MultiRetrieverRequest.newBuilder().addRetrievers(textRetriever(5)).build();

    assertSearchError(
        baseRequest()
            .setQuery(
                Query.newBuilder()
                    .setMatchQuery(
                        MatchQuery.newBuilder().setField("text_field").setQuery("test").build())
                    .build())
            .setMultiRetriever(multiRetriever)
            .build(),
        "Query should not be set along with a MultiRetriever request");

    assertSearchError(
        baseRequest().setQueryText("test").setMultiRetriever(multiRetriever).build(),
        "QueryText should not be set along with a MultiRetriever request");

    assertSearchError(
        baseRequest()
            .setQuerySort(
                QuerySortField.newBuilder()
                    .setFields(
                        SortFields.newBuilder()
                            .addSortedFields(SortType.newBuilder().setFieldName("doc_id").build())
                            .build())
                    .build())
            .setMultiRetriever(multiRetriever)
            .build(),
        "QuerySort is not supported with MultiRetriever requests");
  }

  @Test
  public void testMultiRetrieverWithKnnQuery() {
    assertSearchError(
        baseRequest()
            .addKnn(
                KnnQuery.newBuilder()
                    .setField("vector_field")
                    .addAllQueryVector(List.of(0.1f, 0.2f, 0.3f))
                    .setNumCandidates(10)
                    .setK(5)
                    .build())
            .setMultiRetriever(
                MultiRetrieverRequest.newBuilder().addRetrievers(textRetriever(5)).build())
            .build(),
        "Knn Query should not be set along with a MultiRetriever request");
  }

  // TODO: Remove this test after Multi-retriever implementation
  @Test
  public void testMultiRetrieverSearchUnsupported() {
    assertSearchError(
        baseRequest()
            .setMultiRetriever(
                MultiRetrieverRequest.newBuilder()
                    .addRetrievers(textRetriever(5))
                    .addRetrievers(knnRetriever())
                    .build())
            .build(),
        "Multi-retriever is not yet supported");
  }
}
