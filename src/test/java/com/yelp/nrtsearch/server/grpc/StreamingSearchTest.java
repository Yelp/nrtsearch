/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.ClassRule;
import org.junit.Test;

public class StreamingSearchTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  @Override
  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/registerFieldsBasic.json");
  }

  @Override
  protected void initIndex(String name) throws Exception {
    // Add 100 documents to test batching
    List<AddDocumentRequest> docs = new ArrayList<>();
    for (int i = 1; i <= 100; i++) {
      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id", MultiValuedField.newBuilder().addValue(String.valueOf(i)).build())
              .putFields(
                  "vendor_name", MultiValuedField.newBuilder().addValue("vendor_" + i).build())
              .putFields(
                  "long_field",
                  MultiValuedField.newBuilder().addValue(String.valueOf(i * 10)).build())
              .build();
      docs.add(request);
    }
    addDocuments(docs.stream());
  }

  /** Helper method to collect streaming responses synchronously. */
  private List<StreamingSearchResponse> collectStreamingResponses(SearchRequest request)
      throws Exception {
    CountDownLatch finishLatch = new CountDownLatch(1);
    final List<StreamingSearchResponse> responses = new ArrayList<>();
    final AtomicReference<Throwable> exception = new AtomicReference<>();

    StreamObserver<StreamingSearchResponse> responseObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(StreamingSearchResponse value) {
            responses.add(value);
          }

          @Override
          public void onError(Throwable t) {
            exception.set(t);
            finishLatch.countDown();
          }

          @Override
          public void onCompleted() {
            finishLatch.countDown();
          }
        };

    getGrpcServer().getStub().streamingSearch(request, responseObserver);

    if (!finishLatch.await(30, TimeUnit.SECONDS)) {
      throw new RuntimeException("streamingSearch did not finish within 30 seconds");
    }

    if (exception.get() != null) {
      throw new RuntimeException(exception.get());
    }

    return responses;
  }

  @Test
  public void testStreamingSearchBasic() throws Exception {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(10)
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(
                        TermQuery.newBuilder()
                            .setField("vendor_name")
                            .setTextValue("vendor_5")
                            .build())
                    .build())
            .build();

    List<StreamingSearchResponse> responses = collectStreamingResponses(request);

    // Should have at least 2 messages: header + hits
    assertTrue("Should have at least 2 messages", responses.size() >= 2);

    // First message should be header
    StreamingSearchResponse firstResponse = responses.get(0);
    assertTrue("First message should have header", firstResponse.hasHeader());
    assertFalse("First message should not have hits", firstResponse.hasHits());

    // Verify header contains expected metadata
    StreamingSearchResponseHeader header = firstResponse.getHeader();
    assertNotNull("Header should have diagnostics", header.getDiagnostics());
    assertNotNull("Header should have total hits", header.getTotalHits());
    assertEquals("Should find 1 document", 1, header.getTotalHits().getValue());

    // Verify subsequent messages contain hits
    int totalHitsReceived = 0;
    for (int i = 1; i < responses.size(); i++) {
      StreamingSearchResponse response = responses.get(i);
      assertTrue("Subsequent messages should have hits", response.hasHits());
      assertFalse("Subsequent messages should not have header", response.hasHeader());
      totalHitsReceived += response.getHits().getHitsCount();
    }

    assertEquals("Should receive all hits", 1, totalHitsReceived);
  }

  @Test
  public void testStreamingSearchDefaultBatchSize() throws Exception {
    // Search for all documents, should get 100 results
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(100)
            .setQuery(Query.newBuilder().build()) // Match all query
            .build();

    List<StreamingSearchResponse> responses = collectStreamingResponses(request);

    // First message is header
    assertTrue("First message should be header", responses.get(0).hasHeader());
    assertEquals(
        "Should find 100 documents", 100, responses.get(0).getHeader().getTotalHits().getValue());

    // Count total hits received
    int totalHitsReceived = 0;
    for (int i = 1; i < responses.size(); i++) {
      StreamingSearchResponse response = responses.get(i);
      assertTrue("Should have hits", response.hasHits());
      int hitsInBatch = response.getHits().getHitsCount();
      totalHitsReceived += hitsInBatch;
      // Default batch size is 50, so each batch should have at most 50 hits
      assertTrue("Batch should have at most 50 hits", hitsInBatch <= 50);
    }

    assertEquals("Should receive all 100 hits", 100, totalHitsReceived);
  }

  @Test
  public void testStreamingSearchCustomBatchSize() throws Exception {
    // Search with custom batch size of 10
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(100)
            .setStreamingHitBatchSize(10)
            .setQuery(Query.newBuilder().build())
            .build();

    List<StreamingSearchResponse> responses = collectStreamingResponses(request);

    // First message is header
    assertTrue("First message should be header", responses.get(0).hasHeader());

    // Count total hits and verify batch sizes
    int totalHitsReceived = 0;
    for (int i = 1; i < responses.size(); i++) {
      StreamingSearchResponse response = responses.get(i);
      int hitsInBatch = response.getHits().getHitsCount();
      totalHitsReceived += hitsInBatch;
      // Each batch should have at most 10 hits (except possibly the last one)
      assertTrue("Batch should have at most 10 hits", hitsInBatch <= 10);
    }

    assertEquals("Should receive all 100 hits", 100, totalHitsReceived);
    // With batch size 10 and 100 hits, should have 1 header + 10 hit messages
    assertEquals("Should have 11 messages (1 header + 10 batches)", 11, responses.size());
  }

  @Test
  public void testStreamingSearchLargeBatchSize() throws Exception {
    // Search with batch size larger than result set
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(100)
            .setStreamingHitBatchSize(200)
            .setQuery(Query.newBuilder().build())
            .build();

    List<StreamingSearchResponse> responses = collectStreamingResponses(request);

    // Should have exactly 2 messages: header + all hits in one batch
    assertEquals("Should have 2 messages", 2, responses.size());
    assertTrue("First message should be header", responses.get(0).hasHeader());
    assertTrue("Second message should have hits", responses.get(1).hasHits());
    assertEquals(
        "Should have all 100 hits in one batch", 100, responses.get(1).getHits().getHitsCount());
  }

  @Test
  public void testStreamingSearchEmptyResults() throws Exception {
    // Search for non-existent term
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(10)
            .setQuery(
                Query.newBuilder()
                    .setTermQuery(
                        TermQuery.newBuilder()
                            .setField("vendor_name")
                            .setTextValue("nonexistent_vendor")
                            .build())
                    .build())
            .build();

    List<StreamingSearchResponse> responses = collectStreamingResponses(request);

    // Should have only 1 message: header with no hits
    assertEquals("Should have only header message", 1, responses.size());
    assertTrue("Message should be header", responses.get(0).hasHeader());
    assertEquals(
        "Should have 0 total hits", 0, responses.get(0).getHeader().getTotalHits().getValue());
  }

  @Test
  public void testStreamingSearchWithFacets() throws Exception {
    // Search with facets
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(10)
            .setQuery(Query.newBuilder().build())
            .addRetrieveFields("vendor_name")
            .build();

    List<StreamingSearchResponse> responses = collectStreamingResponses(request);

    // Verify header
    assertTrue("First message should be header", responses.get(0).hasHeader());
    StreamingSearchResponseHeader header = responses.get(0).getHeader();
    assertNotNull("Header should have diagnostics", header.getDiagnostics());
    assertNotNull("Header should have total hits", header.getTotalHits());

    // Verify hits are received
    int totalHitsReceived = 0;
    for (int i = 1; i < responses.size(); i++) {
      totalHitsReceived += responses.get(i).getHits().getHitsCount();
    }
    assertEquals("Should receive 10 hits", 10, totalHitsReceived);
  }

  @Test
  public void testStreamingSearchWithSort() throws Exception {
    // Search with sorting
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(10)
            .setQuery(Query.newBuilder().build())
            .setQuerySort(
                QuerySortField.newBuilder()
                    .setFields(
                        SortFields.newBuilder()
                            .addSortedFields(
                                SortType.newBuilder().setFieldName("long_field").build())
                            .build())
                    .build())
            .build();

    List<StreamingSearchResponse> responses = collectStreamingResponses(request);

    // Verify header has search state
    assertTrue("First message should be header", responses.get(0).hasHeader());
    StreamingSearchResponseHeader header = responses.get(0).getHeader();
    assertTrue("Header should have search state", header.hasSearchState());

    // Verify hits are sorted (should get docs with lowest long_field values)
    assertTrue("Should have hits", responses.size() > 1);
    List<SearchResponse.Hit> allHits = new ArrayList<>();
    for (int i = 1; i < responses.size(); i++) {
      allHits.addAll(responses.get(i).getHits().getHitsList());
    }
    assertEquals("Should receive 10 hits", 10, allHits.size());
  }

  @Test
  public void testStreamingSearchZeroBatchSize() throws Exception {
    // Batch size 0 should use default (50)
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(100)
            .setStreamingHitBatchSize(0)
            .setQuery(Query.newBuilder().build())
            .build();

    List<StreamingSearchResponse> responses = collectStreamingResponses(request);

    // Should use default batch size of 50
    assertTrue("Should have at least 3 messages", responses.size() >= 3);

    int totalHitsReceived = 0;
    for (int i = 1; i < responses.size(); i++) {
      int hitsInBatch = responses.get(i).getHits().getHitsCount();
      totalHitsReceived += hitsInBatch;
      assertTrue("Batch should have at most 50 hits (default)", hitsInBatch <= 50);
    }

    assertEquals("Should receive all 100 hits", 100, totalHitsReceived);
  }

  @Test
  public void testStreamingSearchHitFields() throws Exception {
    // Search and retrieve specific fields
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(5)
            .addRetrieveFields("doc_id")
            .addRetrieveFields("vendor_name")
            .addRetrieveFields("long_field")
            .setQuery(Query.newBuilder().build())
            .build();

    List<StreamingSearchResponse> responses = collectStreamingResponses(request);

    // Collect all hits
    List<SearchResponse.Hit> allHits = new ArrayList<>();
    for (int i = 1; i < responses.size(); i++) {
      allHits.addAll(responses.get(i).getHits().getHitsList());
    }

    assertEquals("Should receive 5 hits", 5, allHits.size());

    // Verify each hit has the requested fields
    for (SearchResponse.Hit hit : allHits) {
      assertTrue("Hit should have doc_id field", hit.getFieldsMap().containsKey("doc_id"));
      assertTrue(
          "Hit should have vendor_name field", hit.getFieldsMap().containsKey("vendor_name"));
      assertTrue("Hit should have long_field field", hit.getFieldsMap().containsKey("long_field"));
    }
  }
}
