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
package com.yelp.nrtsearch.server.luceneserver.search.collectors.additional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.BucketResult;
import com.yelp.nrtsearch.server.grpc.BucketResult.Bucket;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.HitsResult;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.grpc.TopHitsCollector;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;

public abstract class TermsCollectorManagerTestsBase extends ServerTestCase {
  static final int NUM_DOCS = 100;
  static final int SEGMENT_CHUNK = 10;
  static final String VALUE_FIELD = "value";
  static final String VALUE_MULTI_FIELD = "value_multi";

  protected abstract AddDocumentRequest getIndexRequest(String index, int id);

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    // add documents one chunk at a time to ensure multiple index segments
    List<AddDocumentRequest> requestChunk = new ArrayList<>();
    for (int id = 0; id < NUM_DOCS; ++id) {
      requestChunk.add(getIndexRequest(name, id));

      if (requestChunk.size() == SEGMENT_CHUNK) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }
  }

  static class ExpectedValues {
    public Set<String> labels;
    public double count;

    public ExpectedValues(Set<String> labels, double count) {
      this.labels = labels;
      this.count = count;
    }
  }

  void assertResponse(
      SearchResponse response,
      int totalBuckets,
      int bucketCount,
      int otherCount,
      ExpectedValues... expectedValues) {
    assertEquals(1, response.getCollectorResultsCount());
    BucketResult result = response.getCollectorResultsOrThrow("test_collector").getBucketResult();
    assertEquals(totalBuckets, result.getTotalBuckets());
    assertEquals(bucketCount, result.getBucketsCount());
    assertEquals(otherCount, result.getTotalOtherCounts());

    int sum = 0;
    for (ExpectedValues v : expectedValues) {
      sum += v.labels.size();
    }
    assertEquals(sum, bucketCount);

    int valuesIndex = 0;
    for (ExpectedValues v : expectedValues) {
      Set<String> valueSet = new HashSet<>();
      for (int i = 0; i < v.labels.size(); ++i) {
        valueSet.add(result.getBuckets(valuesIndex).getKey());
        assertEquals(v.count, result.getBuckets(valuesIndex).getCount(), 0);
        valuesIndex++;
      }
      assertEquals(v.labels, valueSet);
    }
  }

  SearchResponse doQuery(TermsCollector terms) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .putCollectors("test_collector", Collector.newBuilder().setTerms(terms).build())
                .build());
  }

  SearchResponse doRangeQuery(TermsCollector terms) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .setQuery(
                    Query.newBuilder()
                        .setRangeQuery(
                            RangeQuery.newBuilder()
                                .setField("int_field")
                                .setLower("41")
                                .setUpper("50")
                                .build())
                        .build())
                .putCollectors("test_collector", Collector.newBuilder().setTerms(terms).build())
                .build());
  }

  SearchResponse doNestedQuery(TermsCollector terms) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .putCollectors(
                    "test_collector",
                    Collector.newBuilder()
                        .setTerms(terms)
                        .putNestedCollectors(
                            "nested",
                            Collector.newBuilder()
                                .setTopHitsCollector(
                                    TopHitsCollector.newBuilder()
                                        .setStartHit(0)
                                        .setTopHits(5)
                                        .addRetrieveFields("doc_id")
                                        .build())
                                .build())
                        .build())
                .build());
  }

  void assertNestedResult(SearchResponse response) {
    assertEquals(1, response.getCollectorResultsCount());
    BucketResult result = response.getCollectorResultsOrThrow("test_collector").getBucketResult();
    assertEquals(3, result.getTotalBuckets());
    assertEquals(3, result.getBucketsCount());

    for (Bucket bucket : result.getBucketsList()) {
      assertEquals(1, bucket.getNestedCollectorResultsCount());
      HitsResult hitsResult = bucket.getNestedCollectorResultsOrThrow("nested").getHitsResult();
      assertTrue(hitsResult.getTotalHits().getValue() > 0);
      assertTrue(hitsResult.getHitsCount() > 0);
      for (Hit hit : hitsResult.getHitsList()) {
        assertTrue(hit.containsFields("doc_id"));
      }
    }
  }
}
