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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.BoolValue;
import com.google.protobuf.Int32Value;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue;
import com.yelp.nrtsearch.server.index.IndexState;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Fields that are both stored and have doc values are retrieved from doc values, whichever fetch
 * path is used. Subclasses run the same assertions with parallel fetch configurations.
 */
public class StoredDocValuesFetchTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final String TEST_INDEX = "test_index";

  @Override
  public List<String> getIndices() {
    return Collections.singletonList(TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) {
    return getFieldsFromJson(
        String.join(
            "\n",
            "{",
            "  \"indexName\": \"" + name + "\",",
            "  \"field\": [",
            "    {\"name\": \"doc_id\", \"type\": \"ATOM\", \"storeDocValues\": true},",
            "    {\"name\": \"stored_and_doc_values\", \"type\": \"ATOM\", \"multiValued\": true,",
            "     \"store\": true, \"storeDocValues\": true},",
            "    {\"name\": \"stored_only\", \"type\": \"ATOM\", \"multiValued\": true,",
            "     \"store\": true}",
            "  ]",
            "}"));
  }

  @Override
  public void initIndex(String name) throws Exception {
    addDocuments(
        List.of(
            AddDocumentRequest.newBuilder()
                .setIndexName(name)
                .putFields("doc_id", values("1"))
                .putFields("stored_and_doc_values", values("b", "a", "b"))
                .putFields("stored_only", values("y", "x", "y"))
                .build(),
            AddDocumentRequest.newBuilder()
                .setIndexName(name)
                .putFields("doc_id", values("2"))
                .putFields("stored_and_doc_values", values("d", "c"))
                .putFields("stored_only", values("z"))
                .build())
            .stream());
  }

  private static AddDocumentRequest.MultiValuedField values(String... values) {
    return AddDocumentRequest.MultiValuedField.newBuilder().addAllValue(List.of(values)).build();
  }

  private static List<String> textValues(CompositeFieldValue compositeFieldValue) {
    return compositeFieldValue.getFieldValueList().stream().map(FieldValue::getTextValue).toList();
  }

  private Map<String, SearchResponse.Hit> searchHitsByDocId() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setTopHits(10)
                    .addAllRetrieveFields(List.of("doc_id", "stored_and_doc_values", "stored_only"))
                    .setQuery(Query.newBuilder().build())
                    .build());
    assertEquals(2, response.getHitsCount());
    return response.getHitsList().stream()
        .collect(
            Collectors.toMap(
                hit -> hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue(), hit -> hit));
  }

  @Test
  public void testStoredAndDocValuesFieldReturnsDocValues() {
    // sorted set doc values are de-duplicated and sorted; the stored values are b, a, b and d, c
    Map<String, SearchResponse.Hit> hits = searchHitsByDocId();
    assertEquals(
        List.of("a", "b"), textValues(hits.get("1").getFieldsOrThrow("stored_and_doc_values")));
    assertEquals(
        List.of("c", "d"), textValues(hits.get("2").getFieldsOrThrow("stored_and_doc_values")));
  }

  @Test
  public void testStoredOnlyFieldReturnsStoredValues() {
    Map<String, SearchResponse.Hit> hits = searchHitsByDocId();
    assertEquals(List.of("y", "x", "y"), textValues(hits.get("1").getFieldsOrThrow("stored_only")));
    assertEquals(List.of("z"), textValues(hits.get("2").getFieldsOrThrow("stored_only")));
  }

  /** Parallel fetch is configured by index live settings; the fetch pool size sets parallelism. */
  public abstract static class ParallelFetchBase extends StoredDocValuesFetchTest {
    abstract boolean parallelFetchByField();

    @Override
    public String getExtraConfig() {
      return String.join("\n", "threadPoolConfiguration:", "  fetch:", "    maxThreads: 8");
    }

    @Override
    public void initIndex(String name) throws Exception {
      getGrpcServer()
          .getBlockingStub()
          .liveSettingsV2(
              LiveSettingsV2Request.newBuilder()
                  .setIndexName(name)
                  .setLiveSettings(
                      IndexLiveSettings.newBuilder()
                          .setParallelFetchByField(
                              BoolValue.newBuilder().setValue(parallelFetchByField()).build())
                          .setParallelFetchChunkSize(Int32Value.newBuilder().setValue(1).build())
                          .build())
                  .build());
      super.initIndex(name);
    }

    // precondition for the other tests, so they cover the parallel fetch path
    @Test
    public void testParallelFetchConfigured() throws IOException {
      IndexState.ParallelFetchConfig config =
          getGlobalState().getIndexOrThrow(TEST_INDEX).getParallelFetchConfig();
      assertEquals(parallelFetchByField(), config.parallelFetchByField());
      assertEquals(1, config.parallelFetchChunkSize());
      assertEquals(8, config.maxParallelism());
    }
  }

  /** Fetch by document chunks in parallel (FillDocsTask on the fetch executor). */
  public static class ParallelDocsFetchTest extends ParallelFetchBase {
    @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @Override
    boolean parallelFetchByField() {
      return false;
    }
  }

  /** Fetch by field chunks in parallel (FillFieldsTask, which always preferred doc values). */
  public static class ParallelFieldsFetchTest extends ParallelFetchBase {
    @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @Override
    boolean parallelFetchByField() {
      return true;
    }
  }
}
