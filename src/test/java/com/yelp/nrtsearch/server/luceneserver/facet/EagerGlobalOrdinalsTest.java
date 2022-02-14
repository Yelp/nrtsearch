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
package com.yelp.nrtsearch.server.luceneserver.facet;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.RefreshRequest;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.IndexReader;
import org.junit.ClassRule;
import org.junit.Test;

public class EagerGlobalOrdinalsTest extends ServerTestCase {
  private static final String NOT_EAGER_FIELD = "sorted_doc_values_facet_field";
  private static final String EAGER_FIELD = "eager_sorted_doc_values_facet_field";

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/facet/eager_global_ordinals.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                NOT_EAGER_FIELD,
                MultiValuedField.newBuilder().addValue("v1").addValue("v2").build())
            .putFields(EAGER_FIELD, MultiValuedField.newBuilder().addValue("v3").build())
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(NOT_EAGER_FIELD, MultiValuedField.newBuilder().addValue("v1").build())
            .putFields(
                EAGER_FIELD, MultiValuedField.newBuilder().addValue("v3").addValue("v4").build())
            .build();
    docs.add(request);
    addDocuments(docs.stream());
  }

  @Test
  public void testEagerOrdinals() throws Exception {
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    ShardState shardState = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0);
    try {
      FieldDef fieldDef = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getField(EAGER_FIELD);
      addDocAndRefresh();
      s = shardState.acquire();
      assertGlobalOrdinals(s.searcher.getIndexReader(), fieldDef);
      doQuery(EAGER_FIELD);
      assertGlobalOrdinals(s.searcher.getIndexReader(), fieldDef);

      // create new reader version
      addDocAndRefresh();
      shardState.release(s);
      s = null;
      s = shardState.acquire();
      assertGlobalOrdinals(s.searcher.getIndexReader(), fieldDef);
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  @Test
  public void testWithoutEagerOrdinals() throws Exception {
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    ShardState shardState = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0);
    try {
      FieldDef fieldDef = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getField(NOT_EAGER_FIELD);
      addDocAndRefresh();
      s = shardState.acquire();
      assertNoGlobalOrdinals(s.searcher.getIndexReader(), fieldDef);
      doQuery(NOT_EAGER_FIELD);
      assertGlobalOrdinals(s.searcher.getIndexReader(), fieldDef);

      // create new reader version
      addDocAndRefresh();
      shardState.release(s);
      s = null;
      s = shardState.acquire();
      assertNoGlobalOrdinals(s.searcher.getIndexReader(), fieldDef);
      doQuery(NOT_EAGER_FIELD);
      assertGlobalOrdinals(s.searcher.getIndexReader(), fieldDef);
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  private void assertNoGlobalOrdinals(IndexReader reader, FieldDef fieldDef) throws IOException {
    Map<String, SortedSetDocValuesReaderState> readerSSDVStates =
        getGlobalState()
            .getIndex(DEFAULT_TEST_INDEX)
            .getShard(0)
            .ssdvStates
            .get(reader.getReaderCacheHelper().getKey());

    if (readerSSDVStates != null) {
      FacetsConfig.DimConfig dimConfig =
          getGlobalState()
              .getIndex(DEFAULT_TEST_INDEX)
              .getFacetsConfig()
              .getDimConfig(fieldDef.getName());
      SortedSetDocValuesReaderState ssdvState = readerSSDVStates.get(dimConfig.indexFieldName);
      assertNull(ssdvState);
    }
  }

  private void assertGlobalOrdinals(IndexReader reader, FieldDef fieldDef) throws IOException {
    Map<String, SortedSetDocValuesReaderState> readerSSDVStates =
        getGlobalState()
            .getIndex(DEFAULT_TEST_INDEX)
            .getShard(0)
            .ssdvStates
            .get(reader.getReaderCacheHelper().getKey());
    assertNotNull(readerSSDVStates);

    FacetsConfig.DimConfig dimConfig =
        getGlobalState()
            .getIndex(DEFAULT_TEST_INDEX)
            .getFacetsConfig()
            .getDimConfig(fieldDef.getName());
    SortedSetDocValuesReaderState ssdvState = readerSSDVStates.get(dimConfig.indexFieldName);
    assertNotNull(ssdvState);
  }

  private void doQuery(String field) {
    getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields(field)
                .addFacets(
                    Facet.newBuilder().setName("test_facet").setDim(field).setTopN(10).build())
                .build());
  }

  private void addDocAndRefresh() throws Exception {
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .putFields(
                NOT_EAGER_FIELD,
                MultiValuedField.newBuilder().addValue("v1").addValue("v2").build())
            .putFields(EAGER_FIELD, MultiValuedField.newBuilder().addValue("v3").build())
            .build();
    addDocuments(Stream.of(request));
    getGrpcServer()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(DEFAULT_TEST_INDEX).build());
  }
}
