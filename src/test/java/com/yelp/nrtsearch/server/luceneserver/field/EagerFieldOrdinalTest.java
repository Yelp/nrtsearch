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
package com.yelp.nrtsearch.server.luceneserver.field;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.RefreshRequest;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.search.GlobalOrdinalLookup;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.IndexReader;
import org.junit.ClassRule;
import org.junit.Test;

public class EagerFieldOrdinalTest extends ServerTestCase {
  private static final String NOT_EAGER_TEXT_FIELD = "text_field";
  private static final String EAGER_TEXT_FIELD = "eager_text_field";
  private static final String NOT_EAGER_ATOM_FIELD = "atom_field";
  private static final String EAGER_ATOM_FIELD = "eager_atom_field";
  private static final String NOT_EAGER_TEXT_FIELD_MULTI = "text_field_multi";
  private static final String EAGER_TEXT_FIELD_MULTI = "eager_text_field_multi";
  private static final String NOT_EAGER_ATOM_FIELD_MULTI = "atom_field_multi";
  private static final String EAGER_ATOM_FIELD_MULTI = "eager_atom_field_multi";

  private static final List<String> NOT_EAGER_FIELDS =
      Arrays.asList(
          NOT_EAGER_TEXT_FIELD,
          NOT_EAGER_ATOM_FIELD,
          NOT_EAGER_TEXT_FIELD_MULTI,
          NOT_EAGER_ATOM_FIELD_MULTI);

  private static final List<String> EAGER_FIELDS =
      Arrays.asList(
          EAGER_TEXT_FIELD, EAGER_ATOM_FIELD, EAGER_TEXT_FIELD_MULTI, EAGER_ATOM_FIELD_MULTI);

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/eager_global_ordinals.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(NOT_EAGER_TEXT_FIELD, MultiValuedField.newBuilder().addValue("v1").build())
            .putFields(EAGER_TEXT_FIELD, MultiValuedField.newBuilder().addValue("v3").build())
            .putFields(
                NOT_EAGER_TEXT_FIELD_MULTI,
                MultiValuedField.newBuilder().addValue("v1").addValue("v2").build())
            .putFields(
                EAGER_TEXT_FIELD_MULTI,
                MultiValuedField.newBuilder().addValue("v3").addValue("v4").build())
            .putFields(NOT_EAGER_ATOM_FIELD, MultiValuedField.newBuilder().addValue("v1").build())
            .putFields(EAGER_ATOM_FIELD, MultiValuedField.newBuilder().addValue("v3").build())
            .putFields(
                NOT_EAGER_ATOM_FIELD_MULTI,
                MultiValuedField.newBuilder().addValue("v1").addValue("v2").build())
            .putFields(
                EAGER_ATOM_FIELD_MULTI,
                MultiValuedField.newBuilder().addValue("v3").addValue("v4").build())
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(NOT_EAGER_TEXT_FIELD, MultiValuedField.newBuilder().addValue("v2").build())
            .putFields(EAGER_TEXT_FIELD, MultiValuedField.newBuilder().addValue("v4").build())
            .putFields(
                NOT_EAGER_TEXT_FIELD_MULTI,
                MultiValuedField.newBuilder().addValue("v3").addValue("v5").build())
            .putFields(
                EAGER_TEXT_FIELD_MULTI,
                MultiValuedField.newBuilder().addValue("v2").addValue("v6").build())
            .putFields(NOT_EAGER_ATOM_FIELD, MultiValuedField.newBuilder().addValue("v7").build())
            .putFields(EAGER_ATOM_FIELD, MultiValuedField.newBuilder().addValue("v8").build())
            .putFields(
                NOT_EAGER_ATOM_FIELD_MULTI,
                MultiValuedField.newBuilder().addValue("v1").addValue("v6").build())
            .putFields(
                EAGER_ATOM_FIELD_MULTI,
                MultiValuedField.newBuilder().addValue("v2").addValue("v7").build())
            .build();
    docs.add(request);
    addDocuments(docs.stream());
  }

  @Test
  public void testEagerOrdinals() throws Exception {
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    ShardState shardState = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0);
    try {
      for (String field : EAGER_FIELDS) {
        FieldDef fieldDef = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getField(field);
        addDocAndRefresh();
        s = shardState.acquire();
        assertGlobalOrdinals(s.searcher.getIndexReader(), fieldDef);
        doQuery(field);
        assertGlobalOrdinals(s.searcher.getIndexReader(), fieldDef);

        // create new reader version
        addDocAndRefresh();
        shardState.release(s);
        s = null;
        s = shardState.acquire();
        assertGlobalOrdinals(s.searcher.getIndexReader(), fieldDef);
      }
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
      for (String field : NOT_EAGER_FIELDS) {
        FieldDef fieldDef = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getField(field);
        addDocAndRefresh();
        s = shardState.acquire();
        assertNoGlobalOrdinals(s.searcher.getIndexReader(), fieldDef);
        doQuery(field);
        assertGlobalOrdinals(s.searcher.getIndexReader(), fieldDef);

        // create new reader version
        addDocAndRefresh();
        shardState.release(s);
        s = null;
        s = shardState.acquire();
        assertNoGlobalOrdinals(s.searcher.getIndexReader(), fieldDef);
        doQuery(field);
        assertGlobalOrdinals(s.searcher.getIndexReader(), fieldDef);
      }
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  private void assertNoGlobalOrdinals(IndexReader reader, FieldDef fieldDef) throws IOException {
    assertTrue(fieldDef instanceof TextBaseFieldDef);
    TextBaseFieldDef textBaseFieldDef = (TextBaseFieldDef) fieldDef;
    GlobalOrdinalLookup lookup =
        textBaseFieldDef.ordinalLookupCache.get(reader.getReaderCacheHelper().getKey());
    assertNull(lookup);
  }

  private void assertGlobalOrdinals(IndexReader reader, FieldDef fieldDef) throws IOException {
    assertTrue(fieldDef instanceof TextBaseFieldDef);
    TextBaseFieldDef textBaseFieldDef = (TextBaseFieldDef) fieldDef;
    GlobalOrdinalLookup lookup =
        textBaseFieldDef.ordinalLookupCache.get(reader.getReaderCacheHelper().getKey());
    assertNotNull(lookup);
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
                .putCollectors(
                    "test_collector",
                    Collector.newBuilder()
                        .setTerms(TermsCollector.newBuilder().setField(field).setSize(10).build())
                        .build())
                .build());
  }

  private void addDocAndRefresh() throws Exception {
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .putFields(NOT_EAGER_TEXT_FIELD, MultiValuedField.newBuilder().addValue("v2").build())
            .putFields(EAGER_TEXT_FIELD, MultiValuedField.newBuilder().addValue("v4").build())
            .putFields(
                NOT_EAGER_TEXT_FIELD_MULTI,
                MultiValuedField.newBuilder().addValue("v3").addValue("v5").build())
            .putFields(
                EAGER_TEXT_FIELD_MULTI,
                MultiValuedField.newBuilder().addValue("v2").addValue("v6").build())
            .putFields(NOT_EAGER_ATOM_FIELD, MultiValuedField.newBuilder().addValue("v7").build())
            .putFields(EAGER_ATOM_FIELD, MultiValuedField.newBuilder().addValue("v8").build())
            .putFields(
                NOT_EAGER_ATOM_FIELD_MULTI,
                MultiValuedField.newBuilder().addValue("v1").addValue("v6").build())
            .putFields(
                EAGER_ATOM_FIELD_MULTI,
                MultiValuedField.newBuilder().addValue("v2").addValue("v7").build())
            .build();
    addDocuments(Stream.of(request));
    getGrpcServer()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(DEFAULT_TEST_INDEX).build());
  }
}
