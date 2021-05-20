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
package com.yelp.nrtsearch.server.luceneserver.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.GlobalOrdinalable;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.stream.Stream;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.util.LongValues;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class GlobalOrdinalLookupTest extends ServerTestCase {
  private static final String VALUE_FIELD = "value";
  private static final String VALUE_MULTI_FIELD = "value_multi";

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/OrdinalsRegisterFields.json");
  }

  protected void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
  }

  @Before
  public void clearIndex() throws Exception {
    IndexWriter writer = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0).writer;
    writer.deleteAll();
  }

  @Test
  public void testEmptyIndex() throws IOException {
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    IndexState indexState = getGlobalState().getIndex(DEFAULT_TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    try {
      s = shardState.acquire();
      assertEmptyLookup(indexState.getField(VALUE_FIELD), s.searcher.getIndexReader());
      assertEmptyLookup(indexState.getField(VALUE_MULTI_FIELD), s.searcher.getIndexReader());
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  private void assertEmptyLookup(FieldDef fieldDef, IndexReader reader) throws IOException {
    assertTrue(fieldDef instanceof GlobalOrdinalable);
    GlobalOrdinalLookup ordinalLookup = ((GlobalOrdinalable) fieldDef).getOrdinalLookup(reader);
    assertNotNull(ordinalLookup);
    assertEquals(0, ordinalLookup.getNumOrdinals());
    assertSame(GlobalOrdinalLookup.IDENTITY_MAPPING, ordinalLookup.getSegmentMapping(0));
    try {
      ordinalLookup.lookupGlobalOrdinal(0);
      fail();
    } catch (IllegalStateException e) {
      assertEquals("No ordinals for field: " + fieldDef.getName(), e.getMessage());
    }
  }

  @Test
  public void testSingleSegment() throws Exception {
    addData("1");

    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    IndexState indexState = getGlobalState().getIndex(DEFAULT_TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    try {
      s = shardState.acquire();
      assertSingleLookup(indexState.getField(VALUE_FIELD), s.searcher.getIndexReader());
      assertSingleLookup(indexState.getField(VALUE_MULTI_FIELD), s.searcher.getIndexReader());
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  private void assertSingleLookup(FieldDef fieldDef, IndexReader reader) throws IOException {
    assertTrue(fieldDef instanceof GlobalOrdinalable);
    GlobalOrdinalLookup ordinalLookup = ((GlobalOrdinalable) fieldDef).getOrdinalLookup(reader);
    assertNotNull(ordinalLookup);
    assertEquals(1, ordinalLookup.getNumOrdinals());
    assertSame(GlobalOrdinalLookup.IDENTITY_MAPPING, ordinalLookup.getSegmentMapping(0));
    assertEquals("1", ordinalLookup.lookupGlobalOrdinal(0));
  }

  @Test
  public void testMultiSegment() throws Exception {
    addData("1");
    addData("3");

    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    IndexState indexState = getGlobalState().getIndex(DEFAULT_TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    try {
      s = shardState.acquire();
      assertMultiLookup(indexState.getField(VALUE_FIELD), s.searcher.getIndexReader());
      assertMultiLookup(indexState.getField(VALUE_MULTI_FIELD), s.searcher.getIndexReader());
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  private void assertMultiLookup(FieldDef fieldDef, IndexReader reader) throws IOException {
    assertTrue(fieldDef instanceof GlobalOrdinalable);
    GlobalOrdinalLookup ordinalLookup = ((GlobalOrdinalable) fieldDef).getOrdinalLookup(reader);
    assertNotNull(ordinalLookup);
    assertEquals(2, ordinalLookup.getNumOrdinals());
    assertNotSame(GlobalOrdinalLookup.IDENTITY_MAPPING, ordinalLookup.getSegmentMapping(0));
    assertNotSame(GlobalOrdinalLookup.IDENTITY_MAPPING, ordinalLookup.getSegmentMapping(1));
    LongValues mapping = ordinalLookup.getSegmentMapping(0);
    assertEquals(0, mapping.get(0));
    mapping = ordinalLookup.getSegmentMapping(1);
    assertEquals(1, mapping.get(0));
    assertEquals("1", ordinalLookup.lookupGlobalOrdinal(0));
    assertEquals("3", ordinalLookup.lookupGlobalOrdinal(1));
  }

  private void addData(String value) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0).writer;

    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .putFields(
                VALUE_FIELD,
                AddDocumentRequest.MultiValuedField.newBuilder().addValue(value).build())
            .putFields(
                VALUE_MULTI_FIELD,
                AddDocumentRequest.MultiValuedField.newBuilder().addValue(value).build())
            .build();
    addDocuments(Stream.of(request));
    writer.flush();
    getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0).maybeRefreshBlocking();
  }
}
