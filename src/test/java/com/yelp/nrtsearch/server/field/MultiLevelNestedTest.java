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
package com.yelp.nrtsearch.server.field;

import static org.junit.Assert.assertEquals;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RefreshRequest;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Tests for multi-level nested document support. Covers the block structure produced when OBJECT
 * fields with nestedDoc=true are nested within other nestedDoc=true OBJECT fields.
 *
 * <p>Schema: orders (nested) → items (nested) → item_name/quantity
 */
public class MultiLevelNestedTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final Gson GSON = new GsonBuilder().serializeNulls().create();

  @Override
  public List<String> getIndices() {
    return List.of(DEFAULT_TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsMultiLevelNested.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    addDocuments(buildTestDocs(name).stream());
  }

  /**
   * Build two parent documents, each with multiple orders, each order having multiple items.
   *
   * <p>Doc 1 (doc_id="1", parent_field="p1"): order1 → [widget(qty=2), gadget(qty=1)] order2 →
   * [thingamajig(qty=3)]
   *
   * <p>Doc 2 (doc_id="2", parent_field="p2"): order3 → [doohickey(qty=5)]
   */
  private List<AddDocumentRequest> buildTestDocs(String indexName) {
    List<AddDocumentRequest> docs = new ArrayList<>();

    // Doc 1
    Map<String, Object> item1 = new LinkedHashMap<>();
    item1.put("item_name", "widget");
    item1.put("quantity", 2);

    Map<String, Object> item2 = new LinkedHashMap<>();
    item2.put("item_name", "gadget");
    item2.put("quantity", 1);

    Map<String, Object> item3 = new LinkedHashMap<>();
    item3.put("item_name", "thingamajig");
    item3.put("quantity", 3);

    Map<String, Object> order1 = new LinkedHashMap<>();
    order1.put("order_name", "order1");
    order1.put("items", List.of(item1, item2));

    Map<String, Object> order2 = new LinkedHashMap<>();
    order2.put("order_name", "order2");
    order2.put("items", List.of(item3));

    docs.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(indexName)
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("1").build())
            .putFields(
                "parent_field",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("p1").build())
            .putFields(
                "orders",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(GSON.toJson(order1))
                    .addValue(GSON.toJson(order2))
                    .build())
            .build());

    // Doc 2
    Map<String, Object> item4 = new LinkedHashMap<>();
    item4.put("item_name", "doohickey");
    item4.put("quantity", 5);

    Map<String, Object> order3 = new LinkedHashMap<>();
    order3.put("order_name", "order3");
    order3.put("items", List.of(item4));

    docs.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(indexName)
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("2").build())
            .putFields(
                "parent_field",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("p2").build())
            .putFields(
                "orders",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(GSON.toJson(order3))
                    .build())
            .build());

    return docs;
  }

  // ─────────────────────────── nested-path tests ────────────────────────────

  /**
   * Items must be indexed with _nested_path="orders.items" so that queryNestedPath can find them.
   * Before the multi-level fix this fails because items were flattened into order docs with
   * _nested_path="orders".
   */
  @Test
  public void testItemsIndexedWithInnerNestedPath() {
    SearchResponse response = queryAtNestedPath("orders.items", List.of("orders.items.item_name"));
    assertDataFields(
        response, "orders.items.item_name", "widget", "gadget", "thingamajig", "doohickey");
  }

  /**
   * Order docs must still carry _nested_path="orders". Regression guard for single-level behavior.
   */
  @Test
  public void testOrdersStillIndexedWithOuterNestedPath() {
    SearchResponse response = queryAtNestedPath("orders", List.of("orders.order_name"));
    assertDataFields(response, "orders.order_name", "order1", "order2", "order3");
  }

  /**
   * Query on an inner-level field restricted to a specific item value should only return that item
   * doc when searching at the items level.
   */
  @Test
  public void testQueryItemByFieldAtInnerNestedPath() {
    SearchResponse response =
        queryAtNestedPath(
            "orders.items",
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder()
                        .setField("orders.items.item_name")
                        .setTextValue("widget")
                        .build())
                .build(),
            List.of("orders.items.item_name"));
    assertDataFields(response, "orders.items.item_name", "widget");
  }

  // ─────────────────────── parent-offset structure tests ────────────────────

  /**
   * Verify that item child docs have a _parent_offset that points to an order doc (not root). This
   * validates that PR1's per-level offset computation is correct.
   *
   * <p>Expected block layout (within a single segment): [item: widget] _nested_path=orders.items
   * [item: gadget] _nested_path=orders.items [order: order1] _nested_path=orders [item:
   * thingamajig] _nested_path=orders.items [order: order2] _nested_path=orders [root doc1]
   * _nested_path=_root [item: doohickey] _nested_path=orders.items [order: order3]
   * _nested_path=orders [root doc2] _nested_path=_root
   */
  @Test
  public void testParentOffsetPointsToImmediateParent() throws Exception {
    IndexState indexState = getGlobalState().getIndexOrThrow(DEFAULT_TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = shardState.acquire();
      DirectoryReader reader = (DirectoryReader) s.searcher().getIndexReader();

      for (LeafReaderContext leaf : reader.leaves()) {
        verifyParentOffsets(leaf, indexState);
      }
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  /**
   * For each leaf segment, verify: 1. Item docs (orders.items) actually exist in the index. 2.
   * Every item doc's _parent_offset points to an order doc (not root). 3. Every order doc's
   * _parent_offset points to a root doc.
   */
  private void verifyParentOffsets(LeafReaderContext leaf, IndexState indexState) throws Exception {
    int maxDoc = leaf.reader().maxDoc();
    if (maxDoc == 0) {
      return;
    }

    // Build bitsets for each nested path level in this leaf
    BitSet itemDocs = docsForNestedPath(leaf, "orders.items");
    BitSet orderDocs = docsForNestedPath(leaf, "orders");
    BitSet rootDocs = docsForNestedPath(leaf, IndexState.ROOT);

    // Item docs must exist in the index — this assertion fails before the fix because
    // items are flattened into order docs and never indexed with _nested_path=orders.items
    assertEquals(
        "Expected item docs with _nested_path=orders.items to exist in segment",
        true,
        itemDocs != null && itemDocs.cardinality() > 0);

    NumericDocValues offsets = leaf.reader().getNumericDocValues(IndexState.NESTED_DOCUMENT_OFFSET);
    if (offsets == null) {
      return; // leaf has no nested docs
    }

    for (int docId = 0; docId < maxDoc; docId++) {
      if (!offsets.advanceExact(docId)) {
        continue; // root doc has no offset
      }
      long offset = offsets.longValue();
      int parentDocId = (int) (docId + offset);

      if (itemDocs != null && itemDocs.get(docId)) {
        // item doc: parent must be an order doc
        assertEquals(
            "Item doc at " + docId + " must have parent at an order doc position",
            true,
            orderDocs != null && orderDocs.get(parentDocId));
      } else if (orderDocs != null && orderDocs.get(docId)) {
        // order doc: parent must be a root doc
        assertEquals(
            "Order doc at " + docId + " must have parent at a root doc position",
            true,
            rootDocs != null && rootDocs.get(parentDocId));
      }
    }
  }

  private BitSet docsForNestedPath(LeafReaderContext leaf, String nestedPath) throws Exception {
    Terms terms = leaf.reader().terms(IndexState.NESTED_PATH);
    if (terms == null) {
      return null;
    }
    TermsEnum te = terms.iterator();
    if (!te.seekExact(new BytesRef(nestedPath))) {
      return null;
    }
    var postings = te.postings(null);
    FixedBitSet bitSet = new FixedBitSet(leaf.reader().maxDoc());
    int doc;
    while ((doc = postings.nextDoc()) != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
      bitSet.set(doc);
    }
    return bitSet;
  }

  // ─────────────────────── update / delete tests ────────────────────────────

  /**
   * When a document with multi-level nested data is updated (same doc_id), all previous child docs
   * (both outer and inner) must be replaced.
   */
  @Test
  public void testUpdateReplacesAllNestedLevels() throws Exception {
    // Initial state: doc_id="1" has order1 with widget/gadget, order2 with thingamajig
    SearchResponse before = queryAtNestedPath("orders.items", List.of("orders.items.item_name"));
    assertDataFields(
        before, "orders.items.item_name", "widget", "gadget", "thingamajig", "doohickey");

    // Update doc_id="1" with entirely new orders/items
    Map<String, Object> newItem = new LinkedHashMap<>();
    newItem.put("item_name", "replacement_item");
    newItem.put("quantity", 99);

    Map<String, Object> newOrder = new LinkedHashMap<>();
    newOrder.put("order_name", "new_order");
    newOrder.put("items", List.of(newItem));

    AddDocumentRequest update =
        AddDocumentRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("1").build())
            .putFields(
                "parent_field",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("p1_updated").build())
            .putFields(
                "orders",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(GSON.toJson(newOrder))
                    .build())
            .build();
    addDocuments(List.of(update).stream());

    refresh();

    // After update: old items (widget, gadget, thingamajig) must be gone
    SearchResponse after = queryAtNestedPath("orders.items", List.of("orders.items.item_name"));
    assertDataFields(after, "orders.items.item_name", "replacement_item", "doohickey");
  }

  // ─────────────────────── helper methods ────────────────────────────────────

  private SearchResponse queryAtNestedPath(String nestedPath, List<String> retrieveFields) {
    return queryAtNestedPath(nestedPath, Query.newBuilder().build(), retrieveFields);
  }

  private SearchResponse queryAtNestedPath(
      String nestedPath, Query query, List<String> retrieveFields) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(100)
                .addAllRetrieveFields(retrieveFields)
                .setQuery(query)
                .setQueryNestedPath(nestedPath)
                .build());
  }

  private void assertDataFields(
      SearchResponse response, String fieldName, String... expectedValues) {
    Set<String> actual = new HashSet<>();
    for (SearchResponse.Hit hit : response.getHitsList()) {
      SearchResponse.Hit.CompositeFieldValue fv = hit.getFieldsOrDefault(fieldName, null);
      if (fv != null) {
        for (int i = 0; i < fv.getFieldValueCount(); i++) {
          actual.add(fv.getFieldValue(i).getTextValue());
        }
      }
    }
    assertEquals(new HashSet<>(Arrays.asList(expectedValues)), actual);
  }

  private void refresh() {
    getGrpcServer()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(DEFAULT_TEST_INDEX).build());
  }
}
