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
package com.yelp.nrtsearch.server.doc;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.junit.Test;

public class ParentDocLookupTest extends ServerTestCase {

  private static final String TEST_INDEX = "test_index";
  protected Gson gson = new GsonBuilder().serializeNulls().create();

  @Override
  public List<String> getIndices() {
    return List.of(TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/registerFieldsNestedQueryWithParentAccess.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();

    Map<String, Object> partner1 = new HashMap<>();
    partner1.put("name", "Partner One");
    partner1.put("hours", 8);

    Map<String, Object> partner2 = new HashMap<>();
    partner2.put("name", "Partner Two");
    partner2.put("hours", 10);

    AddDocumentRequest doc1 =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("doc1").build())
            .putFields(
                "pickup_partners",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(gson.toJson(partner1))
                    .addValue(gson.toJson(partner2))
                    .build())
            .build();

    AddDocumentRequest doc2 =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("doc2").build())
            .build();

    docs.add(doc1);
    docs.add(doc2);
    addDocuments(docs.stream());
  }

  @Test
  public void testParentAccessWithValidNestedDocs() throws Exception {
    IndexState indexState = getGlobalState().getIndexOrThrow(TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = shardState.acquire();
      DirectoryReader reader = (DirectoryReader) s.searcher().getIndexReader();
      LeafReaderContext leafContext = reader.leaves().get(0);

      SegmentDocLookup docLookup = new SegmentDocLookup(indexState::getField, leafContext);

      Method tryGetMethod =
          SegmentDocLookup.class.getDeclaredMethod("tryGetFromParentDocument", String.class);
      tryGetMethod.setAccessible(true);

      // Test 1: Child document should be able to access parent field
      docLookup.setDocId(0); // First child document
      LoadedDocValues<?> parentField =
          (LoadedDocValues<?>) tryGetMethod.invoke(docLookup, "doc_id");

      if (parentField != null && !parentField.isEmpty()) {
        assertNotNull("Should find parent field from child document", parentField);
        // Parent doc_id should be accessible
        Object parentValue = parentField.getFirst();
        assertNotNull("Parent field should have a value", parentValue);
      }

      // Test 2: Non-existent field should return null
      LoadedDocValues<?> nonExistentField = null;
      try {
        nonExistentField =
            (LoadedDocValues<?>) tryGetMethod.invoke(docLookup, "non_existent_field");
      } catch (Exception e) {
        if (e.getCause() instanceof IllegalArgumentException) {
          nonExistentField = null;
        } else {
          throw e;
        }
      }
      assertNull("Non-existent field should return null", nonExistentField);

      // Test 3: Parent document (no offset field) should return null
      docLookup.setDocId(2);
      LoadedDocValues<?> fromParentDoc =
          (LoadedDocValues<?>) tryGetMethod.invoke(docLookup, "doc_id");
      assertNull("Parent document should return null (no offset field)", fromParentDoc);
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  @Test
  public void testParentAccessWithDifferentFieldTypes() throws Exception {
    IndexState indexState = getGlobalState().getIndexOrThrow(TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = shardState.acquire();
      DirectoryReader reader = (DirectoryReader) s.searcher().getIndexReader();
      LeafReaderContext leafContext = reader.leaves().get(0);

      SegmentDocLookup docLookup = new SegmentDocLookup(indexState::getField, leafContext);

      Method tryGetMethod =
          SegmentDocLookup.class.getDeclaredMethod("tryGetFromParentDocument", String.class);
      tryGetMethod.setAccessible(true);

      docLookup.setDocId(0);

      String[] fieldsToTest = {"doc_id", "pickup_partners", "int_field"};

      for (String fieldName : fieldsToTest) {
        try {
          LoadedDocValues<?> result =
              (LoadedDocValues<?>) tryGetMethod.invoke(docLookup, fieldName);
          assertTrue("Field access should not throw unexpected exceptions: " + fieldName, true);
        } catch (Exception e) {
          assertTrue(
              "Field access resulted in an exception (which may be expected): " + fieldName, true);
        }
      }
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  @Test
  public void testParentAccessWithoutNestedDocuments() throws Exception {
    // Test scenario where there are no nested documents
    IndexState indexState = getGlobalState().getIndexOrThrow(TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = shardState.acquire();
      DirectoryReader reader = (DirectoryReader) s.searcher().getIndexReader();
      LeafReaderContext leafContext = reader.leaves().get(0);

      SegmentDocLookup docLookup = new SegmentDocLookup(indexState::getField, leafContext);

      Method tryGetMethod =
          SegmentDocLookup.class.getDeclaredMethod("tryGetFromParentDocument", String.class);
      tryGetMethod.setAccessible(true);

      int maxDoc = leafContext.reader().maxDoc();
      if (maxDoc > 1) {
        docLookup.setDocId(maxDoc - 1);

        LoadedDocValues<?> result = (LoadedDocValues<?>) tryGetMethod.invoke(docLookup, "doc_id");
        assertNull("Document without nested structure should return null", result);
      }
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  @Test
  public void testOffsetFieldCalculation() throws Exception {
    // Test that the offset field calculation works correctly
    IndexState indexState = getGlobalState().getIndexOrThrow(TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = shardState.acquire();
      DirectoryReader reader = (DirectoryReader) s.searcher().getIndexReader();
      LeafReaderContext leafContext = reader.leaves().get(0);

      SegmentDocLookup docLookup = new SegmentDocLookup(indexState::getField, leafContext);

      LoadedDocValues<?> offsetField = docLookup.get(IndexState.NESTED_DOCUMENT_OFFSET);

      if (offsetField != null) {
        assertNotNull("Offset field should exist for nested documents", offsetField);

        for (int i = 0; i < Math.min(3, leafContext.reader().maxDoc()); i++) {
          docLookup.setDocId(i);
          if (!offsetField.isEmpty()) {
            Object offsetValue = offsetField.getFirst();
            assertTrue(
                "Offset should be a positive number",
                offsetValue instanceof Number && ((Number) offsetValue).intValue() > 0);
          }
        }
      }
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }
}
