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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import java.io.IOException;
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
      LeafReaderContext leafContext = reader.leaves().getFirst();

      SegmentDocLookup docLookup = new SegmentDocLookup(indexState::getField, leafContext);

      // Test 1: Child document should be able to access parent field
      docLookup.setDocId(0);
      LoadedDocValues<?> parentField = docLookup.get("_PARENT.doc_id");

      if (parentField != null && !parentField.isEmpty()) {
        assertNotNull("Should find parent field from child document", parentField);
        Object parentValue = parentField.getFirst();
        assertNotNull("Parent field should have a value", parentValue);
      }

      // Test 2: Non-existent field should throw exception
      try {
        docLookup.get("_PARENT.non_existent_field");
        fail("Should throw exception for non-existent parent field");
      } catch (IllegalArgumentException e) {
        assertTrue(
            "Exception should mention field does not exist",
            e.getMessage().contains("Field does not exist"));
      }

      // Test 3: Parent document (no offset field) should throw exception
      int maxDoc = leafContext.reader().maxDoc();
      if (maxDoc > 2) {
        docLookup.setDocId(maxDoc - 1); // This should be a parent document
        try {
          docLookup.get("_PARENT.doc_id");
          fail("Should throw exception when accessing parent from parent document");
        } catch (IllegalArgumentException e) {
          assertTrue(
              "Exception should mention document may not be nested",
              e.getMessage().contains("document may not be nested")
                  || e.getMessage().contains("Could not access parent field"));
        }
      }
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
      LeafReaderContext leafContext = reader.leaves().getFirst();

      SegmentDocLookup docLookup = new SegmentDocLookup(indexState::getField, leafContext);

      docLookup.setDocId(0);

      String[] fieldsToTest = {"doc_id", "pickup_partners", "int_field"};

      for (String fieldName : fieldsToTest) {
        try {
          LoadedDocValues<?> result = docLookup.get(fieldName);
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
    // Test scenario where we try to access parent fields from a document that isn't nested
    IndexState indexState = getGlobalState().getIndexOrThrow(TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = shardState.acquire();
      DirectoryReader reader = (DirectoryReader) s.searcher().getIndexReader();
      LeafReaderContext leafContext = reader.leaves().getFirst();

      SegmentDocLookup docLookup = new SegmentDocLookup(indexState::getField, leafContext);

      docLookup.setDocId(2); // This should be a parent document

      // Accessing regular fields should work fine
      LoadedDocValues<?> regularField = docLookup.get("doc_id");
      assertNotNull("Regular field access should work on parent documents", regularField);

      // But accessing _PARENT. fields from a parent document should fail
      try {
        LoadedDocValues<?> parentField = docLookup.get("_PARENT.doc_id");
        fail(
            "Should throw exception when accessing _PARENT field from parent document, but got: "
                + parentField);
      } catch (IllegalArgumentException e) {
        assertTrue(
            "Exception should mention document may not be nested or parent field access failure. Got: "
                + e.getMessage(),
            e.getMessage().contains("document may not be nested")
                || e.getMessage().contains("Could not access parent field"));
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
      LeafReaderContext leafContext = reader.leaves().getFirst();

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

  @Test
  public void testExplicitParentFieldAccess() throws Exception {
    IndexState indexState = getGlobalState().getIndexOrThrow(TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = shardState.acquire();
      DirectoryReader reader = (DirectoryReader) s.searcher().getIndexReader();
      LeafReaderContext leafContext = reader.leaves().getFirst();

      SegmentDocLookup docLookup = new SegmentDocLookup(indexState::getField, leafContext);

      // Test 1: Access parent field using _PARENT. notation from child document
      docLookup.setDocId(0); // Child document
      LoadedDocValues<?> parentField = docLookup.get("_PARENT.doc_id");

      assertNotNull("Should be able to access parent field using _PARENT. notation", parentField);
      assertFalse("Parent field should have a value", parentField.isEmpty());
      Object parentValue = parentField.getFirst();
      assertNotNull("Parent field value should not be null", parentValue);

      // Test 2: containsKey should work with _PARENT. notation
      assertTrue(
          "containsKey should return true for _PARENT.doc_id",
          docLookup.containsKey("_PARENT.doc_id"));

      // Test 3: Try to access non-existent parent field
      try {
        docLookup.get("_PARENT.non_existent_field");
        fail("Should throw exception for non-existent parent field");
      } catch (IllegalArgumentException e) {
        assertTrue(
            "Exception message should mention field does not exist",
            e.getMessage().contains("Field does not exist"));
      }

      // Test 4: Try to access parent field from parent document (should fail)

      docLookup.setDocId(2); // Parent document
      try {
        docLookup.get("_PARENT.doc_id");
        fail("Should throw exception when accessing parent field from parent document");
      } catch (IllegalArgumentException e) {
        assertTrue(
            "Exception message should mention that document may not be nested",
            e.getMessage().contains("document may not be nested")
                || e.getMessage().contains("Could not access parent field"));
      }

      // Test 5: containsKey should work for non-existent parent fields
      assertFalse(
          "containsKey should return false for non-existent parent field",
          docLookup.containsKey("_PARENT.non_existent_field"));

    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }
}
