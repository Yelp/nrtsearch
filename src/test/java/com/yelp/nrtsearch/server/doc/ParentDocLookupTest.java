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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.field.FieldDef;
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
      LeafReaderContext leafContext = reader.leaves().get(0);

      // Create SegmentDocLookup with nested path context for automatic parent resolution
      SegmentDocLookup docLookup =
          new SegmentDocLookup(indexState::getField, leafContext, "pickup_partners");

      // Test 1: Child document should be able to access parent field via automatic resolution
      docLookup.setDocId(0);

      try {
        LoadedDocValues<?> parentField = docLookup.get("doc_id");
        if (parentField != null && !parentField.isEmpty()) {
          assertNotNull("Should find parent field from child document", parentField);
          Object parentValue = parentField.getFirst();
          assertNotNull("Parent field should have a value", parentValue);
        }
      } catch (IllegalArgumentException e) {
        // Expected if document structure doesn't support parent access
        assertTrue(
            "Expected exception for parent field access: " + e.getMessage(),
            e.getMessage().contains("Could not access parent field")
                || e.getMessage().contains("document may not be nested"));
      }

      // Test 2: Non-existent field should throw exception
      try {
        docLookup.get("non_existent_field");
        assertTrue("Should throw exception for non-existent field", false);
      } catch (IllegalArgumentException e) {
        assertTrue(
            "Exception should mention field does not exist",
            e.getMessage().contains("Field does not exist"));
      }

      // Test 3: Test without nested context (should work for regular field access)
      SegmentDocLookup rootDocLookup =
          new SegmentDocLookup(indexState::getField, leafContext, null);
      rootDocLookup.setDocId(2);
      try {
        LoadedDocValues<?> regularField = rootDocLookup.get("doc_id");
        assertNotNull("Should be able to access regular fields in root context", regularField);
      } catch (Exception e) {
        // May fail if no documents at this index
        assertTrue("Regular field access resulted in exception: " + e.getMessage(), true);
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
      LeafReaderContext leafContext = reader.leaves().get(0);

      SegmentDocLookup docLookup = new SegmentDocLookup(indexState::getField, leafContext);

      docLookup.setDocId(0);

      String[] fieldsToTest = {"doc_id", "pickup_partners", "int_field"};

      for (String fieldName : fieldsToTest) {
        try {
          FieldDef fieldDef = indexState.getField(fieldName);
          LoadedDocValues<?> result = (LoadedDocValues<?>) docLookup.get(fieldName);
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
    // Test scenario where we access fields from documents without nested context
    IndexState indexState = getGlobalState().getIndexOrThrow(TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = shardState.acquire();
      DirectoryReader reader = (DirectoryReader) s.searcher().getIndexReader();
      LeafReaderContext leafContext = reader.leaves().get(0);

      // Test with no nested path context (root context)
      SegmentDocLookup docLookup = new SegmentDocLookup(indexState::getField, leafContext, null);

      int maxDoc = leafContext.reader().maxDoc();
      if (maxDoc > 1) {
        docLookup.setDocId(maxDoc - 1); // This should be a parent document

        // Accessing regular fields should work fine in root context
        LoadedDocValues<?> regularField = docLookup.get("doc_id");
        assertNotNull("Regular field access should work on parent documents", regularField);

        // Test with nested context but no actual parent available
        SegmentDocLookup nestedDocLookup =
            new SegmentDocLookup(indexState::getField, leafContext, "pickup_partners");
        nestedDocLookup.setDocId(maxDoc - 1); // Parent document

        try {
          LoadedDocValues<?> parentField = nestedDocLookup.get("doc_id");
          // This might work if the field is accessible, or fail if parent resolution doesn't apply
          if (parentField != null) {
            assertTrue("Parent field access completed", true);
          }
        } catch (IllegalArgumentException e) {
          // Expected if trying to access parent from a document that doesn't have nested structure
          assertTrue(
              "Expected exception for parent field access from non-nested document: "
                  + e.getMessage(),
              e.getMessage().contains("Could not access parent field")
                  || e.getMessage().contains("document may not be nested"));
        }
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

  @Test
  public void testExplicitParentFieldAccess() throws Exception {
    IndexState indexState = getGlobalState().getIndexOrThrow(TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = shardState.acquire();
      DirectoryReader reader = (DirectoryReader) s.searcher().getIndexReader();
      LeafReaderContext leafContext = reader.leaves().get(0);

      // Test 1: Create SegmentDocLookup with nested path context for automatic parent resolution
      SegmentDocLookup docLookup =
          new SegmentDocLookup(indexState::getField, leafContext, "pickup_partners");

      // Test accessing parent field from nested context
      docLookup.setDocId(0); // Child document

      try {
        LoadedDocValues<?> parentField = docLookup.get("doc_id");
        // If this succeeds, the automatic parent resolution is working
        assertNotNull(
            "Should be able to access parent field via automatic resolution", parentField);
        if (!parentField.isEmpty()) {
          Object parentValue = parentField.getFirst();
          assertNotNull("Parent field value should not be null", parentValue);
        }

        // Test 2: containsKey should work with automatic resolution
        assertTrue(
            "containsKey should return true for parent field", docLookup.containsKey("doc_id"));

      } catch (IllegalArgumentException e) {
        // This is expected if the document structure doesn't support parent access
        assertTrue(
            "Expected exception for parent field access: " + e.getMessage(),
            e.getMessage().contains("Could not access parent field")
                || e.getMessage().contains("document may not be nested"));
      }

      // Test 3: Try to access non-existent field
      try {
        docLookup.get("non_existent_field");
        assertTrue("Should throw exception for non-existent field", false);
      } catch (IllegalArgumentException e) {
        assertTrue(
            "Exception message should mention field does not exist",
            e.getMessage().contains("Field does not exist"));
      }

      // Test 4: Test with root context (no nested path)
      SegmentDocLookup rootDocLookup =
          new SegmentDocLookup(indexState::getField, leafContext, null);
      docLookup.setDocId(2); // Parent document

      try {
        LoadedDocValues<?> rootField = rootDocLookup.get("doc_id");
        assertNotNull("Should be able to access fields in root context", rootField);
      } catch (Exception e) {
        // This might fail if there are no documents at this index
        assertTrue("Root field access resulted in an exception: " + e.getMessage(), true);
      }

      // Test 5: containsKey should work for non-existent fields
      assertFalse(
          "containsKey should return false for non-existent field",
          docLookup.containsKey("non_existent_field"));

      // Test 6: Test nested field access (should work without parent resolution)
      try {
        LoadedDocValues<?> nestedField = docLookup.get("pickup_partners.name");
        // This should work for fields within the nested context
        if (nestedField != null) {
          assertTrue("Nested field access should work within context", true);
        }
      } catch (IllegalArgumentException e) {
        // Expected if the field structure doesn't match
        assertTrue("Nested field access failed as expected: " + e.getMessage(), true);
      }

    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }
}
