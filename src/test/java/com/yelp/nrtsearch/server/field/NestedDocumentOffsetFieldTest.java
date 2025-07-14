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
package com.yelp.nrtsearch.server.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.index.IndexState;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NestedDocumentOffsetFieldTest {

  private Directory directory;
  private IndexWriter indexWriter;
  private DirectoryReader directoryReader;

  @Before
  public void setUp() throws Exception {
    directory = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig();
    indexWriter = new IndexWriter(directory, config);
  }

  @After
  public void tearDown() throws Exception {
    if (directoryReader != null) {
      directoryReader.close();
    }
    indexWriter.close();
    directory.close();
  }

  @Test
  public void testNestedDocumentOffsetFieldCalculation() throws Exception {
    List<Map<String, Object>> nestedObjects = createTestNestedObjects();

    Document parentDoc = new Document();
    parentDoc.add(new StringField("id", "parent1", Field.Store.YES));

    List<Document> childDocuments = new ArrayList<>();
    int totalDocs = nestedObjects.size();

    for (int i = 0; i < totalDocs; i++) {
      Document childDoc = new Document();
      Map<String, Object> nestedObj = nestedObjects.get(i);

      childDoc.add(new StringField("name", (String) nestedObj.get("name"), Field.Store.YES));
      childDoc.add(
          new NumericDocValuesField("hours", ((Number) nestedObj.get("hours")).longValue()));

      // Calculate offset as n-i (total docs minus current index)
      int expectedOffset = totalDocs - i;

      childDoc.add(new NumericDocValuesField(IndexState.NESTED_DOCUMENT_OFFSET, expectedOffset));

      childDoc.add(new StringField(IndexState.NESTED_PATH, "pickup_partners", Field.Store.YES));

      childDocuments.add(childDoc);
    }

    for (Document childDoc : childDocuments) {
      indexWriter.addDocument(childDoc);
    }
    indexWriter.addDocument(parentDoc);
    indexWriter.commit();

    directoryReader = DirectoryReader.open(directory);
    assertEquals(
        "Should have correct number of documents", totalDocs + 1, directoryReader.numDocs());

    for (int leafIndex = 0; leafIndex < directoryReader.leaves().size(); leafIndex++) {
      LeafReaderContext leafContext = directoryReader.leaves().get(leafIndex);
      verifyOffsetFieldsInLeaf(leafContext, totalDocs);
    }
  }

  @Test
  public void testSingleNestedDocumentOffset() throws Exception {
    Document parentDoc = new Document();
    parentDoc.add(new StringField("id", "parent1", Field.Store.YES));

    Document childDoc = new Document();
    childDoc.add(new StringField("name", "SingleChild", Field.Store.YES));
    // For single child document, offset should be 1 (1 - 0)
    childDoc.add(new NumericDocValuesField(IndexState.NESTED_DOCUMENT_OFFSET, 1));
    childDoc.add(new StringField(IndexState.NESTED_PATH, "pickup_partners", Field.Store.YES));

    indexWriter.addDocument(childDoc);
    indexWriter.addDocument(parentDoc);
    indexWriter.commit();

    directoryReader = DirectoryReader.open(directory);

    LeafReaderContext leafContext = directoryReader.leaves().get(0);
    NumericDocValues offsetDocValues =
        leafContext.reader().getNumericDocValues(IndexState.NESTED_DOCUMENT_OFFSET);

    assertNotNull("Offset field should exist", offsetDocValues);

    assertTrue("Should advance to first document", offsetDocValues.advanceExact(0));
    assertEquals("Single child document should have offset of 1", 1L, offsetDocValues.longValue());
  }

  @Test
  public void testMultipleNestedDocumentsOffsetOrder() throws Exception {
    List<String> childNames = List.of("Child1", "Child2", "Child3", "Child4");
    int totalChildren = childNames.size();

    Document parentDoc = new Document();
    parentDoc.add(new StringField("id", "parent1", Field.Store.YES));

    for (int i = 0; i < totalChildren; i++) {
      Document childDoc = new Document();
      childDoc.add(new StringField("name", childNames.get(i), Field.Store.YES));

      // Offset calculation: n-i where n is total count, i is current index
      int expectedOffset = totalChildren - i;
      childDoc.add(new NumericDocValuesField(IndexState.NESTED_DOCUMENT_OFFSET, expectedOffset));
      childDoc.add(new StringField(IndexState.NESTED_PATH, "pickup_partners", Field.Store.YES));

      indexWriter.addDocument(childDoc);
    }

    indexWriter.addDocument(parentDoc);
    indexWriter.commit();

    directoryReader = DirectoryReader.open(directory);

    LeafReaderContext leafContext = directoryReader.leaves().get(0);
    NumericDocValues offsetDocValues =
        leafContext.reader().getNumericDocValues(IndexState.NESTED_DOCUMENT_OFFSET);

    assertNotNull("Offset field should exist", offsetDocValues);

    for (int docId = 0; docId < totalChildren; docId++) {
      assertTrue("Should advance to document " + docId, offsetDocValues.advanceExact(docId));
      int expectedOffset = totalChildren - docId;
      assertEquals(
          "Document " + docId + " should have correct offset",
          expectedOffset,
          offsetDocValues.longValue());
    }
  }

  @Test
  public void testOffsetFieldExistsInAllChildDocuments() throws Exception {
    List<Map<String, Object>> nestedObjects = createTestNestedObjects();
    int totalDocs = nestedObjects.size();

    Document parentDoc = new Document();
    parentDoc.add(new StringField("id", "parent1", Field.Store.YES));

    for (int i = 0; i < totalDocs; i++) {
      Document childDoc = new Document();
      Map<String, Object> nestedObj = nestedObjects.get(i);

      childDoc.add(new StringField("name", (String) nestedObj.get("name"), Field.Store.YES));
      childDoc.add(
          new NumericDocValuesField("hours", ((Number) nestedObj.get("hours")).longValue()));

      int expectedOffset = totalDocs - i;
      childDoc.add(new NumericDocValuesField(IndexState.NESTED_DOCUMENT_OFFSET, expectedOffset));
      childDoc.add(new StringField(IndexState.NESTED_PATH, "pickup_partners", Field.Store.YES));

      indexWriter.addDocument(childDoc);
    }

    indexWriter.addDocument(parentDoc);
    indexWriter.commit();

    directoryReader = DirectoryReader.open(directory);

    for (LeafReaderContext leafContext : directoryReader.leaves()) {
      NumericDocValues offsetDocValues =
          leafContext.reader().getNumericDocValues(IndexState.NESTED_DOCUMENT_OFFSET);

      if (offsetDocValues != null) {
        int docsWithOffset = 0;
        for (int docId = 0; docId < leafContext.reader().maxDoc(); docId++) {
          if (offsetDocValues.advanceExact(docId)) {
            docsWithOffset++;
            assertTrue("Offset value should be positive", offsetDocValues.longValue() > 0);
          }
        }

        // Should have offset for all child documents (excluding parent)
        assertTrue("Should have offset fields in child documents", docsWithOffset > 0);
      }
    }
  }

  @Test
  public void testParentDocumentsDoNotHaveOffsetField() throws Exception {
    // Create nested documents with parent
    List<Map<String, Object>> nestedObjects = createTestNestedObjects();
    int totalDocs = nestedObjects.size();

    Document parentDoc = new Document();
    parentDoc.add(new StringField("id", "parent1", Field.Store.YES));
    parentDoc.add(new StringField("type", "parent", Field.Store.YES));

    // Create child documents with offset fields
    for (int i = 0; i < totalDocs; i++) {
      Document childDoc = new Document();
      Map<String, Object> nestedObj = nestedObjects.get(i);

      childDoc.add(new StringField("name", (String) nestedObj.get("name"), Field.Store.YES));
      childDoc.add(new StringField("type", "child", Field.Store.YES));

      int expectedOffset = totalDocs - i;
      childDoc.add(new NumericDocValuesField(IndexState.NESTED_DOCUMENT_OFFSET, expectedOffset));
      childDoc.add(new StringField(IndexState.NESTED_PATH, "pickup_partners", Field.Store.YES));

      indexWriter.addDocument(childDoc);
    }

    indexWriter.addDocument(parentDoc);
    indexWriter.commit();

    directoryReader = DirectoryReader.open(directory);

    for (LeafReaderContext leafContext : directoryReader.leaves()) {
      NumericDocValues offsetDocValues =
          leafContext.reader().getNumericDocValues(IndexState.NESTED_DOCUMENT_OFFSET);
      StoredFields storedFields = leafContext.reader().storedFields();

      for (int docId = 0; docId < leafContext.reader().maxDoc(); docId++) {
        Document doc = storedFields.document(docId);
        String docType = doc.get("type");

        if ("parent".equals(docType)) {
          // Parent document should NOT have offset field
          if (offsetDocValues != null) {
            boolean hasOffset = offsetDocValues.advanceExact(docId);
            assertFalse("Parent document should not have offset field", hasOffset);
          }
        } else if ("child".equals(docType)) {
          // Child document SHOULD have offset field
          if (offsetDocValues != null) {
            boolean hasOffset = offsetDocValues.advanceExact(docId);
            assertTrue("Child document should have offset field", hasOffset);
            assertTrue("Child offset should be positive", offsetDocValues.longValue() > 0);
          }
        }
      }
    }
  }

  @Test
  public void testOnlyChildDocumentsHaveOffsetField() throws Exception {

    Document parent1 = new Document();
    parent1.add(new StringField("id", "parent1", Field.Store.YES));
    parent1.add(new StringField("doc_type", "parent", Field.Store.YES));

    Document child1 = new Document();
    child1.add(new StringField("name", "Child1", Field.Store.YES));
    child1.add(new StringField("doc_type", "child", Field.Store.YES));
    child1.add(new NumericDocValuesField(IndexState.NESTED_DOCUMENT_OFFSET, 1));
    child1.add(new StringField(IndexState.NESTED_PATH, "pickup_partners", Field.Store.YES));

    Document parent2 = new Document();
    parent2.add(new StringField("id", "parent2", Field.Store.YES));
    parent2.add(new StringField("doc_type", "parent", Field.Store.YES));

    Document child2a = new Document();
    child2a.add(new StringField("name", "Child2A", Field.Store.YES));
    child2a.add(new StringField("doc_type", "child", Field.Store.YES));
    child2a.add(new NumericDocValuesField(IndexState.NESTED_DOCUMENT_OFFSET, 2));
    child2a.add(new StringField(IndexState.NESTED_PATH, "pickup_partners", Field.Store.YES));

    Document child2b = new Document();
    child2b.add(new StringField("name", "Child2B", Field.Store.YES));
    child2b.add(new StringField("doc_type", "child", Field.Store.YES));
    child2b.add(new NumericDocValuesField(IndexState.NESTED_DOCUMENT_OFFSET, 1));
    child2b.add(new StringField(IndexState.NESTED_PATH, "pickup_partners", Field.Store.YES));

    indexWriter.addDocument(child1);
    indexWriter.addDocument(parent1);
    indexWriter.addDocument(child2a);
    indexWriter.addDocument(child2b);
    indexWriter.addDocument(parent2);
    indexWriter.commit();

    directoryReader = DirectoryReader.open(directory);

    int parentDocsWithOffset = 0;
    int parentDocsWithoutOffset = 0;
    int childDocsWithOffset = 0;
    int childDocsWithoutOffset = 0;

    for (LeafReaderContext leafContext : directoryReader.leaves()) {
      NumericDocValues offsetDocValues =
          leafContext.reader().getNumericDocValues(IndexState.NESTED_DOCUMENT_OFFSET);
      StoredFields storedFields = leafContext.reader().storedFields();

      for (int docId = 0; docId < leafContext.reader().maxDoc(); docId++) {
        Document doc = storedFields.document(docId);
        String docType = doc.get("doc_type");

        boolean hasOffset = offsetDocValues != null && offsetDocValues.advanceExact(docId);

        if ("parent".equals(docType)) {
          if (hasOffset) {
            parentDocsWithOffset++;
          } else {
            parentDocsWithoutOffset++;
          }
        } else if ("child".equals(docType)) {
          if (hasOffset) {
            childDocsWithOffset++;
          } else {
            childDocsWithoutOffset++;
          }
        }
      }
    }
    assertEquals("No parent documents should have offset field", 0, parentDocsWithOffset);
    assertEquals("All parent documents should be without offset field", 2, parentDocsWithoutOffset);
    assertEquals("All child documents should have offset field", 3, childDocsWithOffset);
    assertEquals("No child documents should be without offset field", 0, childDocsWithoutOffset);
  }

  // This test simulates the scenario where tryGetFromParentDocument is called
  // and verifies that parent documents themselves don't have offset values
  @Test
  public void testOffsetFieldAccessFromParentDocument() throws Exception {
    Document parentDoc = new Document();
    parentDoc.add(new StringField("id", "parent1", Field.Store.YES));
    parentDoc.add(new StringField("some_field", "parent_value", Field.Store.YES));

    Document childDoc = new Document();
    childDoc.add(new StringField("name", "Child1", Field.Store.YES));
    childDoc.add(new NumericDocValuesField(IndexState.NESTED_DOCUMENT_OFFSET, 1));
    childDoc.add(new StringField(IndexState.NESTED_PATH, "pickup_partners", Field.Store.YES));

    indexWriter.addDocument(childDoc);
    indexWriter.addDocument(parentDoc);
    indexWriter.commit();

    directoryReader = DirectoryReader.open(directory);
    LeafReaderContext leafContext = directoryReader.leaves().get(0);

    // Get offset doc values
    NumericDocValues offsetDocValues =
        leafContext.reader().getNumericDocValues(IndexState.NESTED_DOCUMENT_OFFSET);
    assertNotNull("Offset field should exist in the segment", offsetDocValues);

    // Check child document (docId 0) - should have offset
    assertTrue("Child document should have offset", offsetDocValues.advanceExact(0));
    assertEquals("Child should have offset value of 1", 1L, offsetDocValues.longValue());

    // Check parent document (docId 1) - should NOT have offset
    assertFalse("Parent document should not have offset field", offsetDocValues.advanceExact(1));
  }

  private List<Map<String, Object>> createTestNestedObjects() {
    List<Map<String, Object>> nestedObjects = new ArrayList<>();

    Map<String, Object> pickup1 = new HashMap<>();
    pickup1.put("name", "PartnerA");
    pickup1.put("hours", 8);
    nestedObjects.add(pickup1);

    Map<String, Object> pickup2 = new HashMap<>();
    pickup2.put("name", "PartnerB");
    pickup2.put("hours", 12);
    nestedObjects.add(pickup2);

    Map<String, Object> pickup3 = new HashMap<>();
    pickup3.put("name", "PartnerC");
    pickup3.put("hours", 16);
    nestedObjects.add(pickup3);

    return nestedObjects;
  }

  private void verifyOffsetFieldsInLeaf(LeafReaderContext leafContext, int expectedTotalDocs)
      throws Exception {
    NumericDocValues offsetDocValues =
        leafContext.reader().getNumericDocValues(IndexState.NESTED_DOCUMENT_OFFSET);

    if (offsetDocValues != null) {
      int docCount = 0;
      for (int docId = 0; docId < leafContext.reader().maxDoc(); docId++) {
        if (offsetDocValues.advanceExact(docId)) {
          long offsetValue = offsetDocValues.longValue();
          assertTrue("Offset should be positive", offsetValue > 0);
          assertTrue("Offset should not exceed total docs", offsetValue <= expectedTotalDocs);
          docCount++;
        }
      }

      if (docCount > 0) {
        assertEquals("Should have offset for all child documents", expectedTotalDocs, docCount);
      }
    }
  }
}
