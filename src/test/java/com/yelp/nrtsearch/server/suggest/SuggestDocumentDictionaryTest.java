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
package com.yelp.nrtsearch.server.suggest;

import com.yelp.nrtsearch.server.luceneserver.suggest.iterator.SuggestDocumentDictionary;
import com.yelp.nrtsearch.server.luceneserver.suggest.iterator.SuggestInputIterator;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

public class SuggestDocumentDictionaryTest extends LuceneTestCase {

  private static final String FIELD_NAME = "suggestText";
  private static final String WEIGHT_FIELD_NAME = "weight";
  private static final String PAYLOAD_FIELD_NAME = "payload";
  private static final String CONTEXT_FIELD_NAME = "context";
  private static final String SEARCH_TEXT_FILED_NAME = "searchText";

  @Test
  public void testDictionaryWithWeightPayloadContextSearchText() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    Document doc1 = new Document();
    doc1.add(new TextField(FIELD_NAME, "food court", Field.Store.YES));
    doc1.add(new NumericDocValuesField(WEIGHT_FIELD_NAME, 1));
    doc1.add(new StoredField(PAYLOAD_FIELD_NAME, new BytesRef("payload1")));
    doc1.add(new StoredField(CONTEXT_FIELD_NAME, new BytesRef("context1")));
    doc1.add(new TextField(SEARCH_TEXT_FILED_NAME, "food court", Field.Store.YES));
    doc1.add(new TextField(SEARCH_TEXT_FILED_NAME, "court", Field.Store.YES));

    Document doc2 = new Document();
    doc2.add(new TextField(FIELD_NAME, "food truck", Field.Store.YES));
    doc2.add(new NumericDocValuesField(WEIGHT_FIELD_NAME, 1));
    doc2.add(new StoredField(PAYLOAD_FIELD_NAME, new BytesRef("payload2")));
    doc2.add(new StoredField(CONTEXT_FIELD_NAME, new BytesRef("context2")));
    doc2.add(new StoredField(CONTEXT_FIELD_NAME, new BytesRef("context3")));
    doc2.add(new TextField(SEARCH_TEXT_FILED_NAME, "food truck", Field.Store.YES));
    doc2.add(new TextField(SEARCH_TEXT_FILED_NAME, "truck", Field.Store.YES));

    Map<String, Document> docMap = new HashMap<>(Map.of("food court", doc1, "food truck", doc2));

    for (Document doc : docMap.values()) {
      writer.addDocument(doc);
    }
    writer.commit();
    writer.close();
    IndexReader ir = DirectoryReader.open(dir);
    Dictionary dictionary =
        new SuggestDocumentDictionary(
            ir,
            FIELD_NAME,
            WEIGHT_FIELD_NAME,
            PAYLOAD_FIELD_NAME,
            CONTEXT_FIELD_NAME,
            SEARCH_TEXT_FILED_NAME);

    InputIterator inputIterator = dictionary.getEntryIterator();

    assertTrue(inputIterator instanceof SuggestInputIterator);
    SuggestInputIterator iter = (SuggestInputIterator) inputIterator;
    BytesRef f;

    while ((f = iter.next()) != null) {
      Document doc = docMap.remove(f.utf8ToString());

      // Compare FIELD field
      assertEquals(f, new BytesRef(doc.get(FIELD_NAME)));

      // Compare WEIGHT field
      IndexableField weightField = doc.getField(WEIGHT_FIELD_NAME);
      assertEquals(iter.weight(), weightField.numericValue().longValue());

      // Compare PAYLOAD field
      IndexableField payloadField = doc.getField(PAYLOAD_FIELD_NAME);
      assertEquals(iter.payload(), payloadField.binaryValue());

      // Compare CONTEXT field
      IndexableField[] contextFields = doc.getFields(CONTEXT_FIELD_NAME);
      assertEquals(iter.contexts().size(), contextFields.length);
      Set<String> expectedContexts =
          Arrays.stream(contextFields)
              .map(IndexableField::binaryValue)
              .map(BytesRef::utf8ToString)
              .collect(Collectors.toSet());
      for (BytesRef context : iter.contexts()) {
        assertTrue(expectedContexts.contains(context.utf8ToString()));
      }

      // Compare SEARCH_TEXT field
      IndexableField[] searchTextFields = doc.getFields(SEARCH_TEXT_FILED_NAME);
      assertEquals(iter.searchTexts().size(), searchTextFields.length);
      Set<String> expectedSearchTexts =
          Arrays.stream(searchTextFields)
              .map(IndexableField::stringValue)
              .collect(Collectors.toSet());
      for (BytesRef searchText : iter.searchTexts()) {
        assertTrue(expectedSearchTexts.contains(searchText.utf8ToString()));
      }
    }

    // All documents have been examined.
    assertTrue(docMap.isEmpty());
    IOUtils.close(ir, analyzer, dir);
  }
}
