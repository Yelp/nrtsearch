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
package com.yelp.nrtsearch.server.luceneserver.suggest.iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.suggest.DocumentDictionary;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

public class SuggestDocumentDictionary extends DocumentDictionary {

  /** Field to read suggest texts from */
  private final String searchTextsField;

  private final String field;
  private final String weightField;

  /** Extend the functionality of DocumentDictionary with extra support to field `Search Text`. */
  public SuggestDocumentDictionary(
      IndexReader reader,
      String field,
      String weightField,
      String payloadField,
      String contextsField,
      String searchTextsField)
      throws IOException {
    super(reader, field, weightField, payloadField, contextsField);
    this.searchTextsField = searchTextsField;
    this.field = field;
    this.weightField = weightField;
  }

  @Override
  public InputIterator getEntryIterator() throws IOException {
    return new SuggestDocumentInputIterator(
        payloadField != null, contextsField != null, searchTextsField != null);
  }

  /** Implements SuggestInputIterator interface from stored fields. */
  protected class SuggestDocumentInputIterator implements SuggestInputIterator {

    private final boolean hasPayloads;
    private final boolean hasContexts;
    private final boolean hasSearchTexts;

    private final int docCount;
    private final Set<String> relevantFields;

    private final Bits liveDocs;
    private int currentDocId = -1;
    private long currentWeight = 0;
    private BytesRef currentPayload = null;
    private Set<BytesRef> currentContexts;
    private Set<BytesRef> currentSearchTexts;
    private final NumericDocValues weightValues;
    IndexableField[] currentDocFields = new IndexableField[0];
    int nextFieldsPosition = 0;

    /**
     * Creates an iterator over term, weight, payload, context, and search text fields from the
     * lucene index.
     */
    public SuggestDocumentInputIterator(
        boolean hasPayloads, boolean hasContexts, boolean hasSearchTexts) throws IOException {
      this.hasPayloads = hasPayloads;
      this.hasContexts = hasContexts;
      this.hasSearchTexts = hasSearchTexts;

      docCount = reader.maxDoc() - 1;
      weightValues =
          (weightField != null) ? MultiDocValues.getNumericValues(reader, weightField) : null;
      liveDocs = (reader.leaves().size() > 0) ? MultiBits.getLiveDocs(reader) : null;
      relevantFields =
          getRelevantFields(field, weightField, payloadField, contextsField, searchTextsField);
    }

    @Override
    public long weight() {
      return currentWeight;
    }

    @Override
    public BytesRef next() throws IOException {
      BytesRef tempPayload;
      Set<BytesRef> tempContexts;
      Set<BytesRef> tempSearchTexts;

      while (true) {
        if (nextFieldsPosition < currentDocFields.length) {
          // Still values left from the document
          IndexableField fieldValue = currentDocFields[nextFieldsPosition++];
          if (fieldValue.binaryValue() != null) {
            return fieldValue.binaryValue();
          } else if (fieldValue.stringValue() != null) {
            return new BytesRef(fieldValue.stringValue());
          } else {
            continue;
          }
        }

        if (currentDocId == docCount) {
          // Iterated over all the documents.
          break;
        }

        currentDocId++;
        if (liveDocs != null && !liveDocs.get(currentDocId)) {
          continue;
        }

        Document doc = reader.document(currentDocId, relevantFields);

        tempPayload = null;
        if (hasPayloads) {
          IndexableField payload = doc.getField(payloadField);
          if (payload != null) {
            if (payload.binaryValue() != null) {
              tempPayload = payload.binaryValue();
            } else if (payload.stringValue() != null) {
              tempPayload = new BytesRef(payload.stringValue());
            }
          }
          // in case that the iterator has payloads configured, use empty values
          // instead of null for payload
          if (tempPayload == null) {
            tempPayload = new BytesRef();
          }
        }

        tempContexts = getBytesRefSet(doc, hasContexts, contextsField);
        tempSearchTexts = getBytesRefSet(doc, hasSearchTexts, searchTextsField);

        currentDocFields = doc.getFields(field);
        nextFieldsPosition = 0;
        if (currentDocFields.length == 0) { // no values in this document
          continue;
        }
        IndexableField fieldValue = currentDocFields[nextFieldsPosition++];
        BytesRef tempTerm;
        if (fieldValue.binaryValue() != null) {
          tempTerm = fieldValue.binaryValue();
        } else if (fieldValue.stringValue() != null) {
          tempTerm = new BytesRef(fieldValue.stringValue());
        } else {
          continue;
        }

        currentPayload = tempPayload;
        currentContexts = tempContexts;
        currentSearchTexts = tempSearchTexts;
        currentWeight = getWeight(doc, currentDocId);

        return tempTerm;
      }

      return null;
    }

    @Override
    public BytesRef payload() {
      return currentPayload;
    }

    @Override
    public boolean hasPayloads() {
      return hasPayloads;
    }

    @Override
    public Set<BytesRef> contexts() {
      if (hasContexts) {
        return currentContexts;
      }
      return null;
    }

    @Override
    public boolean hasContexts() {
      return hasContexts;
    }

    @Override
    public boolean hasSearchTexts() {
      return hasSearchTexts;
    }

    @Override
    public Set<BytesRef> searchTexts() {
      if (hasSearchTexts) {
        return currentSearchTexts;
      }
      return null;
    }

    /**
     * Returns the value of the <code>weightField</code> for the current document. Retrieves the
     * value for the <code>weightField</code> if it's stored (using <code>doc</code>) or if it's
     * indexed as {@link NumericDocValues} (using <code>docId</code>) for the document. If no value
     * is found, then the weight is 0.
     *
     * <p>Note that this function is identical to getWeight in DocumentInputIterator
     */
    protected long getWeight(Document doc, int docId) throws IOException {
      IndexableField weight = doc.getField(weightField);
      if (weight != null) { // found weight as stored
        return (weight.numericValue() != null) ? weight.numericValue().longValue() : 0;
      } else if (weightValues != null) { // found weight as NumericDocValue
        if (weightValues.docID() < docId) {
          weightValues.advance(docId);
        }
        if (weightValues.docID() == docId) {
          return weightValues.longValue();
        } else {
          // missing
          return 0;
        }
      } else { // fall back
        return 0;
      }
    }

    private Set<BytesRef> getBytesRefSet(Document doc, boolean isAvailable, String fieldName) {
      Set<BytesRef> tempSet;
      if (isAvailable) {
        tempSet = new HashSet<>();
        final IndexableField[] fields = doc.getFields(fieldName);
        for (IndexableField field : fields) {
          if (field.binaryValue() != null) {
            tempSet.add(field.binaryValue());
          } else if (field.stringValue() != null) {
            tempSet.add(new BytesRef(field.stringValue()));
          } else {
            continue;
          }
        }
      } else {
        tempSet = Collections.emptySet();
      }

      return tempSet;
    }

    private Set<String> getRelevantFields(String... fields) {
      Set<String> relevantFields = new HashSet<>();
      for (String relevantField : fields) {
        if (relevantField != null) {
          relevantFields.add(relevantField);
        }
      }
      return relevantFields;
    }
  }
}
