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

import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.index.IndexState;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.lucene.index.LeafReaderContext;

/**
 * Class that provides a lookup interface for doc values in a single lucene segment. Values are
 * accessed through the Map interface, with each field name mapping to its {@link LoadedDocValues}.
 *
 * <p>The {@link LoadedDocValues} for each field are cached and can be reused for all subsequent
 * documents in the segment.
 */
public class SegmentDocLookup implements Map<String, LoadedDocValues<?>> {

  private final Function<String, FieldDef> fieldDefLookup;
  private final LeafReaderContext context;
  private final Map<String, LoadedDocValues<?>> loaderCache = new HashMap<>();

  private int docId = -1;

  public SegmentDocLookup(Function<String, FieldDef> fieldDefLookup, LeafReaderContext context) {
    this.fieldDefLookup = fieldDefLookup;
    this.context = context;
  }

  /**
   * Set target document id. All retrieved {@link LoadedDocValues} after this call will contain the
   * doc values for this id.
   *
   * @param docId target document id
   */
  public void setDocId(int docId) {
    this.docId = docId;
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException();
  }

  /**
   * Check if a given field name is capable of having doc values. This does not mean there is data
   * present, just that there can be.
   *
   * @param key field name
   * @return if this field may have stored doc values
   */
  @Override
  public boolean containsKey(Object key) {
    if (key == null) {
      return false;
    }
    String fieldName = key.toString();
    try {
      FieldDef field = fieldDefLookup.apply(fieldName);
      return field instanceof IndexableFieldDef && ((IndexableFieldDef<?>) field).hasDocValues();
    } catch (Exception ignored) {
      return false;
    }
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the {@link LoadedDocValues} for a given field. Creates a new instance or uses one from the
   * cache. The data is loaded for the current set document id.
   *
   * <p>If the field is not found in the current document and it is a nested document, it will
   * attempt to find the field in the parent document using the NESTED_DOCUMENT_OFFSET field.
   *
   * @param key field name
   * @return {@link LoadedDocValues} implementation for the given field
   * @throws IllegalArgumentException if the field does not support doc values, if there is a
   *     problem setting the target doc id, or if the field does not exist in the index
   * @throws NullPointerException if key is null
   */
  @Override
  public LoadedDocValues<?> get(Object key) {
    Objects.requireNonNull(key);
    String fieldName = key.toString();
    LoadedDocValues<?> docValues = loaderCache.get(fieldName);
    FieldDef fieldDef = null;
    if (docValues == null) {
      fieldDef = fieldDefLookup.apply(fieldName);
      if (fieldDef == null) {
        throw new IllegalArgumentException("Field does not exist: " + fieldName);
      }
      if (!(fieldDef instanceof IndexableFieldDef<?> indexableFieldDef)) {
        throw new IllegalArgumentException("Field cannot have doc values: " + fieldName);
      }
      try {
        docValues = indexableFieldDef.getDocValues(context);
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not get doc values for field: " + fieldName, e);
      }
      loaderCache.put(fieldName, docValues);
    }
    try {
      docValues.setDocId(docId);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Could not set doc: " + docId + ", field: " + fieldName, e);
    }
    if (docValues.isEmpty()) {
      if (fieldDef == null) {
        fieldDef = fieldDefLookup.apply(fieldName);
      }
      LoadedDocValues<?> parentDocValues = tryGetFromParentDocument(fieldName, fieldDef);
      if (parentDocValues != null) {
        return parentDocValues;
      }
    }
    return docValues;
  }

  /**
   * Attempt to retrieve the field from the parent document using NESTED_DOCUMENT_OFFSET.
   *
   * @param fieldName the name of the field to retrieve
   * @param fieldDef the definition of the field to retrieve
   * @return LoadedDocValues from parent document, or null if not found or not a nested document
   * @throws IllegalArgumentException if there are issues accessing the offset field or parent
   *     document
   */
  private LoadedDocValues<?> tryGetFromParentDocument(String fieldName, FieldDef fieldDef) {
    FieldDef offsetFieldDef;
    try {
      offsetFieldDef = IndexState.getMetaField(IndexState.NESTED_DOCUMENT_OFFSET);
    } catch (IllegalArgumentException e) {
      // This can happen if the meta field doesn't exist, which means the caller was not a nested
      // document
      return null;
    }

    if (!(offsetFieldDef instanceof IndexableFieldDef<?> offsetIndexableFieldDef)) {
      throw new IllegalArgumentException("NESTED_DOCUMENT_OFFSET field cannot have doc values");
    }

    LoadedDocValues<?> offsetDocValues;
    try {
      offsetDocValues = offsetIndexableFieldDef.getDocValues(context);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Could not get doc values for NESTED_DOCUMENT_OFFSET field", e);
    }

    try {
      offsetDocValues.setDocId(docId);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Could not set doc: " + docId + " for NESTED_DOCUMENT_OFFSET field", e);
    }

    // If there's no offset value, this is not a nested document and therefore we should terminate
    if (offsetDocValues.isEmpty()) {
      return null;
    }

    Object offsetValue = offsetDocValues.getFirst();
    int offset = ((Number) offsetValue).intValue();

    // The offset represents the exact number of documents to jump forward to reach the parent
    int parentDocId = docId + offset;
    if (!(fieldDef instanceof IndexableFieldDef<?> indexableFieldDef)) {
      throw new IllegalArgumentException("Field cannot have doc values: " + fieldName);
    }

    LoadedDocValues<?> parentDocValues;
    try {
      parentDocValues = indexableFieldDef.getDocValues(context);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Could not get doc values for parent field: " + fieldName, e);
    }

    try {
      parentDocValues.setDocId(parentDocId);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Could not set parent doc: " + parentDocId + ", field: " + fieldName, e);
    }

    if (!parentDocValues.isEmpty()) {
      return parentDocValues;
    }

    return null;
  }

  @Override
  public LoadedDocValues<?> put(String key, LoadedDocValues<?> value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public LoadedDocValues<?> remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends String, ? extends LoadedDocValues<?>> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> keySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<LoadedDocValues<?>> values() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Entry<String, LoadedDocValues<?>>> entrySet() {
    throw new UnsupportedOperationException();
  }
}
