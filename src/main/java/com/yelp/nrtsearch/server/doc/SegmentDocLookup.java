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
  private final String queryNestedPath;

  private int docId = -1;
  private SegmentDocLookup parentLookup = null; // Lazy initialized

  public SegmentDocLookup(Function<String, FieldDef> fieldDefLookup, LeafReaderContext context) {
    this(fieldDefLookup, context, null);
  }

  public SegmentDocLookup(
      Function<String, FieldDef> fieldDefLookup,
      LeafReaderContext context,
      String queryNestedPath) {
    this.fieldDefLookup = fieldDefLookup;
    this.context = context;
    this.queryNestedPath = queryNestedPath;
  }

  /**
   * Set target document id. All retrieved {@link LoadedDocValues} after this call will contain the
   * doc values for this id.
   *
   * @param docId target document id
   */
  public void setDocId(int docId) {
    this.docId = docId;
    // Reset parent lookup when document changes
    this.parentLookup = null;
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
   * <p>The system automatically determines if a field requires parent document access based on the
   * current nested path. Fields are resolved automatically without requiring explicit notation.
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

    if (queryNestedPath != null && !queryNestedPath.isEmpty() && !"_root".equals(queryNestedPath)) {
      int parentLevels = resolveParentLevels(queryNestedPath, fieldName);
      if (parentLevels > 0) {
        SegmentDocLookup currentLookup = getParentLookup();
        if (currentLookup == null) {
          throw new IllegalArgumentException(
              "Could not access parent field: "
                  + fieldName
                  + " (document may not be nested or parent field may not exist)");
        }

        for (int i = 1; i < parentLevels; i++) {
          currentLookup = currentLookup.getParentLookup();
          if (currentLookup == null) {
            throw new IllegalArgumentException(
                "Could not access field: " + fieldName + " (required parent level not accessible)");
          }
        }

        return currentLookup.get(fieldName);
      }
    }

    LoadedDocValues<?> docValues = loaderCache.get(fieldName);
    if (docValues == null) {
      FieldDef fieldDef = fieldDefLookup.apply(fieldName);
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

    return docValues;
  }

  /**
   * Lazily initializes and returns the parent document lookup. Returns null if this document is not
   * nested or if parent document cannot be accessed.
   *
   * @return SegmentDocLookup for parent document, or null if not nested
   */
  private SegmentDocLookup getParentLookup() {
    if (parentLookup == null) {
      int parentDocId = getParentDocId();
      if (parentDocId == -1) {
        return null; // Not a nested document or parent not found
      }
      parentLookup = new SegmentDocLookup(fieldDefLookup, context);
      parentLookup.setDocId(parentDocId);
    }
    return parentLookup;
  }

  /**
   * Calculates the parent document ID using NESTED_DOCUMENT_OFFSET.
   *
   * @return parent document ID, or -1 if not found or not a nested document
   */
  private int getParentDocId() {
    FieldDef offsetFieldDef;
    try {
      offsetFieldDef = IndexState.getMetaField(IndexState.NESTED_DOCUMENT_OFFSET);
    } catch (IllegalArgumentException e) {
      return -1;
    }

    if (!(offsetFieldDef instanceof IndexableFieldDef<?> offsetIndexableFieldDef)) {
      return -1;
    }

    LoadedDocValues<?> offsetDocValues;
    try {
      offsetDocValues = offsetIndexableFieldDef.getDocValues(context);
      offsetDocValues.setDocId(docId);
    } catch (IOException e) {
      return -1;
    }

    if (offsetDocValues.isEmpty()) {
      return -1;
    }

    Object offsetValue = offsetDocValues.getFirst();
    int offset = ((Number) offsetValue).intValue();
    // The offset represents the exact number of documents to jump forward to reach the parent
    return docId + offset;
  }

  /**
   * Utility method to resolve the relative path from current nested location to target field.
   * Returns the number of parent levels to traverse to access the field.
   *
   * @param currentNestedPath current nested document path (e.g., "reviews.generation")
   * @param targetFieldPath absolute field path (e.g., "biz_name" or "reviews.rating")
   * @return number of parent levels to traverse, or -1 if field is in current or child level
   */
  private static int resolveParentLevels(String currentNestedPath, String targetFieldPath) {
    if (currentNestedPath == null
        || currentNestedPath.isEmpty()
        || "_root".equals(currentNestedPath)) {
      return -1; // Field is at current level or below
    }

    if (targetFieldPath.startsWith(currentNestedPath + ".")
        || targetFieldPath.equals(currentNestedPath)) {
      return -1; // Field is at current level or below
    }

    String[] currentPathParts = currentNestedPath.split("\\.");
    String[] targetPathParts = targetFieldPath.split("\\.");

    // Find common prefix
    int commonPrefixLength = 0;
    int minLength = Math.min(currentPathParts.length, targetPathParts.length);
    for (int i = 0; i < minLength; i++) {
      if (currentPathParts[i].equals(targetPathParts[i])) {
        commonPrefixLength++;
      } else {
        break;
      }
    }

    int levelsUp = currentPathParts.length - commonPrefixLength;

    // If we need to go up to access the field, return the number of levels
    // If levelsUp is 0, it means the field is at the same level or below
    return levelsUp > 0 ? levelsUp : -1;
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
