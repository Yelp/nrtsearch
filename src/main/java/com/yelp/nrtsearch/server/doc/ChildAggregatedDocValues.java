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
package com.yelp.nrtsearch.server.doc;

import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.index.IndexState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;

/**
 * LoadedDocValues implementation that collects doc values from all child documents belonging to a
 * parent document. When setDocId is called with a parent doc ID, this class:
 *
 * <p>1. Reads the {@code _nested_child_count} field from the parent to determine the child range 2.
 * Scans forward from {@code parentDocId - childCount} to {@code parentDocId - 1} 3. Optionally
 * filters children by their nested path using childPathBitSet 4. Loads the specified field's doc
 * values from each child 5. Exposes all child values as a flat multi-valued list
 *
 * <p>The forward scan is required because Lucene's doc values iterators only support forward
 * iteration. The {@code _nested_child_count} field on the parent document tells us exactly where
 * the children start, avoiding the need for a backward scan.
 *
 * <p>The childPathBitSet filtering is essential for indexes with multiple nested paths (e.g., both
 * "appointments" and "reviews" under the same parent). Without it, iterating through the child
 * range would collect values from children of all nested paths, not just the target path.
 *
 * <p>This enables scripts and scorers running on parent documents to access aggregated child
 * features (e.g., min price across appointments).
 */
public class ChildAggregatedDocValues extends LoadedDocValues<Object> {

  private final IndexableFieldDef<?> fieldDef;
  private final LeafReaderContext leafContext;
  private final BitSet parentBitSet;
  private final BitSet childPathBitSet;
  private final LoadedDocValues<?> childCountDocValues;
  private List<Object> values = Collections.emptyList();

  /**
   * Constructor.
   *
   * @param fieldDef the child field definition to load doc values from
   * @param leafContext the current segment context
   * @param parentBitSetProducer produces the BitSet identifying parent docs
   * @param childPathBitSetProducer produces the BitSet identifying children of the target nested
   *     path, or null if no path filtering is needed (single nested path case)
   * @throws IOException if the BitSet cannot be loaded for this segment
   */
  public ChildAggregatedDocValues(
      IndexableFieldDef<?> fieldDef,
      LeafReaderContext leafContext,
      BitSetProducer parentBitSetProducer,
      BitSetProducer childPathBitSetProducer)
      throws IOException {
    this.fieldDef = fieldDef;
    this.leafContext = leafContext;
    this.parentBitSet = parentBitSetProducer.getBitSet(leafContext);
    this.childPathBitSet =
        childPathBitSetProducer != null ? childPathBitSetProducer.getBitSet(leafContext) : null;
    // Load the child count doc values for this segment once
    IndexableFieldDef<?> childCountFieldDef =
        (IndexableFieldDef<?>) IndexState.getMetaField(IndexState.NESTED_CHILD_COUNT);
    this.childCountDocValues = childCountFieldDef.getDocValues(leafContext);
  }

  /**
   * Set the parent document ID. This triggers collection of all child doc values for this parent.
   *
   * @param parentDocId segment-relative parent document ID
   * @throws IOException if doc values cannot be loaded
   */
  @Override
  public void setDocId(int parentDocId) throws IOException {
    values = new ArrayList<>();

    if (parentBitSet == null || parentDocId <= 0) {
      return;
    }

    if (!parentBitSet.get(parentDocId)) {
      return;
    }

    // Read the child count from the parent document
    childCountDocValues.setDocId(parentDocId);
    if (childCountDocValues.isEmpty()) {
      return;
    }
    int childCount = ((Number) childCountDocValues.get(0)).intValue();
    if (childCount <= 0) {
      return;
    }

    // Children are stored in the range [parentDocId - childCount, parentDocId - 1]
    // in block join layout. Scan forward so Lucene's doc values iterators
    // (which only support forward access) work correctly.
    int childStart = parentDocId - childCount;

    // Get a fresh doc values instance for child traversal.
    // We need our own instance because the caller may also be reading
    // the same field's doc values at the parent level.
    LoadedDocValues<?> childDocValues = fieldDef.getDocValues(leafContext);

    for (int childDocId = childStart; childDocId < parentDocId; childDocId++) {
      // Skip children that don't belong to our target nested path.
      // This is essential when the parent has multiple nested paths
      // (e.g., both "appointments" and "reviews"). Without this check,
      // we would incorrectly collect values from all child types.
      if (childPathBitSet != null && !childPathBitSet.get(childDocId)) {
        continue;
      }

      childDocValues.setDocId(childDocId);
      for (int i = 0; i < childDocValues.size(); i++) {
        values.add(childDocValues.get(i));
      }
    }
  }

  @Override
  public Object get(int index) {
    return values.get(index);
  }

  @Override
  public int size() {
    return values.size();
  }

  /**
   * Convert a value at the given index to a FieldValue proto for response serialization. Delegates
   * to the underlying field definition's conversion.
   */
  @Override
  public com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue toFieldValue(int index) {
    throw new UnsupportedOperationException(
        "ChildAggregatedDocValues is intended for script access, " + "not direct field retrieval");
  }
}
