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
 * <p>Reads the {@code _nested_child_count} field from the parent to determine the child range 2.
 * Scans forward from {@code parentDocId - childCount} to {@code parentDocId - 1} 3. Optionally
 * filters children by their nested path using childPathBitSet 4. Loads the specified field's doc
 * values from each child 5. Exposes all child values as a flat multi-valued list
 *
 * <p>The childPathBitSet filtering is essential for indexes with multiple nested paths (e.g., both
 * "appointments" and "reviews" under the same parent). Without it, iterating through the child
 * range would collect values from children of all nested paths, not just the target path.
 */
public class ChildAggregatedDocValues extends LoadedDocValues<Object> {

  private final LeafReaderContext leafContext;
  private final IndexableFieldDef<?> childCountFieldDef;
  private final BitSet parentBitSet;
  private final BitSet childPathBitSet;
  private final boolean hasChildPathFilter;
  private final LoadedDocValues<?> childFieldDocValues;
  private int lastParentDocId = -1;
  private List<Object> values = Collections.emptyList();

  /**
   * Constructor.
   *
   * @param fieldDef the child field definition to load doc values from
   * @param leafContext the current segment context
   * @param parentBitSetProducer produces the BitSet identifying parent docs
   * @param childPathBitSetProducer produces the BitSet identifying children of the target nested
   *     path, or null if no path filtering is needed (single nested path case). Note: Lucene's
   *     {@link QueryBitSetProducer} returns null from {@code getBitSet()} when no documents match
   *     the query in a segment. When this producer is non-null but produces a null BitSet, all
   *     children are excluded (no matches in this segment).
   * @throws IOException if the BitSet cannot be loaded for this segment
   */
  public ChildAggregatedDocValues(
      IndexableFieldDef<?> fieldDef,
      LeafReaderContext leafContext,
      BitSetProducer parentBitSetProducer,
      BitSetProducer childPathBitSetProducer)
      throws IOException {
    this.leafContext = leafContext;
    this.childCountFieldDef =
        (IndexableFieldDef<?>) IndexState.getMetaField(IndexState.NESTED_CHILD_COUNT);
    this.parentBitSet = parentBitSetProducer.getBitSet(leafContext);
    this.hasChildPathFilter = childPathBitSetProducer != null;
    this.childPathBitSet =
        childPathBitSetProducer != null ? childPathBitSetProducer.getBitSet(leafContext) : null;
    this.childFieldDocValues = fieldDef.getDocValues(leafContext);
  }

  /**
   * Set the parent document ID. This triggers collection of all child doc values for this parent.
   *
   * @param parentDocId segment-relative parent document ID
   * @throws IOException if doc values cannot be loaded
   */
  @Override
  public void setDocId(int parentDocId) throws IOException {
    if (parentDocId == lastParentDocId) {
      return;
    }
    lastParentDocId = parentDocId;
    values = new ArrayList<>();

    if (parentBitSet == null || parentDocId <= 0) {
      return;
    }

    if (!parentBitSet.get(parentDocId)) {
      return;
    }

    // A child path filter was provided but produced no matches in this segment.
    // This happens when QueryBitSetProducer returns null for an empty result set.
    // In this case, no children should be included.
    if (hasChildPathFilter && childPathBitSet == null) {
      return;
    }

    LoadedDocValues<?> childCountDocValues = childCountFieldDef.getDocValues(leafContext);
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

    for (int childDocId = childStart; childDocId < parentDocId; childDocId++) {
      if (childPathBitSet != null && !childPathBitSet.get(childDocId)) {
        continue;
      }
      childFieldDocValues.setDocId(childDocId);
      for (int i = 0; i < childFieldDocValues.size(); i++) {
        values.add(childFieldDocValues.get(i));
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
        "ChildAggregatedDocValues is intended for script access, not direct field retrieval");
  }
}
