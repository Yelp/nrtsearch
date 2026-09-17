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
 * <p>1. Uses the parent BitSet to find the previous parent via {@code prevSetBit}, deriving the
 * child range as {@code [prevParent + 1, parentDocId - 1]} 2. Optionally filters children by their
 * nested path using childPathBitSet 3. Loads the specified field's doc values from each child 4.
 * Exposes all child values as a flat multi-valued list
 *
 * <p>The childPathBitSet filtering is essential for indexes with multiple nested paths (e.g., both
 * "appointments" and "reviews" under the same parent). Without it, iterating through the child
 * range would collect values from children of all nested paths, not just the target path.
 *
 * <p><b>Multi-level nesting limitation:</b> The {@code parentBitSet} supplied at construction time
 * always identifies <em>root</em> documents ({@code _nested_path = "_root"}). This means {@code
 * _CHILDREN.} aggregation correctly collects grandchild docs when called from a root document — the
 * child range spans the full block and {@code childPathBitSet} filters to the desired nested path.
 * However, calling {@code _CHILDREN.} from a mid-level parent (e.g., an order doc inside a {@code
 * queryNestedPath="orders"} search) produces incorrect results: {@code prevSetBit} finds the
 * previous <em>root</em> document rather than the previous order, so the child range extends across
 * multiple orders' items. This is a known limitation. Fix requires a context-sensitive {@code
 * parentBitSetProducer} that varies by the current document's nesting level.
 */
public class ChildAggregatedDocValues extends LoadedDocValues<Object> {

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
   *     {@link org.apache.lucene.search.join.QueryBitSetProducer} returns null from {@code
   *     getBitSet()} when no documents match the query in a segment. When this producer is non-null
   *     but produces a null BitSet, all children are excluded (no matches in this segment).
   * @throws IOException if the BitSet cannot be loaded for this segment
   */
  public ChildAggregatedDocValues(
      IndexableFieldDef<?> fieldDef,
      LeafReaderContext leafContext,
      BitSetProducer parentBitSetProducer,
      BitSetProducer childPathBitSetProducer)
      throws IOException {
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

    // Find the previous parent to determine child range. In Lucene's block join layout,
    // children are stored contiguously between their parent and the previous parent:
    //   [prevParent] [child0] [child1] ... [childN] [thisParent]
    // prevSetBit returns -1 if there is no previous parent (first parent in segment).
    int prevParent = parentBitSet.prevSetBit(parentDocId - 1);
    int firstChild = prevParent + 1;

    if (firstChild >= parentDocId) {
      return;
    }

    for (int childDocId = firstChild; childDocId < parentDocId; childDocId++) {
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
