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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;

/**
 * LoadedDocValues implementation that collects doc values from all child documents belonging to a
 * parent document. When setDocId is called with a parent doc ID, this class:
 *
 * <p>1. Determines the nesting level of the current document by checking which level's BitSet
 * contains it. 2. Uses that level's BitSet to find the previous sibling parent via {@code
 * prevSetBit}, deriving the child range as {@code [prevParent + 1, parentDocId - 1]}. 3. Optionally
 * filters children by their nested path using childPathBitSet. 4. Loads the specified field's doc
 * values from each matching child. 5. Exposes all child values as a flat multi-valued list.
 *
 * <p>The level-aware parent BitSet means this class works correctly at any nesting depth: a root
 * document sees children in its full block; an order document (inside a NestedQuery or a
 * queryNestedPath="orders" search) sees only items belonging to that specific order.
 *
 * <p>The childPathBitSet filtering is essential for indexes with multiple nested paths (e.g., both
 * "appointments" and "reviews" under the same parent). Without it, iterating through the child
 * range would collect values from children of all nested paths, not just the target path.
 */
public class ChildAggregatedDocValues extends LoadedDocValues<Object> {

  private final Collection<BitSet> levelBitSets;
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
   * @param levelBitSetProducers map from nested path name to BitSetProducer for that level; must
   *     include all levels present in the index (including "_root"). The correct parent boundary is
   *     determined at {@link #setDocId} time by checking which level the current document belongs
   *     to.
   * @param childPathBitSetProducer produces the BitSet identifying children of the target nested
   *     path, or null if no path filtering is needed (single nested path case). Note: Lucene's
   *     {@link org.apache.lucene.search.join.QueryBitSetProducer} returns null from {@code
   *     getBitSet()} when no documents match the query in a segment. When this producer is non-null
   *     but produces a null BitSet, all children are excluded (no matches in this segment).
   * @throws IOException if any BitSet cannot be loaded for this segment
   */
  public ChildAggregatedDocValues(
      IndexableFieldDef<?> fieldDef,
      LeafReaderContext leafContext,
      Map<String, BitSetProducer> levelBitSetProducers,
      BitSetProducer childPathBitSetProducer)
      throws IOException {
    List<BitSet> resolved = new ArrayList<>(levelBitSetProducers.size());
    for (BitSetProducer producer : levelBitSetProducers.values()) {
      BitSet bs = producer.getBitSet(leafContext);
      if (bs != null) {
        resolved.add(bs);
      }
    }
    this.levelBitSets = resolved;
    this.hasChildPathFilter = childPathBitSetProducer != null;
    this.childPathBitSet =
        childPathBitSetProducer != null ? childPathBitSetProducer.getBitSet(leafContext) : null;
    this.childFieldDocValues = fieldDef.getDocValues(leafContext);
  }

  /**
   * Set the parent document ID. This triggers collection of all child doc values for this parent.
   *
   * <p>The parent boundary is determined dynamically: the level BitSet that contains {@code
   * parentDocId} is used to locate the previous parent, defining the child range. This correctly
   * handles root docs, mid-level docs (e.g. order docs inside a NestedQuery), and any search
   * context.
   *
   * @param parentDocId segment-relative document ID
   * @throws IOException if doc values cannot be loaded
   */
  @Override
  public void setDocId(int parentDocId) throws IOException {
    if (parentDocId == lastParentDocId) {
      return;
    }
    lastParentDocId = parentDocId;
    values = new ArrayList<>();

    if (parentDocId < 0) {
      return;
    }

    // Find which level this document belongs to. Each document appears in exactly one level's
    // BitSet. The level's BitSet defines the parent boundary for child range computation.
    BitSet currentLevelBitSet = null;
    for (BitSet bs : levelBitSets) {
      if (bs.get(parentDocId)) {
        currentLevelBitSet = bs;
        break;
      }
    }
    if (currentLevelBitSet == null) {
      return; // doc not found in any level — should not happen in a valid index
    }

    // A child path filter was provided but produced no matches in this segment.
    if (hasChildPathFilter && childPathBitSet == null) {
      return;
    }

    // Find the previous parent at the same level to determine child range.
    // In Lucene's block join layout, children are stored contiguously between parents:
    //   [prevParent] [child0] [child1] ... [childN] [thisParent]
    // prevSetBit returns -1 if there is no previous parent (first parent in segment).
    int prevParent = currentLevelBitSet.prevSetBit(parentDocId - 1);
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
