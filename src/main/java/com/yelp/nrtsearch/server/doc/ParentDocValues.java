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
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue;
import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;

/**
 * LoadedDocValues wrapper that navigates from a child doc ID to the parent doc ID using the parent
 * BitSet (nextSetBit), then loads doc values from the parent document for the specified field.
 *
 * <p>In Lucene's block join layout, children are stored before their parent: [child0] [child1] ...
 * [childN] [parent]. The parent doc ID is the next set bit in the parent BitSet at or after the
 * child doc ID.
 *
 * <p>This is used for `_PARENT.field` retrieval when querying child documents via queryNestedPath.
 */
public class ParentDocValues extends LoadedDocValues<Object> {

  private final BitSet parentBitSet;
  private final LoadedDocValues<?> parentFieldDocValues;
  private int lastChildDocId = -1;

  /**
   * Constructor.
   *
   * @param fieldDef the parent field definition to load doc values from
   * @param leafContext the current segment context
   * @param parentBitSetProducer produces the BitSet identifying parent docs
   * @throws IOException if the BitSet cannot be loaded for this segment
   */
  public ParentDocValues(
      IndexableFieldDef<?> fieldDef,
      LeafReaderContext leafContext,
      BitSetProducer parentBitSetProducer)
      throws IOException {
    this.parentBitSet = parentBitSetProducer.getBitSet(leafContext);
    this.parentFieldDocValues = fieldDef.getDocValues(leafContext);
  }

  /**
   * Set the child document ID. Navigates to the parent using nextSetBit and loads parent doc
   * values.
   *
   * @param childDocId segment-relative child document ID
   * @throws IOException if doc values cannot be loaded
   */
  @Override
  public void setDocId(int childDocId) throws IOException {
    if (childDocId == lastChildDocId) {
      return;
    }
    lastChildDocId = childDocId;

    if (parentBitSet == null) {
      return;
    }

    // In block-join layout, the parent is the next set bit at or after the child
    int parentDocId = parentBitSet.nextSetBit(childDocId);
    if (parentDocId == DocIdSetIterator.NO_MORE_DOCS) {
      return;
    }
    parentFieldDocValues.setDocId(parentDocId);
  }

  @Override
  public Object get(int index) {
    return parentFieldDocValues.get(index);
  }

  @Override
  public int size() {
    if (parentBitSet == null) {
      return 0;
    }
    return parentFieldDocValues.size();
  }

  @Override
  public FieldValue toFieldValue(int index) {
    return parentFieldDocValues.toFieldValue(index);
  }
}
