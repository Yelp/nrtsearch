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
package com.yelp.nrtsearch.server.luceneserver.doc;

import java.io.IOException;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;

/**
 * Static utility class that can create the appropriate {@link LoadedDocValues} implementation for a
 * given binary {@link DocValuesType}.
 */
public class DocValuesFactory {
  // This class can have no instance
  private DocValuesFactory() {}

  /**
   * Get the {@link LoadedDocValues} implementation for the given {@link DocValuesType} and bound to
   * the given segment context.
   *
   * @param name field name
   * @param docValuesType type of doc value, must be BINARY, SORTED, or SORTED_SET
   * @param context lucene segment context
   * @return doc values for field
   * @throws IOException if loading doc values fails
   */
  public static LoadedDocValues<?> getBinaryDocValues(
      String name, DocValuesType docValuesType, LeafReaderContext context) throws IOException {
    switch (docValuesType) {
      case BINARY:
        BinaryDocValues binaryDocValues = DocValues.getBinary(context.reader(), name);
        return new LoadedDocValues.SingleBinary(binaryDocValues);
      case SORTED:
        SortedDocValues sortedDocValues = DocValues.getSorted(context.reader(), name);
        return new LoadedDocValues.SingleString(sortedDocValues);
      case SORTED_SET:
        SortedSetDocValues sortedSetDocValues = DocValues.getSortedSet(context.reader(), name);
        return new LoadedDocValues.SortedStrings(sortedSetDocValues);
      default:
        throw new IllegalArgumentException(
            "Unable to load binary doc values for field: "
                + name
                + ", doc value type: "
                + docValuesType);
    }
  }
}
