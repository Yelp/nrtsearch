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
import java.util.function.Function;
import org.apache.lucene.index.LeafReaderContext;

/**
 * Index level class for providing access to doc values data. Provides a means to get a {@link
 * SegmentDocLookup} bound to single lucene segment.
 */
public class DocLookup {
  private final Function<String, FieldDef> fieldDefLookup;
  private final String queryNestedPath;

  public DocLookup(Function<String, FieldDef> fieldDefLookup) {
    this(fieldDefLookup, null);
  }

  public DocLookup(Function<String, FieldDef> fieldDefLookup, String queryNestedPath) {
    this.fieldDefLookup = fieldDefLookup;
    this.queryNestedPath = queryNestedPath;
  }

  /**
   * Get the doc value lookup accessor bound to the given lucene segment.
   *
   * @param context lucene segment context
   * @return lookup accessor for given segment context
   */
  public SegmentDocLookup getSegmentLookup(LeafReaderContext context) {
    return new SegmentDocLookup(fieldDefLookup, context, queryNestedPath);
  }

  /**
   * Get the field definition for the given field name.
   *
   * @param fieldName field name
   * @return field definition
   */
  public FieldDef getFieldDef(String fieldName) {
    return fieldDefLookup.apply(fieldName);
  }
}
