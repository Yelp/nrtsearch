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
package com.yelp.nrtsearch.server.luceneserver.field.properties;

import com.yelp.nrtsearch.server.luceneserver.search.GlobalOrdinalLookup;
import java.io.IOException;
import org.apache.lucene.index.IndexReader;

/**
 * Trait interface for {@link com.yelp.nrtsearch.server.luceneserver.field.FieldDef} types that can
 * use global ordinals for text based doc values.
 */
public interface GlobalOrdinalable {

  /**
   * Get the global ordinal lookup for a given index version. It is recommended to cache this
   * lookup, since it can be expensive to build.
   *
   * @param reader index reader
   * @return global ordinal lookup
   * @throws IOException on error building global ordinal mapping
   */
  GlobalOrdinalLookup getOrdinalLookup(IndexReader reader) throws IOException;

  /** If this field has ordinal based doc values enabled. */
  boolean usesOrdinals();

  /**
   * If the global ordinals should be computed eagerly for each new index version. Otherwise, this
   * will be done on first access. A value of 'true' will have {@link
   * #getOrdinalLookup(IndexReader)} called prior to each new searcher being opened.
   */
  boolean getEagerFieldGlobalOrdinals();
}
