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
package com.yelp.nrtsearch.server.luceneserver.suggest.iterator;

import java.util.Set;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Interface is the child of InputIterator interface with extra support for search texts. currently
 * only CompletionInfixSuggest adopts the iterator implementing this interface.
 */
public interface SuggestInputIterator extends InputIterator {
  boolean hasSearchTexts();

  /**
   * A term's search texts are used to perform infix suggest for both exact matching and fuzzy
   * match. This field is critical in CompletionInfixSuggest, so that it can not be empty.
   */
  Set<BytesRef> searchTexts();
}
