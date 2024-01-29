/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.highlights;

import com.yelp.nrtsearch.server.luceneserver.field.TextBaseFieldDef;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;

/**
 * This is the highlighter interface to provide the new highlighters with different implementations.
 * Highlighters are supposed to be initiated once at the startup time and registered in the {@link
 * HighlighterService}. Therefore, a single instance of the highlighter will be responsible for
 * handling all the corresponding highlighting fetch tasks.
 */
public interface Highlighter {

  /**
   * The name is used as the identifier of this highlighter. It must be unique. The highlight_type
   * in the search request will be used to match this name during the highlighter selection.
   *
   * @return the unique name/identifier of this highlighter
   */
  String getName();

  /**
   * Get highlighted segments for the given leafDocId and field.
   *
   * <p>The indexReader gives the full readabity access to the highlighter. And using the leafDocId
   * and the fieldName derived from the textBasedFieldDef, the target field can be retrieved.</>
   *
   * @param hitLeaf the hitLeaf context has the random access to the segment
   * @param settings the highlight settings derived from the search request for this field
   * @param textBaseFieldDef the target's field information
   * @param leafDocId the target's identifier to retrieve the highlighting document from the
   *     leafReaderContext base (luceneId - hitLeaf.docBase)
   * @param searchContext the searchContext to keep the contexts for this search request
   * @return an array of Strings containing all highlighted fragments
   * @throws IOException will be thrown when fail during the document reading
   */
  String[] getHighlights(
      LeafReaderContext hitLeaf,
      HighlightSettings settings,
      TextBaseFieldDef textBaseFieldDef,
      int leafDocId,
      SearchContext searchContext)
      throws IOException;

  /**
   * This will be invoked once per highlighted field before execution of the search request. All
   * highlighter implementations' additional highlight-ability checks should be done here.
   *
   * <p>If failed, an {@link IllegalArgumentException} with detailed explanation must be thrown.
   * Otherwise, do nothing.
   *
   * @param fieldDef a {@link TextBaseFieldDef} object of the field that is intended to be
   *     highlighted
   */
  default void verifyFieldIsSupported(TextBaseFieldDef fieldDef) {}
}
