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
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.IndexReader;

public interface Highlighter {
  String getName();

  String[] getHighlights(
      IndexReader indexReader,
      HighlightSettings settings,
      TextBaseFieldDef textBaseFieldDef,
      int docId,
      Map<String, Object> cache)
      throws IOException;

  default void verifyTheSpecificHighlighter(IndexReader indexReader, TextBaseFieldDef fieldDef) {};

  default boolean needCache() {
    return false;
  }
}
