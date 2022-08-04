/*
 * Copyright 2022 Yelp Inc.
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

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.search.vectorhighlight.ScoreOrderFragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.SimpleFragListBuilder;

/** Handle highlights for a search query. Currently only supports fast vector highlighter. */
public class HighlightHandler {

  static final FastVectorHighlighter FAST_VECTOR_HIGHLIGHTER = new FastVectorHighlighter();
  private static final SimpleFragListBuilder SIMPLE_FRAG_LIST_BUILDER = new SimpleFragListBuilder();
  private static final ScoreOrderFragmentsBuilder SCORE_ORDER_FRAGMENTS_BUILDER =
      new ScoreOrderFragmentsBuilder();
  private static final DefaultEncoder DEFAULT_ENCODER = new DefaultEncoder();

  private static final HighlightHandler INSTANCE = new HighlightHandler();

  public static HighlightHandler getInstance() {
    return INSTANCE;
  }

  public String[] getHighlights(
      IndexReader indexReader, HighlightSettings settings, String fieldName, int docId)
      throws IOException {
    return FAST_VECTOR_HIGHLIGHTER.getBestFragments(
        settings.getFieldQuery(),
        indexReader,
        docId,
        fieldName,
        settings.getFragmentSize(),
        settings.getMaxNumFragments(),
        SIMPLE_FRAG_LIST_BUILDER,
        SCORE_ORDER_FRAGMENTS_BUILDER,
        settings.getPreTags(),
        settings.getPostTags(),
        DEFAULT_ENCODER);
  }
}
