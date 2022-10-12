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
import org.apache.lucene.search.vectorhighlight.FragListBuilder;
import org.apache.lucene.search.vectorhighlight.ScoreOrderFragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.SimpleFragListBuilder;
import org.apache.lucene.search.vectorhighlight.SingleFragListBuilder;

/** Handle highlights for a search query. Currently only supports fast vector highlighter. */
public class HighlightHandler {

  private static final FastVectorHighlighter FAST_VECTOR_HIGHLIGHTER = new FastVectorHighlighter();
  private static final SimpleFragListBuilder SIMPLE_FRAG_LIST_BUILDER = new SimpleFragListBuilder();
  private static final FragListBuilder SINGLE_FRAG_LIST_BUILDER = new SingleFragListBuilder();
  private static final ScoreOrderFragmentsBuilder SCORE_ORDER_FRAGMENTS_BUILDER =
      new ScoreOrderFragmentsBuilder();
  private static final DefaultEncoder DEFAULT_ENCODER = new DefaultEncoder();

  private static final HighlightHandler INSTANCE = new HighlightHandler();

  public static HighlightHandler getInstance() {
    return INSTANCE;
  }

  /**
   * Use {@link FastVectorHighlighter} instance to obtain highlighted fragments for a document.
   *
   * @param indexReader {@link IndexReader} for the index
   * @param settings {@link HighlightSettings} created from the search request
   * @param fieldName Field in document to highlight
   * @param docId Lucene document ID of the document to highlight
   * @return Array of highlight fragments
   * @throws IOException if there is a low-level IO error
   */
  public String[] getHighlights(
      IndexReader indexReader, HighlightSettings settings, String fieldName, int docId)
      throws IOException {
    FragListBuilder fragListBuilder;
    if (settings.getMaxNumFragments() == 0) {
      fragListBuilder = SINGLE_FRAG_LIST_BUILDER;
    } else {
      fragListBuilder = SIMPLE_FRAG_LIST_BUILDER;
    }
    return FAST_VECTOR_HIGHLIGHTER.getBestFragments(
        settings.getFieldQuery(),
        indexReader,
        docId,
        fieldName,
        settings.getFragmentSize(),
        settings.getMaxNumFragments(),
        fragListBuilder,
        SCORE_ORDER_FRAGMENTS_BUILDER,
        settings.getPreTags(),
        settings.getPostTags(),
        DEFAULT_ENCODER);
  }
}
