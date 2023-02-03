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

import com.yelp.nrtsearch.server.luceneserver.field.TextBaseFieldDef;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.vectorhighlight.FieldQuery;
import org.apache.lucene.search.vectorhighlight.FragListBuilder;
import org.apache.lucene.search.vectorhighlight.FragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.ScoreOrderFragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.SimpleFragListBuilder;
import org.apache.lucene.search.vectorhighlight.SimpleFragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.SingleFragListBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The FVH highlighter class for a search query. The highlighted field must be indexed with term
 * vector with positions and offsets. And the multivalue fields are always fragmented and scored
 * discretely.
 */
public class FastVectorHighlighter implements Highlighter {

  static final String HIGHLIGHTER_NAME = "fast-vector-highlighter";
  private static final org.apache.lucene.search.vectorhighlight.FastVectorHighlighter
      FAST_VECTOR_HIGHLIGHTER =
          new org.apache.lucene.search.vectorhighlight.FastVectorHighlighter();
  private static final SimpleFragListBuilder SIMPLE_FRAG_LIST_BUILDER = new SimpleFragListBuilder();
  private static final SingleFragListBuilder SINGLE_FRAG_LIST_BUILDER = new SingleFragListBuilder();
  private static final SimpleFragmentsBuilder SIMPLE_FRAGMENTS_BUILDER =
      new SimpleFragmentsBuilder();
  private static final ScoreOrderFragmentsBuilder SCORE_ORDER_FRAGMENTS_BUILDER =
      new ScoreOrderFragmentsBuilder();
  private static final DefaultEncoder DEFAULT_ENCODER = new DefaultEncoder();

  private static final FastVectorHighlighter INSTANCE = new FastVectorHighlighter();

  private static Logger logger = LoggerFactory.getLogger(FastVectorHighlighter.class);

  public static FastVectorHighlighter getInstance() {
    return INSTANCE;
  }

  static {
    // always treat multivalue field discretely
    SIMPLE_FRAGMENTS_BUILDER.setDiscreteMultiValueHighlighting(true);
    SCORE_ORDER_FRAGMENTS_BUILDER.setDiscreteMultiValueHighlighting(true);
  }

  @Override
  public String getName() {
    return HIGHLIGHTER_NAME;
  }

  /**
   * Use {@link org.apache.lucene.search.vectorhighlight.FastVectorHighlighter} instance to obtain
   * highlighted fragments for a document.
   *
   * @param indexReader {@link IndexReader} for the index
   * @param settings {@link HighlightSettings} created from the search request
   * @param textBaseFieldDef Field in document to highlight
   * @param docId Lucene document ID of the document to highlight
   * @return Array of highlight fragments
   * @throws IOException if there is a low-level IO error
   */
  @Override
  public String[] getHighlights(
      IndexReader indexReader,
      HighlightSettings settings,
      TextBaseFieldDef textBaseFieldDef,
      int docId,
      Map<String, Object> _cache)
      throws IOException {
    FragListBuilder fragListBuilder;
    int numberOfFragments = settings.getMaxNumFragments();
    int fragmentCharSize = settings.getFragmentSize();
    if (settings.getMaxNumFragments() == 0) {
      // a HACK to make highlighter do highlighting, even though its using the single frag list
      // builder
      fragListBuilder = SINGLE_FRAG_LIST_BUILDER;
      numberOfFragments = Integer.MAX_VALUE;
      fragmentCharSize = Integer.MAX_VALUE;
    } else {
      fragListBuilder = SIMPLE_FRAG_LIST_BUILDER;
    }

    FragmentsBuilder fragmentsBuilder;
    if (settings.isScoreOrdered()) {
      fragmentsBuilder = SCORE_ORDER_FRAGMENTS_BUILDER;
    } else {
      fragmentsBuilder = SIMPLE_FRAGMENTS_BUILDER;
    }

    return FAST_VECTOR_HIGHLIGHTER.getBestFragments(
        getFieldQuery(indexReader, settings.getHighlightQuery(), settings.getFieldMatch()),
        indexReader,
        docId,
        textBaseFieldDef.getName(),
        fragmentCharSize,
        numberOfFragments,
        fragListBuilder,
        fragmentsBuilder,
        settings.getPreTags(),
        settings.getPostTags(),
        DEFAULT_ENCODER);
  }

  private static FieldQuery getFieldQuery(IndexReader indexReader, Query query, boolean fieldMatch)
      throws IOException {
    return new FieldQuery(query, indexReader, true, fieldMatch);
  }

  @Override
  public void verifyTheSpecificHighlighter(IndexReader indexReader, TextBaseFieldDef fieldDef) {
    FieldType fieldType = fieldDef.getFieldType();
    if (!fieldType.storeTermVectorPositions() || !fieldType.storeTermVectorOffsets()) {
      throw new IllegalArgumentException(
          String.format(
              "Field %s does not have term vectors with positions and offsets and cannot support highlights",
              fieldDef.getName()));
    }
  }
}
