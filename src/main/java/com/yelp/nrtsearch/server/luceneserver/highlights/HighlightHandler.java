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

import com.yelp.nrtsearch.server.grpc.Highlight;
import com.yelp.nrtsearch.server.grpc.Highlight.Settings;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.QueryNodeMapper;
import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.search.vectorhighlight.FieldQuery;
import org.apache.lucene.search.vectorhighlight.ScoreOrderFragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.SimpleFragListBuilder;

public class HighlightHandler {

  private static final String[] DEFAULT_PRE_TAGS = new String[] {"<em>"};
  private static final String[] DEFAULT_POST_TAGS = new String[] {"</em>"};
  private static final int DEFAULT_FRAGMENT_SIZE = 100; // In number of characters
  private static final FastVectorHighlighter FAST_VECTOR_HIGHLIGHTER = new FastVectorHighlighter();
  private static final SimpleFragListBuilder SIMPLE_FRAG_LIST_BUILDER = new SimpleFragListBuilder();
  private static final ScoreOrderFragmentsBuilder SCORE_ORDER_FRAGMENTS_BUILDER =
      new ScoreOrderFragmentsBuilder();
  private static final DefaultEncoder DEFAULT_ENCODER = new DefaultEncoder();
  QueryNodeMapper queryNodeMapper = new QueryNodeMapper();

  public String[] getHighlights(
      IndexState indexState,
      IndexReader reader,
      Query searchQuery,
      Highlight highlight,
      String fieldName,
      int docId)
      throws IOException {
    Query query =
        highlight.hasHighlightQuery()
            ? queryNodeMapper.getQuery(highlight.getHighlightQuery(), indexState)
            : searchQuery;
    FieldQuery fieldQuery = FAST_VECTOR_HIGHLIGHTER.getFieldQuery(query, reader);
    String[] preTags =
        highlight.getPreTagsList().isEmpty()
            ? DEFAULT_PRE_TAGS
            : highlight.getPreTagsList().toArray(new String[0]);
    String[] postTags =
        highlight.getPostTagsList().isEmpty()
            ? DEFAULT_POST_TAGS
            : highlight.getPostTagsList().toArray(new String[0]);
    ;
    Settings settings = highlight.getFieldsMap().get(fieldName);
    int fragmentSize =
        settings.hasFragmentSize() ? settings.getFragmentSize().getValue() : DEFAULT_FRAGMENT_SIZE;
    int maxNumFragments = settings.getMaxNumberOfFragments();

    if (maxNumFragments == 0) {
      String frag =
          FAST_VECTOR_HIGHLIGHTER.getBestFragment(
              fieldQuery,
              reader,
              docId,
              fieldName,
              fragmentSize,
              SIMPLE_FRAG_LIST_BUILDER,
              SCORE_ORDER_FRAGMENTS_BUILDER,
              preTags,
              postTags,
              DEFAULT_ENCODER);
      return new String[] {frag};
    } else {
      return FAST_VECTOR_HIGHLIGHTER.getBestFragments(
          fieldQuery,
          reader,
          docId,
          fieldName,
          fragmentSize,
          maxNumFragments,
          SIMPLE_FRAG_LIST_BUILDER,
          SCORE_ORDER_FRAGMENTS_BUILDER,
          preTags,
          postTags,
          DEFAULT_ENCODER);
    }
  }
}
