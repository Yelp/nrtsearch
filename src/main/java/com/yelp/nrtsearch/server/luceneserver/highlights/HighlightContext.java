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

import static com.yelp.nrtsearch.server.luceneserver.highlights.HighlightHandler.FAST_VECTOR_HIGHLIGHTER;

import com.yelp.nrtsearch.server.grpc.Highlight;
import com.yelp.nrtsearch.server.grpc.Highlight.Settings;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.QueryNodeMapper;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.vectorhighlight.FieldQuery;

public class HighlightContext {

  private static final String[] DEFAULT_PRE_TAGS = new String[] {"<em>"};
  private static final String[] DEFAULT_POST_TAGS = new String[] {"</em>"};

  private static final int DEFAULT_FRAGMENT_SIZE = 100; // In number of characters
  private static final QueryNodeMapper QUERY_NODE_MAPPER = QueryNodeMapper.getInstance();

  static class FieldSettings {
    private final int fragmentSize;
    private final int maxNumFragments;

    public FieldSettings(Settings settings) {
      this.fragmentSize =
          settings.hasFragmentSize()
              ? settings.getFragmentSize().getValue()
              : DEFAULT_FRAGMENT_SIZE;
      ;
      this.maxNumFragments = settings.getMaxNumberOfFragments();
    }

    public int getFragmentSize() {
      return fragmentSize;
    }

    public int getMaxNumFragments() {
      return maxNumFragments;
    }
  }

  private final IndexReader indexReader;
  private final String[] preTags;
  private final String[] postTags;
  private final FieldQuery fieldQuery;
  private final Map<String, FieldSettings> fieldSettings;

  public HighlightContext(
      IndexState indexState,
      SearcherAndTaxonomy searcherAndTaxonomy,
      Query searchQuery,
      Highlight highlight)
      throws IOException {
    indexReader = searcherAndTaxonomy.searcher.getIndexReader();

    preTags =
        highlight.getPreTagsList().isEmpty()
            ? DEFAULT_PRE_TAGS
            : highlight.getPreTagsList().toArray(new String[0]);
    postTags =
        highlight.getPostTagsList().isEmpty()
            ? DEFAULT_POST_TAGS
            : highlight.getPostTagsList().toArray(new String[0]);

    Query query =
        highlight.hasHighlightQuery()
            ? QUERY_NODE_MAPPER.getQuery(highlight.getHighlightQuery(), indexState)
            : searchQuery;
    fieldQuery = FAST_VECTOR_HIGHLIGHTER.getFieldQuery(query, indexReader);

    fieldSettings =
        highlight.getFieldsMap().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Entry::getKey,
                    fieldNameToSettings -> new FieldSettings(fieldNameToSettings.getValue())));
  }

  public IndexReader getIndexReader() {
    return indexReader;
  }

  public String[] getPreTags() {
    return preTags;
  }

  public String[] getPostTags() {
    return postTags;
  }

  public FieldQuery getFieldQuery() {
    return fieldQuery;
  }

  public Map<String, FieldSettings> getFieldSettings() {
    return fieldSettings;
  }
}
