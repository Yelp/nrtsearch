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
import com.yelp.nrtsearch.server.luceneserver.highlights.HighlightSettings.Builder;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.vectorhighlight.FieldQuery;

/** Stores information required to provide highlights. */
public class HighlightContext {

  private static final String[] DEFAULT_PRE_TAGS = new String[] {"<em>"};
  private static final String[] DEFAULT_POST_TAGS = new String[] {"</em>"};
  private static final int DEFAULT_FRAGMENT_SIZE = 100; // In number of characters
  private static final int DEFAULT_MAX_NUM_FRAGMENTS = 5;
  private static final QueryNodeMapper QUERY_NODE_MAPPER = QueryNodeMapper.getInstance();

  private final IndexReader indexReader;

  private final Map<String, HighlightSettings> fieldSettings;

  public HighlightContext(
      IndexState indexState,
      SearcherAndTaxonomy searcherAndTaxonomy,
      Query searchQuery,
      Highlight highlight)
      throws IOException {
    indexReader = searcherAndTaxonomy.searcher.getIndexReader();

    HighlightSettings globalSettings =
        createGlobalFieldSettings(indexState, searchQuery, highlight);
    fieldSettings = createPerFieldSettings(highlight, globalSettings, indexState);
  }

  private HighlightSettings createGlobalFieldSettings(
      IndexState indexState, Query searchQuery, Highlight highlight) throws IOException {
    Settings settings = highlight.getSettings();

    HighlightSettings.Builder builder = new HighlightSettings.Builder();

    builder.withPreTags(
        settings.getPreTagsList().isEmpty()
            ? DEFAULT_PRE_TAGS
            : settings.getPreTagsList().toArray(new String[0]));
    builder.withPostTags(
        settings.getPostTagsList().isEmpty()
            ? DEFAULT_POST_TAGS
            : settings.getPostTagsList().toArray(new String[0]));

    if (settings.hasFragmentSize() && settings.getFragmentSize().getValue() == 0) {
      builder.withMaxNumFragments(Integer.MAX_VALUE).withFragmentSize(Integer.MAX_VALUE);
    } else {
      builder
          .withMaxNumFragments(
              settings.hasMaxNumberOfFragments()
                  ? settings.getMaxNumberOfFragments().getValue()
                  : DEFAULT_MAX_NUM_FRAGMENTS)
          .withFragmentSize(
              settings.hasFragmentSize()
                  ? settings.getFragmentSize().getValue()
                  : DEFAULT_FRAGMENT_SIZE);
    }

    Query query =
        settings.hasHighlightQuery()
            ? QUERY_NODE_MAPPER.getQuery(settings.getHighlightQuery(), indexState)
            : searchQuery;
    builder.withFieldQuery(FAST_VECTOR_HIGHLIGHTER.getFieldQuery(query, indexReader));

    return builder.build();
  }

  private Map<String, HighlightSettings> createPerFieldSettings(
      Highlight highlight, HighlightSettings globalSettings, IndexState indexState)
      throws IOException {
    Map<String, HighlightSettings> fieldSettings = new HashMap<>();
    Map<String, Settings> fieldSettingsFromRequest = highlight.getFieldSettingsMap();
    for (String field : highlight.getFieldsList()) {
      if (!fieldSettingsFromRequest.containsKey(field)) {
        fieldSettings.put(field, globalSettings);
      } else {
        Settings settings = fieldSettingsFromRequest.get(field);
        HighlightSettings.Builder builder =
            new Builder()
                .withPreTags(
                    !settings.getPreTagsList().isEmpty()
                        ? settings.getPreTagsList().toArray(new String[0])
                        : globalSettings.getPreTags())
                .withPostTags(
                    !settings.getPostTagsList().isEmpty()
                        ? settings.getPostTagsList().toArray(new String[0])
                        : globalSettings.getPostTags())
                .withMaxNumFragments(
                    settings.hasMaxNumberOfFragments()
                        ? settings.getMaxNumberOfFragments().getValue()
                        : globalSettings.getMaxNumFragments())
                .withFieldQuery(
                    settings.hasHighlightQuery()
                        ? getFieldQuery(indexState, settings)
                        : globalSettings.getFieldQuery());
        if (!settings.hasFragmentSize()) {
          builder.withFragmentSize(globalSettings.getFragmentSize());
        } else {
          if (settings.getFragmentSize().getValue() == 0) {
            builder.withMaxNumFragments(Integer.MAX_VALUE).withFragmentSize(Integer.MAX_VALUE);
          } else {
            builder.withFragmentSize(settings.getFragmentSize().getValue());
          }
        }
        HighlightSettings highlightSettings = builder.build();
        fieldSettings.put(field, highlightSettings);
      }
    }
    return fieldSettings;
  }

  private FieldQuery getFieldQuery(IndexState indexState, Settings settings) throws IOException {
    Query query = QUERY_NODE_MAPPER.getQuery(settings.getHighlightQuery(), indexState);
    return FAST_VECTOR_HIGHLIGHTER.getFieldQuery(query, indexReader);
  }

  public IndexReader getIndexReader() {
    return indexReader;
  }

  public Map<String, HighlightSettings> getFieldSettings() {
    return fieldSettings;
  }
}
