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
import com.yelp.nrtsearch.server.luceneserver.highlights.HighlightSettings.Builder;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.vectorhighlight.FieldQuery;

/** Helper class to create {@link HighlightSettings} from a search request. */
public class HighlightSettingsHelper {

  private static final String[] DEFAULT_PRE_TAGS = new String[] {"<em>"};
  private static final String[] DEFAULT_POST_TAGS = new String[] {"</em>"};
  private static final int DEFAULT_FRAGMENT_SIZE = 100; // In number of characters
  private static final int DEFAULT_MAX_NUM_FRAGMENTS = 5;
  private static final QueryNodeMapper QUERY_NODE_MAPPER = QueryNodeMapper.getInstance();

  /**
   * Create the {@link HighlightSettings} for every field that is required to be highlighted.
   * Converts query-level and field-level settings into a single setting for every field.
   *
   * @param indexReader {@link IndexReader} for the index
   * @param highlight Highlighting-related information in search request
   * @param searchQuery Compiled Lucene-level query from the search request
   * @param indexState {@link IndexState} for the index
   * @return {@link Map} of field name to its {@link HighlightSettings}
   * @throws IOException if there is a low-level IO exception
   */
  static Map<String, HighlightSettings> createPerFieldSettings(
      IndexReader indexReader, Highlight highlight, Query searchQuery, IndexState indexState)
      throws IOException {
    HighlightSettings globalSettings =
        createGlobalFieldSettings(indexReader, indexState, searchQuery, highlight);
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
                .withFragmentSize(
                    settings.hasFragmentSize()
                        ? settings.getFragmentSize().getValue()
                        : globalSettings.getFragmentSize())
                .withFieldQuery(
                    settings.hasHighlightQuery()
                        ? getFieldQuery(indexReader, indexState, settings)
                        : globalSettings.getFieldQuery());
        if (!settings.hasMaxNumberOfFragments()) {
          builder.withMaxNumFragments(globalSettings.getMaxNumFragments());
        } else {
          if (settings.getMaxNumberOfFragments().getValue() == 0) {
            builder.withMaxNumFragments(Integer.MAX_VALUE).withFragmentSize(Integer.MAX_VALUE);
          } else {
            builder.withMaxNumFragments(settings.getMaxNumberOfFragments().getValue());
          }
        }
        HighlightSettings highlightSettings = builder.build();
        fieldSettings.put(field, highlightSettings);
      }
    }
    return Collections.unmodifiableMap(fieldSettings);
  }

  private static HighlightSettings createGlobalFieldSettings(
      IndexReader indexReader, IndexState indexState, Query searchQuery, Highlight highlight)
      throws IOException {
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

    if (settings.hasMaxNumberOfFragments() && settings.getMaxNumberOfFragments().getValue() == 0) {
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
    builder.withFieldQuery(getFieldQuery(indexReader, query, settings.getFieldMatch()));

    return builder.build();
  }

  private static FieldQuery getFieldQuery(
      IndexReader indexReader, IndexState indexState, Settings settings) throws IOException {
    Query query = QUERY_NODE_MAPPER.getQuery(settings.getHighlightQuery(), indexState);
    return getFieldQuery(indexReader, query, settings.getFieldMatch());
  }

  private static FieldQuery getFieldQuery(IndexReader indexReader, Query query, boolean fieldMatch)
      throws IOException {
    return new FieldQuery(query, indexReader, true, fieldMatch);
  }
}
