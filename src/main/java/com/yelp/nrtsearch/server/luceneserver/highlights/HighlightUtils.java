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
import com.yelp.nrtsearch.server.grpc.Highlight.Type;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.QueryNodeMapper;
import com.yelp.nrtsearch.server.luceneserver.highlights.HighlightSettings.Builder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.search.Query;

/** Helper class to create {@link HighlightSettings} from a search request. */
public class HighlightUtils {

  private static final String[] DEFAULT_PRE_TAGS = new String[] {"<em>"};
  private static final String[] DEFAULT_POST_TAGS = new String[] {"</em>"};
  private static final int DEFAULT_FRAGMENT_SIZE = 100; // In number of characters
  private static final int DEFAULT_MAX_NUM_FRAGMENTS = 5;

  private static String DEFAULT_HIGHLIGHTER_NAME = FastVectorHighlighter.HIGHLIGHTER_NAME;
  private static final String DEFAULT_FRAGMENTER = "span";
  private static final boolean DEFAULT_SCORE_ORDERED = true;
  private static final boolean DEFAULT_FIELD_MATCH = false;
  private static final boolean DEFAULT_DISCRETE_MULTIVALUE = false;
  private static final QueryNodeMapper QUERY_NODE_MAPPER = QueryNodeMapper.getInstance();

  /**
   * Create the {@link HighlightSettings} for every field that is required to be highlighted.
   * Converts query-level and field-level settings into a single setting for every field.
   *
   * @param highlight Highlighting-related information in search request
   * @param searchQuery Compiled Lucene-level query from the search request
   * @param indexState {@link IndexState} for the index
   * @param highlighterService service to provide highlighters for each field
   * @return {@link Map} of field name to its {@link HighlightSettings}
   */
  static Map<String, HighlightSettings> createPerFieldSettings(
      Highlight highlight,
      Query searchQuery,
      IndexState indexState,
      HighlighterService highlighterService) {
    HighlightSettings globalSettings =
        createGlobalFieldSettings(indexState, searchQuery, highlight, highlighterService);
    Map<String, HighlightSettings> fieldSettings = new HashMap<>();
    Map<String, Settings> fieldSettingsFromRequest = highlight.getFieldSettingsMap();
    for (String field : highlight.getFieldsList()) {
      if (!fieldSettingsFromRequest.containsKey(field)) {
        fieldSettings.put(field, globalSettings);
      } else {
        Settings settings = fieldSettingsFromRequest.get(field);
        HighlightSettings.Builder builder =
            new Builder()
                .withHighlighter(
                    // DEFAULT in field override has a different meaning: no override
                    settings.getHighlighterType() == Type.DEFAULT
                        ? globalSettings.getHighlighter()
                        : highlighterService.getHighlighter(resolveHighlighterName(settings)))
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
                .withHighlightQuery(
                    settings.hasHighlightQuery()
                        ? QUERY_NODE_MAPPER.getQuery(settings.getHighlightQuery(), indexState)
                        : globalSettings.getHighlightQuery())
                .withScoreOrdered(
                    settings.hasScoreOrdered()
                        ? settings.getScoreOrdered().getValue()
                        : globalSettings.isScoreOrdered())
                .withFragmenter(
                    settings.hasFragmenter()
                        ? settings.getFragmenter().getValue()
                        : globalSettings.getFragmenter())
                .withFieldMatch(
                    // ignore field_match(v1) in field override
                    settings.hasFieldMatchV2()
                        ? settings.getFieldMatchV2().getValue()
                        : globalSettings.getFieldMatch())
                .withDiscreteMultivalue(
                    settings.hasDiscreteMultivalue()
                        ? settings.getDiscreteMultivalue().getValue()
                        : globalSettings.getDiscreteMultivalue());

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
      IndexState indexState,
      Query searchQuery,
      Highlight highlight,
      HighlighterService highlighterService) {
    Settings settings = highlight.getSettings();

    HighlightSettings.Builder builder = new HighlightSettings.Builder();

    builder
        .withHighlighter(highlighterService.getHighlighter(resolveHighlighterName(settings)))
        .withPreTags(
            settings.getPreTagsList().isEmpty()
                ? DEFAULT_PRE_TAGS
                : settings.getPreTagsList().toArray(new String[0]))
        .withPostTags(
            settings.getPostTagsList().isEmpty()
                ? DEFAULT_POST_TAGS
                : settings.getPostTagsList().toArray(new String[0]))
        .withScoreOrdered(
            settings.hasScoreOrdered()
                ? settings.getScoreOrdered().getValue()
                : DEFAULT_SCORE_ORDERED)
        .withFragmenter(
            settings.hasFragmenter() ? settings.getFragmenter().getValue() : DEFAULT_FRAGMENTER)
        .withFieldMatch(
            // for backward-compatibility get v1 if v2 is omitted
            settings.hasFieldMatchV2()
                ? settings.getFieldMatchV2().getValue()
                : settings.getFieldMatch())
        .withDiscreteMultivalue(
            settings.hasDiscreteMultivalue()
                ? settings.getDiscreteMultivalue().getValue()
                : DEFAULT_DISCRETE_MULTIVALUE);

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
    builder.withHighlightQuery(query);

    return builder.build();
  }

  private static String resolveHighlighterName(Settings settings) {
    switch (settings.getHighlighterType()) {
      case DEFAULT:
        /* default -> default-highlighter is only applicable in global settings.
         * In field override, we should return the one in global settings instead.
         */
        return DEFAULT_HIGHLIGHTER_NAME;
      case PLAIN:
        throw new UnsupportedOperationException("plain-highlighter is not supported yet.");
      case FAST_VECTOR:
        return FastVectorHighlighter.HIGHLIGHTER_NAME;
      case CUSTOM:
        return settings.getCustomHighlighterName();
      default:
        throw new IllegalArgumentException(
            String.format("Unknown highlighter_type [%s]", settings.getHighlighterType()));
    }
  }
}
