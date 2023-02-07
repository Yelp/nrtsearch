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

import com.google.protobuf.BoolValue;
import com.yelp.nrtsearch.server.grpc.Highlight;
import com.yelp.nrtsearch.server.grpc.HighlightV2;
import com.yelp.nrtsearch.server.grpc.HighlightV2.Settings;
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
  private static final String DEFAULT_FRAGMENTER = "span";
  private static final boolean DEFAULT_SCORE_ORDERED = true;
  private static final boolean DEFAULT_FIELD_MATCH = false;
  private static final boolean DEFAULT_DISCRETE_MULTIVALUE = false;
  private static final QueryNodeMapper QUERY_NODE_MAPPER = QueryNodeMapper.getInstance();

  /**
   * Create the {@link HighlightSettings} for every field that is required to be highlighted.
   * Converts query-level and field-level settings into a single setting for every field.
   *
   * @param highlightV2 Highlighting-related information in search request
   * @param searchQuery Compiled Lucene-level query from the search request
   * @param indexState {@link IndexState} for the index
   * @return {@link Map} of field name to its {@link HighlightSettings}
   */
  static Map<String, HighlightSettings> createPerFieldSettings(
      HighlightV2 highlightV2, Query searchQuery, IndexState indexState) {
    HighlightSettings globalSettings =
        createGlobalFieldSettings(indexState, searchQuery, highlightV2);
    Map<String, HighlightSettings> fieldSettings = new HashMap<>();
    Map<String, Settings> fieldSettingsFromRequest = highlightV2.getFieldSettingsMap();
    for (String field : highlightV2.getFieldsList()) {
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
                    settings.hasFieldMatch()
                        ? settings.getFieldMatch().getValue()
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
      IndexState indexState, Query searchQuery, HighlightV2 highlightV2) {
    Settings settings = highlightV2.getSettings();

    HighlightSettings.Builder builder = new HighlightSettings.Builder();

    builder
        .withHighlighterType(highlightV2.getHighlighterType())
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
            settings.hasFieldMatch() ? settings.getFieldMatch().getValue() : DEFAULT_FIELD_MATCH)
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

  private static HighlightV2.Settings adaptSettingsToV2(Highlight.Settings settings) {
    HighlightV2.Settings.Builder v2SettingsBuilder = HighlightV2.Settings.newBuilder();
    v2SettingsBuilder
        .addAllPreTags(settings.getPreTagsList())
        .addAllPostTags(settings.getPostTagsList());
    if (settings.hasHighlightQuery()) {
      v2SettingsBuilder.setHighlightQuery(settings.getHighlightQuery());
    }
    if (settings.hasFragmentSize()) {
      v2SettingsBuilder.setFragmentSize(settings.getFragmentSize());
    }
    if (settings.hasMaxNumberOfFragments()) {
      v2SettingsBuilder.setMaxNumberOfFragments(settings.getMaxNumberOfFragments());
    }
    v2SettingsBuilder.setFieldMatch(
        BoolValue.of(
            settings
                .getFieldMatch())); // v1 has field_match typed plain-boolean, so it's always set
    // false if omitted
    v2SettingsBuilder.setScoreOrdered(BoolValue.of(true)); // Always true in V1
    v2SettingsBuilder.setDiscreteMultivalue(BoolValue.of(false)); // Always false in V1
    return v2SettingsBuilder.build();
  }

  /**
   * convert the deprecated highlight V1 to V2 protobuf settings.
   *
   * @param highlight
   * @return highlightV2 representative of highlight(V1)
   */
  public static HighlightV2 adaptHighlightToV2(Highlight highlight) {
    HighlightV2.Builder v2Builder = HighlightV2.newBuilder();
    v2Builder
        .setHighlighterType("fast-vector-highlighter") // This field is ignored in v1
        .addAllFields(highlight.getFieldsList())
        .setSettings(adaptSettingsToV2(highlight.getSettings()));
    if (!highlight.getFieldSettingsMap().isEmpty()) {
      for (Map.Entry<String, Highlight.Settings> entry :
          highlight.getFieldSettingsMap().entrySet()) {
        v2Builder.putFieldSettings(entry.getKey(), adaptSettingsToV2(entry.getValue()));
      }
    }
    return v2Builder.build();
  }

  /**
   * This is an adapter method for compatibility between v1 and v2 highlight settings. Currently, if
   * v1 exists, we will convert it into v2 form and continue. Otherwise, return the v2 settings to
   * continue, no matter if v2 settings are empty or not. We do not expect the case v1 settings and
   * v2 settings co-exist in the same search request.
   *
   * @param highlight v1 highlight setting
   * @param highlightV2 v2 highlight settings
   * @return finalized the v2 version of highlight settings
   */
  public static HighlightV2 getFinalizedHighlightV2(Highlight highlight, HighlightV2 highlightV2) {
    if (!highlight.getFieldsList().isEmpty()) {
      return adaptHighlightToV2(highlight); // v1 takes precedence
    }
    return highlightV2;
  }
}
