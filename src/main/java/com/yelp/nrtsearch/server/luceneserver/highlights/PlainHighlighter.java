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
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.NullFragmenter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.search.highlight.TextFragment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlainHighlighter implements Highlighter {

  static final String HIGHLIGHTER_NAME = "plain-highlighter";
  private static final String CACHE_KEY = HIGHLIGHTER_NAME;
  private static final PlainHighlighter INSTANCE = new PlainHighlighter();
  private static final Encoder DEFAULT_ENCODER = new DefaultEncoder();
  private static Logger logger = LoggerFactory.getLogger(PlainHighlighter.class);

  public static PlainHighlighter getInstance() {
    return INSTANCE;
  }

  @Override
  public String getName() {
    return HIGHLIGHTER_NAME;
  }

  @Override
  public void verifyFieldIsSupported(TextBaseFieldDef fieldDef) {
    if (!fieldDef.isStored()) {
      throw new IllegalArgumentException(
          String.format(
              "Field %s is not stored and cannot support plain-highlighter", fieldDef.getName()));
    }
  }

  @Override
  public String[] getHighlights(
      LeafReaderContext hitLeaf,
      HighlightSettings settings,
      TextBaseFieldDef textBaseFieldDef,
      int leafDocId,
      SearchContext searchContext)
      throws IOException {
    String fieldName = textBaseFieldDef.getName();
    // always treat multivalue fields discretely
    String[] fieldTexts =
        Arrays.stream(hitLeaf.reader().document(leafDocId).getFields(fieldName))
            .map(IndexableField::stringValue)
            .toArray(String[]::new);

    return getHighlights(fieldTexts, settings, textBaseFieldDef, searchContext);
  }

  public String[] getHighlights(
      String[] texts,
      HighlightSettings highlightSettings,
      TextBaseFieldDef textBaseFieldDef,
      SearchContext searchContext)
      throws IOException {
    // TODO: do we need to make this analyzer selectable between index and search?
    Analyzer analyzer = textBaseFieldDef.getIndexAnalyzer().get();
    // 0 means retrieving the while field as a single fragment.
    final int numberOfFragments =
        highlightSettings.getMaxNumFragments() == 0 ? 1 : highlightSettings.getMaxNumFragments();
    org.apache.lucene.search.highlight.Highlighter plainHighlighter =
        getPlainHighlighter(
            highlightSettings, textBaseFieldDef.getName(), searchContext.getExtraContext());

    // if it isn't a multivalue field, the fragments are already sorted by score;
    Queue<TextFragment> fragQueue =
        texts.length <= 1 || !highlightSettings.isScoreOrdered()
            ? new LinkedList<>()
            : new PriorityQueue<>(((o1, o2) -> Double.compare(o2.getScore(), o1.getScore())));

    for (String text : texts) {
      try (TokenStream tokenStream = analyzer.tokenStream(textBaseFieldDef.getName(), text)) {
        if (tokenStream.hasAttribute(CharTermAttribute.class) == false
            || tokenStream.hasAttribute(OffsetAttribute.class) == false) {
          // can't perform highlighting if the stream has no terms (binary token stream) or no
          // offsets
          return null;
        }
        TextFragment[] bestTextFragments =
            plainHighlighter.getBestTextFragments(tokenStream, text, false, numberOfFragments);

        for (TextFragment frag : bestTextFragments) {
          if (frag != null && frag.getScore() > 0d) {
            fragQueue.offer(frag);
          } else {
            // best fragments for each field is already sorted, not need to continue if meets 0
            // score.
            break;
          }
        }
      } catch (InvalidTokenOffsetsException e) {
        throw new IllegalArgumentException(e);
      }
    }

    final int limitNumberOfFragments =
        numberOfFragments == 0 ? Integer.MAX_VALUE : numberOfFragments;

    String[] finalFragments = new String[fragQueue.size()];
    for (int i = 0; i < limitNumberOfFragments && !fragQueue.isEmpty(); i++) {
      finalFragments[i] = fragQueue.poll().toString();
    }

    return finalFragments;
  }

  /** Builds the Lucene highlighter with the right settings. Cache it per field per request. */
  private org.apache.lucene.search.highlight.Highlighter getPlainHighlighter(
      HighlightSettings highlightSettings, String fieldName, Map<String, Object> contextCache) {
    @SuppressWarnings("unchecked")
    Map<String, org.apache.lucene.search.highlight.Highlighter> cache =
        (Map<String, org.apache.lucene.search.highlight.Highlighter>)
            contextCache.computeIfAbsent(CACHE_KEY, k -> new HashMap<>());
    org.apache.lucene.search.highlight.Highlighter cachedHighlighter = cache.get(fieldName);
    if (cachedHighlighter == null) {
      QueryScorer queryScorer =
          new QueryScorer(
              highlightSettings.getHighlightQuery(),
              highlightSettings.getFieldMatch() ? fieldName : null);
      queryScorer.setExpandMultiTermQuery(true);
      final Fragmenter fragmenter =
          getFragmenter(
              highlightSettings,
              fieldName,
              queryScorer,
              highlightSettings.getMaxNumFragments() == 0
                  ? Integer.MAX_VALUE
                  : highlightSettings.getFragmentSize());
      Formatter formatter =
          new SimpleHTMLFormatter(
              highlightSettings.getPreTags()[0], highlightSettings.getPostTags()[0]);
      cachedHighlighter =
          new org.apache.lucene.search.highlight.Highlighter(
              formatter, DEFAULT_ENCODER, queryScorer);
      cachedHighlighter.setTextFragmenter(fragmenter);
      // always highlight across all data
      cachedHighlighter.setMaxDocCharsToAnalyze(Integer.MAX_VALUE);

      cache.put(fieldName, cachedHighlighter);
    }
    return cachedHighlighter;
  }

  /**
   * Get fragmenter based on the options provided by the highlight query Whatever is the fragmenter,
   * we get the fragment character size from the options
   */
  private Fragmenter getFragmenter(
      HighlightSettings highlightSettings,
      String fieldName,
      QueryScorer queryScorer,
      int actualFragmentSize) {
    if (highlightSettings.getMaxNumFragments() == 0) {
      return new NullFragmenter();
    } else if ("simple".equals(highlightSettings.getFragmenter())) {
      return new SimpleFragmenter(actualFragmentSize);
    } else if ("span".equals(highlightSettings.getFragmenter())) {
      return new SimpleSpanFragmenter(queryScorer, actualFragmentSize);
    } else {
      throw new IllegalArgumentException(
          "unknown fragmenter option ["
              + highlightSettings.getFragmenter()
              + "] for the field ["
              + fieldName
              + "]");
    }
  }
}
