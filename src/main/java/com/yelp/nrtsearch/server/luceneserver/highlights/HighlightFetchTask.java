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

import static com.yelp.nrtsearch.server.luceneserver.highlights.HighlightUtils.createPerFieldSettings;

import com.yelp.nrtsearch.server.grpc.Highlight;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Highlights;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.TextBaseFieldDef;
import com.yelp.nrtsearch.server.luceneserver.search.FetchTasks.FetchTask;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.DoubleAdder;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;

/**
 * Implementation of {@link FetchTask} which holds the required context to and used to provide
 * highlights for a search request.
 */
public class HighlightFetchTask implements FetchTask {

  private static final double TEN_TO_THE_POWER_SIX = Math.pow(10, 6);
  private final IndexState indexState;
  private final Map<String, HighlightSettings> fieldSettings;
  private final DoubleAdder timeTakenMs = new DoubleAdder();

  public HighlightFetchTask(
      IndexState indexState,
      Query searchQuery,
      HighlighterService highlighterService,
      Highlight highlight) {
    this.indexState = indexState;
    this.fieldSettings =
        createPerFieldSettings(highlight, searchQuery, indexState, highlighterService);
    verifyHighlights();
  }

  /**
   * Add highlighted fragments for a single hit to the response. Also counts the time taken for each
   * hit which can be retrieved by calling {@link #getTimeTakenMs()}.
   *
   * @param searchContext search context
   * @param hitLeaf lucene segment for hit
   * @param hit hit builder for query response
   * @throws IOException if there is a low-level IO error in highlighter
   */
  @Override
  public void processHit(SearchContext searchContext, LeafReaderContext hitLeaf, Builder hit)
      throws IOException {
    if (fieldSettings.isEmpty()) {
      return;
    }
    long startTime = System.nanoTime();
    for (Entry<String, HighlightSettings> fieldSetting : fieldSettings.entrySet()) {
      String fieldName = fieldSetting.getKey();
      Highlighter highlighter = fieldSetting.getValue().getHighlighter();
      FieldDef fieldDef = indexState.getField(fieldName);
      TextBaseFieldDef textBaseFieldDef =
          (TextBaseFieldDef) fieldDef; // This is safe as we verified earlier
      String[] highlights =
          highlighter.getHighlights(
              hitLeaf,
              fieldSetting.getValue(),
              textBaseFieldDef,
              hit.getLuceneDocId() - hitLeaf.docBase,
              searchContext);
      if (highlights != null && highlights.length > 0 && highlights[0] != null) {
        Highlights.Builder builder = Highlights.newBuilder();
        for (String fragment : highlights) {
          builder.addFragments(fragment);
        }
        hit.putHighlights(fieldName, builder.build());
      }
    }
    timeTakenMs.add(((System.nanoTime() - startTime) / TEN_TO_THE_POWER_SIX));
  }

  /**
   * Get the total time taken so far to generate highlights.
   *
   * @return Total time taken to generate highlights in ms.
   */
  public double getTimeTakenMs() {
    return timeTakenMs.doubleValue();
  }

  /**
   * Verify each highlighted field is highlight-able.
   *
   * @throws IllegalArgumentException if any field failed pass the verification.
   */
  private void verifyHighlights() {
    for (Entry<String, HighlightSettings> entry : fieldSettings.entrySet()) {
      String fieldName = entry.getKey();
      Highlighter highlighter = entry.getValue().getHighlighter();
      FieldDef field = indexState.getField(fieldName);
      if (!(field instanceof TextBaseFieldDef)) {
        throw new IllegalArgumentException(
            String.format(
                "Field %s is not a text field and does not support highlights", fieldName));
      }
      if (!((TextBaseFieldDef) field).isSearchable()) {
        throw new IllegalArgumentException(
            String.format("Field %s is not searchable and cannot support highlights", fieldName));
      }
      highlighter.verifyFieldIsSupported(((TextBaseFieldDef) field));
    }
  }
}
