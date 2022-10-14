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

import static com.yelp.nrtsearch.server.luceneserver.highlights.HighlightSettingsHelper.createPerFieldSettings;

import com.yelp.nrtsearch.server.grpc.Highlight;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Highlights;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.search.FetchTasks.FetchTask;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.DoubleAdder;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;

/**
 * Implementation of {@link FetchTask} which holds the required context to and used to provide
 * highlights for a search request.
 */
public class HighlightFetchTask implements FetchTask {

  private static final double TEN_TO_THE_POWER_SIX = Math.pow(10, 6);
  private final IndexReader indexReader;
  private final Map<String, HighlightSettings> fieldSettings;
  private final HighlightHandler highlightHandler = HighlightHandler.getInstance();

  private final DoubleAdder timeTakenMs = new DoubleAdder();

  public HighlightFetchTask(
      IndexState indexState,
      SearcherAndTaxonomy searcherAndTaxonomy,
      Query searchQuery,
      Highlight highlight)
      throws IOException {
    indexReader = searcherAndTaxonomy.searcher.getIndexReader();
    fieldSettings = createPerFieldSettings(indexReader, highlight, searchQuery, indexState);
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
      String[] highlights =
          highlightHandler.getHighlights(
              indexReader, fieldSetting.getValue(), fieldName, hit.getLuceneDocId());
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
}
