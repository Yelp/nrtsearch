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
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;

public class HighlightFetchTask implements FetchTask {

  private final IndexReader indexReader;
  private final Map<String, HighlightSettings> fieldSettings;
  private final HighlightHandler highlightHandler = new HighlightHandler();

  public HighlightFetchTask(
      IndexState indexState,
      SearcherAndTaxonomy searcherAndTaxonomy,
      Query searchQuery,
      Highlight highlight)
      throws IOException {
    indexReader = searcherAndTaxonomy.searcher.getIndexReader();
    fieldSettings = createPerFieldSettings(indexReader, highlight, searchQuery, indexState);
  }

  @Override
  public void processHit(SearchContext searchContext, LeafReaderContext hitLeaf, Builder hit)
      throws IOException {
    if (fieldSettings.isEmpty()) {
      return;
    }
    for (String fieldName : fieldSettings.keySet()) {
      String[] highlights =
          highlightHandler.getHighlights(
              indexReader, fieldSettings.get(fieldName), fieldName, hit.getLuceneDocId());
      if (highlights != null && highlights.length > 0 && highlights[0] != null) {
        Highlights.Builder builder = Highlights.newBuilder();
        for (String fragment : highlights) {
          builder.addFragments(fragment);
        }
        hit.putHighlights(fieldName, builder.build());
      }
    }
  }
}
