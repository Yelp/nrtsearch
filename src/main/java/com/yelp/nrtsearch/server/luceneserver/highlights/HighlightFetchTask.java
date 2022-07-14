package com.yelp.nrtsearch.server.luceneserver.highlights;

import com.yelp.nrtsearch.server.grpc.Highlight.Settings;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Highlights;
import com.yelp.nrtsearch.server.luceneserver.search.FetchTasks.FetchTask;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;

public class HighlightFetchTask implements FetchTask {

  private final HighlightHandler highlightHandler = new HighlightHandler();

  @Override
  public void processHit(SearchContext searchContext, LeafReaderContext hitLeaf, Builder hit)
      throws IOException {
    Map<String, Settings> fieldsMap = searchContext.getHighlight().getFieldsMap();
    IndexReader indexReader = searchContext.getSearcherAndTaxonomy().searcher.getIndexReader();
    for (String fieldName : fieldsMap.keySet()) {
      String[] highlights = highlightHandler.getHighlights(searchContext.getIndexState(), indexReader, searchContext.getQuery(),
          searchContext.getHighlight(), fieldName, hit.getLuceneDocId());
      Highlights.Builder builder = Highlights.newBuilder();
      for (String fragment : highlights) {
        builder.addFragments(fragment);
      }
      hit.putHighlights(fieldName, builder.build());
    }
  }
}
