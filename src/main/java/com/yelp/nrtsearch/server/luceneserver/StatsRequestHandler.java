/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.grpc.IndexStatsResponse;
import com.yelp.nrtsearch.server.grpc.IndicesResponse;
import com.yelp.nrtsearch.server.grpc.Searcher;
import com.yelp.nrtsearch.server.grpc.StatsRequest;
import com.yelp.nrtsearch.server.grpc.StatsResponse;
import com.yelp.nrtsearch.server.grpc.Taxonomy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherLifetimeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsRequestHandler implements Handler<StatsRequest, StatsResponse> {
  Logger logger = LoggerFactory.getLogger(StatsRequestHandler.class);

  @Override
  public StatsResponse handle(IndexState indexState, StatsRequest statsRequest)
      throws HandlerException {
    try {
      return process(indexState);
    } catch (IOException e) {
      logger.warn(" Failed to generate stats for index:  " + indexState.getName(), e);
      throw new HandlerException(e);
    }
  }

  private StatsResponse process(IndexState indexState) throws IOException {
    StatsResponse.Builder statsResponseBuilder = StatsResponse.newBuilder();
    if (indexState.getShards().size() > 1) {
      logger.error(
          "{} shards present for index {}, unable to process more than 1 shard",
          indexState.getShards().size(),
          indexState.getName());
      throw new IllegalStateException(
          "Unable to get stats as more than 1 shard found for index " + indexState.getName());
    }
    for (Map.Entry<Integer, ShardState> entry : indexState.getShards().entrySet()) {
      ShardState shardState = entry.getValue();
      statsResponseBuilder.setOrd(entry.getKey());
      if (shardState.writer != null) { // primary and standalone mode
        IndexWriter.DocStats docStats = shardState.writer.getDocStats();
        statsResponseBuilder.setMaxDoc(docStats.maxDoc);
        statsResponseBuilder.setNumDocs(docStats.numDocs);
      }
      String[] fNames = shardState.indexDir.listAll();
      long dirSize = 0;
      for (int i = 0; i < fNames.length; i++) {
        try {
          dirSize += shardState.indexDir.fileLength(fNames[i]);
        } catch (IOException ignored) {
          // files may be deleted from merging, don't fail the request
        }
      }
      statsResponseBuilder.setDirSize(dirSize);
      // TODO: snapshots

      // TODO: go per segment and print more details, and
      // only print segment for a given searcher if it's
      // "new"

      // Doesn't actually prune; just gathers stats
      List<Searcher> tmpSearchers = new ArrayList<>();
      shardState.slm.prune(
          new SearcherLifetimeManager.Pruner() {
            @Override
            public boolean doPrune(double ageSec, IndexSearcher indexSearcher) {
              Searcher.Builder searcher = Searcher.newBuilder();
              searcher.setVersion(((DirectoryReader) indexSearcher.getIndexReader()).getVersion());
              searcher.setStaleAgeSeconds(ageSec);
              searcher.setSegments(indexSearcher.getIndexReader().toString());
              searcher.setNumDocs(indexSearcher.getIndexReader().maxDoc());
              tmpSearchers.add(searcher.build());
              return false;
            }
          });
      statsResponseBuilder.addAllSearchers(tmpSearchers);
      statsResponseBuilder.setState(shardState.getState());

      SearcherTaxonomyManager.SearcherAndTaxonomy s = shardState.acquire();
      try {
        Taxonomy.Builder taxonomy = Taxonomy.newBuilder();
        if (s.taxonomyReader != null) { // taxo reader is null for primary and replica
          taxonomy.setNumOrds(s.taxonomyReader.getSize());
          taxonomy.setSegments(s.taxonomyReader.toString());
        }
        statsResponseBuilder.setTaxonomy(taxonomy.build());
        Searcher.Builder searcher = Searcher.newBuilder();
        if (s.searcher != null) {
          searcher.setSegments(s.searcher.toString());
          IndexReader indexReader = s.searcher.getIndexReader();
          searcher.setNumDocs(indexReader.numDocs());
          if (indexReader instanceof StandardDirectoryReader) {
            StandardDirectoryReader standardDirectoryReader = (StandardDirectoryReader) indexReader;
            searcher.setNumSegments(standardDirectoryReader.getSegmentInfos().asList().size());
          }
        }
        statsResponseBuilder.setCurrentSearcher(searcher.build());
      } finally {
        shardState.release(s);
      }
    }
    return statsResponseBuilder.build();
  }

  public static IndicesResponse getIndicesResponse(GlobalState globalState)
      throws IOException, HandlerException {
    Set<String> indexNames = globalState.getIndexNames();
    IndicesResponse.Builder builder = IndicesResponse.newBuilder();
    for (String indexName : indexNames) {
      IndexState indexState = globalState.getIndex(indexName);
      if (indexState.isStarted()) {
        StatsResponse statsResponse =
            new StatsRequestHandler()
                .handle(indexState, StatsRequest.newBuilder().setIndexName(indexName).build());
        builder.addIndicesResponse(
            IndexStatsResponse.newBuilder()
                .setIndexName(indexName)
                .setStatsResponse(statsResponse)
                .build());
      } else {
        builder.addIndicesResponse(
            IndexStatsResponse.newBuilder()
                .setIndexName(indexName)
                .setStatsResponse(StatsResponse.newBuilder().setState("not_started").build()));
      }
    }
    return builder.build();
  }
}
