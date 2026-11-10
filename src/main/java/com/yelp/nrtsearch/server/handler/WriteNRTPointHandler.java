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
package com.yelp.nrtsearch.server.handler;

import com.yelp.nrtsearch.server.grpc.IndexName;
import com.yelp.nrtsearch.server.grpc.SearcherVersion;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.IndexStateManager;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteNRTPointHandler extends Handler<IndexName, SearcherVersion> {
  private static final Logger logger = LoggerFactory.getLogger(WriteNRTPointHandler.class);

  public WriteNRTPointHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public SearcherVersion handle(IndexName indexNameRequest) throws Exception {
    IndexStateManager indexStateManager = getIndexStateManager(indexNameRequest.getIndexName());
    IndexState indexState = indexStateManager.getCurrent();
    SearcherVersion reply = handle(indexState);
    logger.debug("WriteNRTPointHandler returned version {}", reply.getVersion());
    return reply;
  }

  private SearcherVersion handle(IndexState indexState) {
    final ShardState shardState = indexState.getShard(0);

    if (!shardState.isPrimary()) {
      throw new IllegalArgumentException(
          "index \"" + indexState.getName() + "\" is either not started or is not a primary index");
    }
    SearcherVersion.Builder searchverVersionBuilder = SearcherVersion.newBuilder();
    try {
      if (shardState.nrtPrimaryNode.flushAndRefresh()) {
        // Something did get flushed (there were indexing ops since the last flush), notify
        // current replicas
        long version = shardState.nrtPrimaryNode.getCopyStateVersion();
        shardState.nrtPrimaryNode.sendNewNRTPointToReplicas(version);
        return searchverVersionBuilder.setVersion(version).setDidRefresh(true).build();
      } else {
        SearcherTaxonomyManager.SearcherAndTaxonomy s = shardState.acquire();
        try {
          long version = ((DirectoryReader) s.searcher().getIndexReader()).getVersion();
          return searchverVersionBuilder.setVersion(version).setDidRefresh(false).build();
        } finally {
          shardState.release(s);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
