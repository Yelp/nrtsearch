/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.plugins;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.handler.AddDocumentHandler;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for ingestion plugins that handles state management and indexing operations. Plugin
 * implementations: 1. Must implement startIngestion/stopIngestion to handle source-specific logic
 * 2. Should use addDocuments/commit methods to index data during ingestion
 */
public abstract class AbstractIngestionPlugin extends Plugin implements IngestionPlugin {
  private static final Logger logger = LoggerFactory.getLogger(AbstractIngestionPlugin.class);

  protected final NrtsearchConfig config;
  private GlobalState globalState;

  protected AbstractIngestionPlugin(NrtsearchConfig config) {
    this.config = config;
  }

  /**
   * Initialize plugin state with index access. Called by NrtSearchServer when index is ready.
   *
   * @param globalState The global server state
   */
  public final void initializeState(GlobalState globalState) throws IOException {
    if (this.globalState != null) {
      throw new IllegalStateException("Plugin already initialized");
    }
    this.globalState = globalState;
  }

  /**
   * Add documents. Available for plugin implementations to call during ingestion.
   *
   * @param addDocRequests List of document requests to add
   * @return The sequence number of the indexing operation
   * @throws IOException if there are indexing errors
   */
  protected final long addDocuments(List<AddDocumentRequest> addDocRequests, String indexName)
      throws Exception {
    return new AddDocumentHandler.DocumentIndexer(globalState, addDocRequests, indexName)
        .runIndexingJob();
  }

  /**
   * Commit ingested documents. Available for plugin implementations to call during ingestion.
   *
   * @throws IOException if there are commit errors
   */
  protected final void commit(String indexName) throws IOException {
    verifyInitialized(indexName);
    IndexState indexState = globalState.getIndexOrThrow(indexName);
    ShardState shard = indexState.getShard(0);
    if (shard == null) {
      throw new IllegalStateException("No shard found for index");
    }
    shard.commit();
  }

  private void verifyInitialized(String indexName) throws IOException {
    IndexState indexState = globalState.getIndexOrThrow(indexName);
    if (indexState == null) {
      throw new IllegalStateException("Plugin not initialized");
    }
  }

  @Override
  public void close() throws IOException {
    stopIngestion();
  }
}
