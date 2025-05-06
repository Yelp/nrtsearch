/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.ingestion;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.handler.AddDocumentHandler;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import java.util.List;

/**
 * Abstract base class for ingestion implementations. Provides common ingestion utilities like
 * addDocuments and commit. Plugin-specific ingestion logic should extend this class and implement
 * start/stop.
 */
public abstract class AbstractIngestor implements Ingestor {
  protected final NrtsearchConfig config;
  protected GlobalState globalState;

  public AbstractIngestor(NrtsearchConfig config) {
    this.config = config;
  }

  /**
   * Called by the framework to initialize the ingestor with global state. Must be called before
   * addDocuments or commit.
   */
  @Override
  public void initialize(GlobalState globalState) {
    this.globalState = globalState;
  }

  /**
   * Add documents to the index.
   *
   * @param addDocRequests list of documents to add
   * @param indexName target index
   * @return sequence number of the indexing operation
   * @throws Exception if indexing fails
   */
  @Override
  public long addDocuments(List<AddDocumentRequest> addDocRequests, String indexName)
      throws Exception {
    verifyInitialized(indexName);
    return new AddDocumentHandler.DocumentIndexer(globalState, addDocRequests, indexName)
        .runIndexingJob();
  }

  /**
   * Commit changes to the index.
   *
   * @param indexName target index
   * @throws IOException if commit fails
   */
  @Override
  public void commit(String indexName) throws IOException {
    verifyInitialized(indexName);
    IndexState indexState = globalState.getIndexOrThrow(indexName);
    indexState.commit();
  }

  private void verifyInitialized(String indexName) throws IOException {
    if (globalState == null) {
      throw new IllegalStateException("Ingestor not initialized with GlobalState");
    }
    IndexState indexState = globalState.getIndexOrThrow(indexName);
    if (indexState == null) {
      throw new IllegalStateException("Index not found: " + indexName);
    }
  }

  /**
   * Start ingestion logic. Must be implemented by plugin-specific subclass.
   *
   * @throws IOException if startup fails
   */
  @Override
  public abstract void start() throws IOException;

  /**
   * Stop ingestion logic. Must be implemented by plugin-specific subclass.
   *
   * @throws IOException if shutdown fails
   */
  @Override
  public abstract void stop() throws IOException;
}
