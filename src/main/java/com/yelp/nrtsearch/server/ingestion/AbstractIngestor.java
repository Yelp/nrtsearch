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
import com.yelp.nrtsearch.server.field.IdFieldDef;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.handler.AddDocumentHandler;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.query.QueryNodeMapper;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

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
   * Delete documents matching the specified queries.
   *
   * <p>This method deletes documents from the index that match any of the provided queries. It is
   * designed to support deletion of documents by ID or other criteria during ingestion (e.g., for
   * handling DELETE and UPDATE_BEFORE row kinds in change data capture scenarios).
   *
   * <p>The delete operation is executed directly against the IndexWriter, ensuring it is processed
   * before any subsequent addDocuments calls. This ordering is critical for UPDATE operations where
   * the old document must be deleted before the new version is added.
   *
   * @param queries list of queries to match documents for deletion
   * @param indexName target index name
   * @return sequence number of the delete operation
   * @throws Exception if delete fails
   */
  @Override
  public long deleteByQuery(List<Query> queries, String indexName) throws Exception {
    verifyInitialized(indexName);
    IndexState indexState = globalState.getIndexOrThrow(indexName);
    ShardState shardState = indexState.getShard(0);
    indexState.verifyStarted();

    QueryNodeMapper queryNodeMapper = QueryNodeMapper.getInstance();
    List<org.apache.lucene.search.Query> luceneQueries =
        queries.stream().map(query -> queryNodeMapper.getQuery(query, indexState)).toList();

    try {
      shardState.writer.deleteDocuments(
          luceneQueries.toArray(new org.apache.lucene.search.Query[] {}));
    } catch (IOException e) {
      throw new RuntimeException("Failed to delete documents by query", e);
    }

    return shardState.writer.getMaxCompletedSequenceNumber();
  }

  /**
   * Get the ID field definition for the index, if one exists.
   *
   * <p>This method provides access to the index's ID field metadata, which is useful for ingestion
   * plugins that need to construct delete queries based on document IDs.
   *
   * @param indexName name of the index
   * @return Optional containing the IdFieldDef, or empty if no ID field is defined
   * @throws IOException if index state cannot be accessed
   */
  protected Optional<IdFieldDef> getIdFieldDef(String indexName) throws IOException {
    verifyInitialized(indexName);
    IndexState indexState = globalState.getIndexOrThrow(indexName);
    return indexState.getIdFieldDef();
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
