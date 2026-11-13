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
package com.yelp.nrtsearch.server.ingestion;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import java.util.List;

/**
 * Interface for ingestion logic used by plugins.
 *
 * <p>This interface defines the lifecycle and operations for ingesting documents into an index.
 * Plugin implementations can use this interface to encapsulate source-specific ingestion logic
 * (e.g., reading from Kafka, S3, etc.) while leveraging shared indexing utilities.
 *
 * <p>Implementations are expected to:
 *
 * <ul>
 *   <li>Initialize with {@link GlobalState} before starting
 *   <li>Start ingestion in a background thread (if needed)
 *   <li>Use {@link #addDocuments(List, String)} and {@link #commit(String)} to index data
 *   <li>Clean up resources in {@link #stop()}
 * </ul>
 */
public interface Ingestor {

  /**
   * Initialize the ingestor with the global server state.
   *
   * <p>This method is called once by the framework before ingestion starts. Implementations should
   * store the global state for later use (e.g., to access index state).
   *
   * @param globalState the global server state
   */
  void initialize(GlobalState globalState);

  /**
   * Start the ingestion process.
   *
   * <p>This method should contain the logic to begin reading from the ingestion source (e.g.,
   * Kafka, file system, etc.). It may block or spawn background threads depending on the
   * implementation.
   *
   * @throws IOException if ingestion startup fails
   */
  void start() throws IOException;

  /**
   * Stop the ingestion process and clean up resources.
   *
   * <p>This method is called during plugin shutdown. Implementations should stop any background
   * threads, close connections, and release resources.
   *
   * @throws IOException if ingestion shutdown fails
   */
  void stop() throws IOException;

  /**
   * Add a batch of documents to the specified index.
   *
   * <p>This method is typically called from within the ingestion loop to index new data. It returns
   * the Lucene sequence number of the indexing operation.
   *
   * @param addDocRequests list of document requests to add
   * @param indexName name of the target index
   * @return sequence number of the indexing operation
   * @throws Exception if indexing fails
   */
  long addDocuments(List<AddDocumentRequest> addDocRequests, String indexName) throws Exception;

  /**
   * Delete documents matching the specified queries.
   *
   * <p>This method is designed to support deletion of documents during ingestion, particularly for
   * change data capture scenarios where DELETE and UPDATE_BEFORE operations need to be processed.
   * The delete operation should be executed before any subsequent addDocuments calls to ensure
   * correct ordering for UPDATE operations.
   *
   * @param queries list of queries to match documents for deletion
   * @param indexName name of the target index
   * @return sequence number of the delete operation
   * @throws Exception if delete fails
   */
  long deleteByQuery(List<Query> queries, String indexName) throws Exception;

  /**
   * If this ingestor supports multi-primary leader election. With leader election enabled, the
   * server calls {@link #start()} each time this node becomes the active primary, and {@link
   * #stop()} when it stops being the active primary, so the same instance is started and stopped
   * many times. An ingestor supporting this must:
   *
   * <ul>
   *   <li>allow {@link #start()} again after {@link #stop()}, creating fresh threads and source
   *       connections each time;
   *   <li>on each start, resume from the checkpoint durable in the index, see {@link
   *       AbstractIngestor#getCommittedCheckpoint(String)}, not from in memory or source system
   *       positions, since the index was restored from remote storage and may contain more or less
   *       data than this node last wrote;
   *   <li>record its position with {@link AbstractIngestor#commit(String, java.util.Map)}, only for
   *       data already added to the index.
   * </ul>
   *
   * When leader election is enabled and an ingestor does not support it, the server fails to start.
   *
   * @return true if supported, false by default
   */
  default boolean supportsLeaderElection() {
    return false;
  }

  /**
   * Commit any pending changes to the specified index.
   *
   * <p>This method should be called periodically or after a batch of documents is added to ensure
   * durability and visibility of the changes.
   *
   * @param indexName name of the target index
   * @throws IOException if commit fails
   */
  void commit(String indexName) throws IOException;
}
