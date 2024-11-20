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
package com.yelp.nrtsearch.server.index;

import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient.DiscoveryFileAndPort;
import com.yelp.nrtsearch.server.grpc.RestoreIndex;
import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import com.yelp.nrtsearch.server.grpc.StartIndexResponse;
import com.yelp.nrtsearch.server.handler.Handler.HandlerException;
import com.yelp.nrtsearch.server.nrt.NrtDataManager;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.state.BackendGlobalState;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.IndexReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartIndexProcessor {
  private static final Set<String> startingIndices = new HashSet<>();

  private final String serviceName;
  private final String ephemeralId;
  private final RemoteBackend remoteBackend;
  private final IndexStateManager indexStateManager;
  private final boolean remoteCommit;
  private final int discoveryFileUpdateIntervalMs;
  private static final Logger logger = LoggerFactory.getLogger(StartIndexProcessor.class);

  /**
   * Constructor for StartIndexHandler.
   *
   * @param serviceName service name
   * @param ephemeralId ephemeral id for this node
   * @param remoteBackend remote state backend
   * @param indexStateManager index state manager
   * @param remoteCommit whether to commit to remote state
   * @param discoveryFileUpdateIntervalMs interval to update backends from discovery file
   */
  public StartIndexProcessor(
      String serviceName,
      String ephemeralId,
      RemoteBackend remoteBackend,
      IndexStateManager indexStateManager,
      boolean remoteCommit,
      int discoveryFileUpdateIntervalMs) {
    this.serviceName = serviceName;
    this.ephemeralId = ephemeralId;
    this.remoteBackend = remoteBackend;
    this.indexStateManager = indexStateManager;
    this.remoteCommit = remoteCommit;
    this.discoveryFileUpdateIntervalMs = discoveryFileUpdateIntervalMs;
  }

  public StartIndexResponse process(IndexState indexState, StartIndexRequest startIndexRequest)
      throws StartIndexProcessorException {
    String indexName = indexState.getName();
    synchronized (startingIndices) {
      if (indexState.isStarted()) {
        throw new IllegalArgumentException(String.format("Index %s is already started", indexName));
      }
      // Ensure that the index is not already being started
      if (startingIndices.contains(indexName)) {
        throw new IllegalArgumentException(
            String.format("Index %s is already being started", indexName));
      }
      startingIndices.add(indexName);
    }

    try {
      return processInternal(indexState, startIndexRequest);
    } finally {
      synchronized (startingIndices) {
        startingIndices.remove(indexName);
      }
    }
  }

  private StartIndexResponse processInternal(
      IndexState indexState, StartIndexRequest startIndexRequest)
      throws StartIndexProcessorException {
    final ShardState shardState = indexState.getShard(0);
    final Mode mode = startIndexRequest.getMode();
    final long primaryGen;
    final ReplicationServerClient primaryClient;

    RestoreIndex restoreIndex =
        startIndexRequest.hasRestore() ? startIndexRequest.getRestore() : null;
    NrtDataManager nrtDataManager =
        new NrtDataManager(
            serviceName,
            BackendGlobalState.getUniqueIndexName(
                indexState.getName(), indexStateManager.getIndexId()),
            ephemeralId,
            remoteBackend,
            restoreIndex,
            remoteCommit);
    if (mode.equals(Mode.PRIMARY)) {
      primaryGen = startIndexRequest.getPrimaryGen();
      primaryClient = null;
    } else if (mode.equals(Mode.REPLICA)) {
      primaryGen = startIndexRequest.getPrimaryGen();
      primaryClient = getPrimaryClientForRequest(startIndexRequest);
    } else {
      primaryGen = -1;
      primaryClient = null;
    }

    long t0 = System.nanoTime();
    try {
      if (mode.equals(Mode.REPLICA)) {
        indexState.initWarmer(remoteBackend);
      }

      indexStateManager.start(mode, nrtDataManager, primaryGen, primaryClient);
    } catch (Exception e) {
      logger.error("Cannot start IndexState/ShardState", e);
      throw new StartIndexProcessorException(e);
    }

    StartIndexResponse.Builder startIndexResponseBuilder = StartIndexResponse.newBuilder();
    SearcherTaxonomyManager.SearcherAndTaxonomy s;
    try {
      s = shardState.acquire();
    } catch (IOException e) {
      logger.error("Acquire shard state failed", e);
      throw new StartIndexProcessorException(e);
    }
    try {
      IndexReader r = s.searcher().getIndexReader();
      startIndexResponseBuilder.setMaxDoc(r.maxDoc());
      startIndexResponseBuilder.setNumDocs(r.numDocs());
      startIndexResponseBuilder.setSegments(r.toString());
    } finally {
      try {
        shardState.release(s);
      } catch (IOException e) {
        logger.error("Release shard state failed", e);
        throw new StartIndexProcessorException(e);
      }
    }
    long t1 = System.nanoTime();
    startIndexResponseBuilder.setStartTimeMS(((t1 - t0) / 1000000.0));
    return startIndexResponseBuilder.build();
  }

  private ReplicationServerClient getPrimaryClientForRequest(StartIndexRequest request) {
    if (!request.getPrimaryAddress().isEmpty()) {
      return new ReplicationServerClient(request.getPrimaryAddress(), request.getPort());
    } else if (!request.getPrimaryDiscoveryFile().isEmpty()) {
      return new ReplicationServerClient(
          new DiscoveryFileAndPort(request.getPrimaryDiscoveryFile(), request.getPort()),
          discoveryFileUpdateIntervalMs);
    } else {
      throw new IllegalArgumentException(
          "Unable to initialize primary replication client for start request: " + request);
    }
  }

  public static class StartIndexProcessorException extends HandlerException {

    public StartIndexProcessorException(Exception e) {
      super(e);
    }
  }
}
