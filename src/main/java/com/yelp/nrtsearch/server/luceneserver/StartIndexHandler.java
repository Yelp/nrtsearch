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

import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.grpc.RestoreIndex;
import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import com.yelp.nrtsearch.server.grpc.StartIndexResponse;
import com.yelp.nrtsearch.server.utils.Archiver;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.IndexReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartIndexHandler implements Handler<StartIndexRequest, StartIndexResponse> {
  public enum INDEXED_DATA_TYPE {
    DATA,
    STATE
  }

  private final Archiver archiver;
  Logger logger = LoggerFactory.getLogger(StartIndexHandler.class);

  public StartIndexHandler(Archiver archiver) {
    this.archiver = archiver;
  }

  public StartIndexHandler() {
    this(null);
  }

  @Override
  public StartIndexResponse handle(IndexState indexState, StartIndexRequest startIndexRequest)
      throws StartIndexHandlerException {
    final ShardState shardState = indexState.getShard(0);
    final Mode mode = startIndexRequest.getMode();
    final long primaryGen;
    final String primaryAddress;
    final int primaryPort;
    Path dataPath = null;
    if (startIndexRequest.hasRestore() && !shardState.isStarted()) {
      synchronized (shardState) {
        try {
          if (!shardState.isRestored()) {
            RestoreIndex restoreIndex = startIndexRequest.getRestore();
            dataPath =
                downloadArtifact(
                    restoreIndex.getServiceName(),
                    restoreIndex.getResourceName(),
                    INDEXED_DATA_TYPE.DATA);
          } else {
            throw new IllegalStateException("Index " + indexState.name + " already restored");
          }
        } finally {
          shardState.setRestored(true);
        }
      }
    }
    if (mode.equals(Mode.PRIMARY)) {
      primaryGen = startIndexRequest.getPrimaryGen();
      primaryAddress = null;
      primaryPort = -1;
    } else if (mode.equals(Mode.REPLICA)) {
      primaryGen = startIndexRequest.getPrimaryGen();
      primaryAddress = startIndexRequest.getPrimaryAddress();
      primaryPort = startIndexRequest.getPort();
    } else {
      primaryGen = -1;
      primaryAddress = null;
      primaryPort = -1;
    }

    long t0 = System.nanoTime();
    try {
      if (mode.equals(Mode.PRIMARY)) {
        shardState.startPrimary(primaryGen, dataPath);
      } else if (mode.equals(Mode.REPLICA)) {
        // channel for replica to talk to primary on
        ReplicationServerClient primaryNodeClient =
            new ReplicationServerClient(primaryAddress, primaryPort);
        shardState.startReplica(primaryNodeClient, primaryGen, dataPath);
      } else {
        indexState.start(dataPath);
      }
    } catch (Exception e) {
      logger.error("Cannot start IndexState/ShardState", e);
      throw new StartIndexHandlerException(e);
    }

    StartIndexResponse.Builder startIndexResponseBuilder = StartIndexResponse.newBuilder();
    SearcherTaxonomyManager.SearcherAndTaxonomy s;
    try {
      s = shardState.acquire();
    } catch (IOException e) {
      logger.error("Acquire shard state failed", e);
      throw new StartIndexHandlerException(e);
    }
    try {
      IndexReader r = s.searcher.getIndexReader();
      startIndexResponseBuilder.setMaxDoc(r.maxDoc());
      startIndexResponseBuilder.setNumDocs(r.numDocs());
      startIndexResponseBuilder.setSegments(r.toString());
    } finally {
      try {
        shardState.release(s);
      } catch (IOException e) {
        logger.error("Release shard state failed", e);
        throw new StartIndexHandlerException(e);
      }
    }
    long t1 = System.nanoTime();
    startIndexResponseBuilder.setStartTimeMS(((t1 - t0) / 1000000.0));
    return startIndexResponseBuilder.build();
  }

  public Path downloadArtifact(
      String serviceName, String resourceName, INDEXED_DATA_TYPE indexDataType) {
    String resource;
    if (indexDataType.equals(INDEXED_DATA_TYPE.DATA)) {
      resource = BackupIndexRequestHandler.getResourceData(resourceName);
    } else if (indexDataType.equals(INDEXED_DATA_TYPE.STATE)) {
      resource = BackupIndexRequestHandler.getResourceMetadata(resourceName);
    } else {
      throw new RuntimeException("Invalid INDEXED_DATA_TYPE " + indexDataType);
    }
    try {
      return archiver.download(serviceName, resource);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static class RestorePathInfo {
    public final Path dataPath;
    public final Path metadataPath;

    RestorePathInfo(Path dataPath, Path metadataPath) {
      this.dataPath = dataPath;
      this.metadataPath = metadataPath;
    }
  }

  public static class StartIndexHandlerException extends HandlerException {

    public StartIndexHandlerException(Exception e) {
      super(e);
    }
  }
}
