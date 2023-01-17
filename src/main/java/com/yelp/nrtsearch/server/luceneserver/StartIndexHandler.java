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

import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient.DiscoveryFileAndPort;
import com.yelp.nrtsearch.server.grpc.RestoreIndex;
import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import com.yelp.nrtsearch.server.grpc.StartIndexResponse;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import com.yelp.nrtsearch.server.utils.FileUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
  private final Archiver incArchiver;
  private final String archiveDirectory;
  private final IndexStateManager indexStateManager;
  private final boolean backupFromIncArchiver;
  private final boolean restoreFromIncArchiver;
  private final int discoveryFileUpdateIntervalMs;
  private static final Logger logger = LoggerFactory.getLogger(StartIndexHandler.class);

  public StartIndexHandler(
      Archiver archiver,
      Archiver incArchiver,
      String archiveDirectory,
      boolean backupFromIncArchiver,
      boolean restoreFromIncArchiver,
      IndexStateManager indexStateManager,
      int discoveryFileUpdateIntervalMs) {
    this.archiver = archiver;
    this.incArchiver = incArchiver;
    this.archiveDirectory = archiveDirectory;
    this.backupFromIncArchiver = backupFromIncArchiver;
    this.restoreFromIncArchiver = restoreFromIncArchiver;
    this.indexStateManager = indexStateManager;
    this.discoveryFileUpdateIntervalMs = discoveryFileUpdateIntervalMs;
  }

  @Override
  public StartIndexResponse handle(IndexState indexState, StartIndexRequest startIndexRequest)
      throws StartIndexHandlerException {
    if (indexState.isStarted()) {
      throw new IllegalArgumentException(
          String.format("Index %s is already started", indexState.getName()));
    }

    final ShardState shardState = indexState.getShard(0);
    final Mode mode = startIndexRequest.getMode();
    final long primaryGen;
    final ReplicationServerClient primaryClient;
    Path dataPath = null;

    try {
      if (startIndexRequest.hasRestore() && !shardState.isStarted()) {
        synchronized (shardState) {
          try {
            if (!shardState.isRestored()) {
              RestoreIndex restoreIndex = startIndexRequest.getRestore();
              if (restoreIndex.getDeleteExistingData()) {
                indexState.deleteIndexRootDir();
                Files.createDirectories(indexState.getRootDir());
                deleteDownloadedBackupDirectories(restoreIndex.getResourceName());
              }

              dataPath =
                  downloadArtifact(
                      restoreIndex.getServiceName(),
                      restoreIndex.getResourceName(),
                      INDEXED_DATA_TYPE.DATA,
                      restoreFromIncArchiver);
            } else {
              throw new IllegalStateException(
                  "Index " + indexState.getName() + " already restored");
            }
          } catch (IOException e) {
            logger.info("Unable to delete existing index data", e);
            throw new StartIndexHandlerException(e);
          }
        }
      }
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
          indexState.initWarmer(archiver);
        }

        indexStateManager.start(mode, dataPath, primaryGen, primaryClient);

        if (mode.equals(Mode.PRIMARY)) {
          BackupIndexRequestHandler backupIndexRequestHandler =
              new BackupIndexRequestHandler(
                  archiver, incArchiver, archiveDirectory, backupFromIncArchiver);
          if (backupIndexRequestHandler.wasBackupPotentiallyInterrupted()) {
            if (backupIndexRequestHandler
                .getIndexNameOfInterruptedBackup()
                .equals(indexState.getName())) {
              backupIndexRequestHandler.interruptedBackupCleanup(
                  indexState, shardState.snapshotGenToVersion.keySet());
            }
          }
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
    } finally {
      if (startIndexRequest.hasRestore()) {
        cleanupDownloadedArtifacts(
            startIndexRequest.getRestore().getResourceName(),
            INDEXED_DATA_TYPE.DATA,
            restoreFromIncArchiver);
      }
    }
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

  private void deleteDownloadedBackupDirectories(String resourceName) throws IOException {
    String resourceDataDirectory = IndexBackupUtils.getResourceData(resourceName);
    FileUtil.deleteAllFiles(Paths.get(archiveDirectory, resourceDataDirectory));
  }

  /**
   * Returns: path to "current" dir containing symlink to point to versionHash dirName that contains
   * index data
   */
  public Path downloadArtifact(
      String serviceName,
      String resourceName,
      INDEXED_DATA_TYPE indexDataType,
      boolean disableLegacyArchiver) {
    String resource;
    if (indexDataType.equals(INDEXED_DATA_TYPE.DATA)) {
      resource = IndexBackupUtils.getResourceData(resourceName);
    } else if (indexDataType.equals(INDEXED_DATA_TYPE.STATE)) {
      resource = IndexBackupUtils.getResourceMetadata(resourceName);
    } else {
      throw new RuntimeException("Invalid INDEXED_DATA_TYPE " + indexDataType);
    }
    try {
      if (!disableLegacyArchiver) {
        return archiver.download(serviceName, resource);
      } else {
        return incArchiver.download(serviceName, resource);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  void cleanupDownloadedArtifacts(
      String resourceName, INDEXED_DATA_TYPE indexDataType, boolean disableLegacyArchiver) {
    String resource;
    if (indexDataType.equals(INDEXED_DATA_TYPE.DATA)) {
      resource = IndexBackupUtils.getResourceData(resourceName);
    } else if (indexDataType.equals(INDEXED_DATA_TYPE.STATE)) {
      resource = IndexBackupUtils.getResourceMetadata(resourceName);
    } else {
      throw new RuntimeException("Invalid INDEXED_DATA_TYPE " + indexDataType);
    }
    logger.info("Cleaning up local index resource: " + resource);
    if (!disableLegacyArchiver) {
      archiver.deleteLocalFiles(resource);
    } else {
      incArchiver.deleteLocalFiles(resource);
    }
  }

  public static class StartIndexHandlerException extends HandlerException {

    public StartIndexHandlerException(Exception e) {
      super(e);
    }
  }
}
