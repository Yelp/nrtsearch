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
import com.yelp.nrtsearch.server.grpc.RestoreIndex;
import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import com.yelp.nrtsearch.server.grpc.StartIndexResponse;
import com.yelp.nrtsearch.server.utils.FileUtil;
import java.io.IOException;
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
  private final boolean backupFromIncArchiver;
  private final boolean restoreFromIncArchiver;
  Logger logger = LoggerFactory.getLogger(StartIndexHandler.class);

  public StartIndexHandler(
      Archiver archiver,
      Archiver incArchiver,
      String archiveDirectory,
      boolean backupFromIncArchiver,
      boolean restoreFromIncArchiver) {
    this.archiver = archiver;
    this.incArchiver = incArchiver;
    this.archiveDirectory = archiveDirectory;
    this.backupFromIncArchiver = backupFromIncArchiver;
    this.restoreFromIncArchiver = restoreFromIncArchiver;
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
    final String primaryAddress;
    final int primaryPort;
    Path dataPath = null;
    if (startIndexRequest.hasRestore() && !shardState.isStarted()) {
      synchronized (shardState) {
        try {
          if (!shardState.isRestored()) {
            RestoreIndex restoreIndex = startIndexRequest.getRestore();
            if (restoreIndex.getDeleteExistingData()) {
              indexState.deleteIndexRootDir();
              deleteDownloadedBackupDirectories(restoreIndex.getResourceName());
            }

            dataPath =
                downloadArtifact(
                    restoreIndex.getServiceName(),
                    restoreIndex.getResourceName(),
                    INDEXED_DATA_TYPE.DATA,
                    restoreFromIncArchiver);
            shardState.setRestored(true);
          } else {
            throw new IllegalStateException("Index " + indexState.getName() + " already restored");
          }
        } catch (IOException e) {
          logger.info("Unable to delete existing index data", e);
          throw new StartIndexHandlerException(e);
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
      if (mode.equals(Mode.REPLICA)) {
        indexState.initWarmer(archiver);
      }

      indexState.start(mode, dataPath, primaryGen, primaryAddress, primaryPort);

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

  public static class StartIndexHandlerException extends HandlerException {

    public StartIndexHandlerException(Exception e) {
      super(e);
    }
  }
}
