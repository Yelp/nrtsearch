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

import com.google.gson.Gson;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.utils.Archiver;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupIndexRequestHandler implements Handler<BackupIndexRequest, BackupIndexResponse> {
  private static final String BACKUP_INDICATOR_FILE_NAME = "backup.txt";
  Logger logger = LoggerFactory.getLogger(BackupIndexRequestHandler.class);
  private final Archiver archiver;
  private final Path archiveDirectory;
  private final Path backupIndicatorFilePath;
  private final Lock lock = new ReentrantLock();

  public BackupIndexRequestHandler(Archiver archiver, String archiveDirectory) {
    this.archiver = archiver;
    this.archiveDirectory = Paths.get(archiveDirectory);
    this.backupIndicatorFilePath = Paths.get(archiveDirectory, BACKUP_INDICATOR_FILE_NAME);
  }

  @Override
  public BackupIndexResponse handle(IndexState indexState, BackupIndexRequest backupIndexRequest)
      throws HandlerException {
    BackupIndexResponse.Builder backupIndexResponseBuilder = BackupIndexResponse.newBuilder();
    if (!lock.tryLock()) {
      throw new IllegalStateException(
          String.format(
              "A backup is ongoing for index %s, please try again after the current backup is finished",
              getIndexNameOfInterruptedBackup()));
    }
    String indexName = backupIndexRequest.getIndexName();
    SnapshotId snapshotId = null;
    try {
      // only upload metadata in case we are replica
      if (indexState.getShard(0).isReplica()) {
        uploadMetadata(
            backupIndexRequest.getServiceName(),
            backupIndexRequest.getResourceName(),
            indexState,
            backupIndexResponseBuilder);
      } else {
        indexState.commit();

        CreateSnapshotRequest createSnapshotRequest =
            CreateSnapshotRequest.newBuilder().setIndexName(indexName).build();

        snapshotId =
            new CreateSnapshotHandler()
                .createSnapshot(indexState, createSnapshotRequest)
                .getSnapshotId();

        createBackupIndicator(indexName, snapshotId);

        uploadArtifacts(
            backupIndexRequest.getServiceName(),
            backupIndexRequest.getResourceName(),
            indexState,
            backupIndexResponseBuilder);
      }

    } catch (IOException e) {
      logger.error(
          String.format(
              "Error while trying to backup index %s with serviceName %s, resourceName %s",
              indexName, backupIndexRequest.getServiceName(), backupIndexRequest.getResourceName()),
          e);
      return backupIndexResponseBuilder.build();
    } finally {
      if (snapshotId != null) {
        releaseSnapshot(indexState, indexName, snapshotId);
        deleteBackupIndicator();
      }
      lock.unlock();
    }

    return backupIndexResponseBuilder.build();
  }

  public boolean wasBackupPotentiallyInterrupted() {
    return Files.exists(backupIndicatorFilePath);
  }

  public String getIndexNameOfInterruptedBackup() {
    return readBackupIndicatorDetails().indexName;
  }

  private synchronized void createBackupIndicator(String indexName, SnapshotId snapshotId) {
    if (wasBackupPotentiallyInterrupted()) {
      throw new IllegalStateException(
          String.format(
              "A backup is ongoing for index %s, please try again after the current backup is finished",
              getIndexNameOfInterruptedBackup()));
    }

    if (!Files.exists(backupIndicatorFilePath.getParent())) {
      try {
        Files.createDirectories(backupIndicatorFilePath.getParent());
        IOUtils.fsync(backupIndicatorFilePath.getParent(), true);
      } catch (IOException e) {
        throw new IllegalStateException(
            "Unable to create parent directories for backup indicator file "
                + backupIndicatorFilePath,
            e);
      }
    }

    try (Writer writer = Files.newBufferedWriter(backupIndicatorFilePath)) {
      BackupIndicatorDetails backupInfo =
          new BackupIndicatorDetails(
              indexName,
              snapshotId.getIndexGen(),
              snapshotId.getStateGen(),
              snapshotId.getTaxonomyGen());
      writer.write(new Gson().toJson(backupInfo));
      writer.flush();
      IOUtils.fsync(backupIndicatorFilePath.getParent(), false);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to create backup indicator file", e);
    }
  }

  private void deleteBackupIndicator() {
    if (Files.exists(backupIndicatorFilePath)) {
      try {
        Files.delete(backupIndicatorFilePath);
      } catch (IOException e) {
        throw new IllegalStateException("Unable to delete backup indicator file", e);
      }
    }
  }

  public void interruptedBackupCleanup(IndexState indexState, Set<Long> allSnapshotIndexGen) {
    BackupIndicatorDetails details = readBackupIndicatorDetails();
    releaseLeftoverSnapshot(indexState, allSnapshotIndexGen, details);
    deleteTemporaryBackupFilesIfAny();
    deleteBackupIndicator();
  }

  private BackupIndicatorDetails readBackupIndicatorDetails() {
    try {
      return new Gson()
          .fromJson(Files.readString(backupIndicatorFilePath), BackupIndicatorDetails.class);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to read backup indicator file", e);
    }
  }

  private void releaseLeftoverSnapshot(
      IndexState indexState,
      Set<Long> allSnapshotIndexGen,
      BackupIndicatorDetails backupIndicatorDetails) {
    if (allSnapshotIndexGen.contains(backupIndicatorDetails.indexGen)) {
      String indexName = backupIndicatorDetails.indexName;
      SnapshotId snapshotId =
          SnapshotId.newBuilder()
              .setIndexGen(backupIndicatorDetails.indexGen)
              .setStateGen(backupIndicatorDetails.stateGen)
              .setTaxonomyGen(backupIndicatorDetails.taxonomyGen)
              .build();
      logger.info("Releasing snapshot: {} for index: {}", snapshotId, indexName);
      releaseSnapshot(indexState, indexName, snapshotId);
    }
  }

  private void deleteTemporaryBackupFilesIfAny() {
    try {
      Files.walk(archiveDirectory)
          .filter(file -> file.startsWith(archiveDirectory))
          .filter(file -> file.toString().endsWith(".tmp"))
          .forEach(
              file -> {
                try {
                  logger.info("Deleting temporary file: {}", file);
                  Files.delete(file);
                } catch (IOException e) {
                  logger.error("Unable to delete temporary file: {}", file, e);
                }
              });
    } catch (IOException e) {
      throw new IllegalStateException("Unable to read files in archive directory", e);
    }
  }

  public void uploadArtifacts(
      String serviceName,
      String resourceName,
      IndexState indexState,
      BackupIndexResponse.Builder backupIndexResponseBuilder)
      throws IOException {
    String resourceData = IndexBackupUtils.getResourceData(resourceName);
    String versionHash = archiver.upload(serviceName, resourceData, indexState.rootDir);
    archiver.blessVersion(serviceName, resourceData, versionHash);
    backupIndexResponseBuilder.setDataVersionHash(versionHash);

    uploadMetadata(serviceName, resourceName, indexState, backupIndexResponseBuilder);
  }

  public void uploadMetadata(
      String serviceName,
      String resourceName,
      IndexState indexState,
      BackupIndexResponse.Builder backupIndexResponseBuilder)
      throws IOException {
    String resourceMetadata = IndexBackupUtils.getResourceMetadata(resourceName);
    String versionHash =
        archiver.upload(serviceName, resourceMetadata, indexState.globalState.stateDir);
    archiver.blessVersion(serviceName, resourceMetadata, versionHash);
    backupIndexResponseBuilder.setMetadataVersionHash(versionHash);
  }

  private void releaseSnapshot(IndexState indexState, String indexName, SnapshotId snapshotId) {
    ReleaseSnapshotRequest releaseSnapshotRequest =
        ReleaseSnapshotRequest.newBuilder()
            .setIndexName(indexName)
            .setSnapshotId(snapshotId)
            .build();
    ReleaseSnapshotResponse releaseSnapshotResponse =
        new ReleaseSnapshotHandler().handle(indexState, releaseSnapshotRequest);
  }

  private static class BackupIndicatorDetails {
    String indexName;
    long indexGen;
    long stateGen;
    long taxonomyGen;

    public BackupIndicatorDetails(
        String indexName, long indexGen, long stateGen, long taxonomyGen) {
      this.indexName = indexName;
      this.indexGen = indexGen;
      this.stateGen = stateGen;
      this.taxonomyGen = taxonomyGen;
    }
  }
}
