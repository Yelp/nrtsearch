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
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.grpc.BackupIndexRequest;
import com.yelp.nrtsearch.server.grpc.BackupIndexResponse;
import com.yelp.nrtsearch.server.grpc.CreateSnapshotRequest;
import com.yelp.nrtsearch.server.grpc.ReleaseSnapshotRequest;
import com.yelp.nrtsearch.server.grpc.ReleaseSnapshotResponse;
import com.yelp.nrtsearch.server.grpc.SnapshotId;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* The functionality of this class will be used only within the scope of Commit. Where commit will take care of uploading
 * files to remote storage*/
@Deprecated
public class BackupIndexRequestHandler implements Handler<BackupIndexRequest, BackupIndexResponse> {
  private static final String BACKUP_INDICATOR_FILE_NAME = "backup.txt";
  private static final ReentrantLock LOCK = new ReentrantLock();
  private static String lastBackedUpIndex = "";
  private final boolean disableLegacy;
  private final Archiver incrementalArchiver;
  Logger logger = LoggerFactory.getLogger(BackupIndexRequestHandler.class);
  private final Archiver archiver;
  private final Path archiveDirectory;
  private final Path backupIndicatorFilePath;

  public BackupIndexRequestHandler(
      Archiver archiver,
      Archiver incrementalArchiver,
      String archiveDirectory,
      boolean disableLegacy) {
    this.archiver = archiver;
    this.incrementalArchiver = incrementalArchiver;
    this.archiveDirectory = Paths.get(archiveDirectory);
    this.disableLegacy = disableLegacy;
    this.backupIndicatorFilePath = Paths.get(archiveDirectory, BACKUP_INDICATOR_FILE_NAME);
  }

  @Override
  public BackupIndexResponse handle(IndexState indexState, BackupIndexRequest backupIndexRequest)
      throws HandlerException {
    BackupIndexResponse.Builder backupIndexResponseBuilder = BackupIndexResponse.newBuilder();
    if (!LOCK.tryLock()) {
      throw new IllegalStateException(
          String.format(
              "A backup is ongoing for index %s, please try again after the current backup is finished",
              lastBackedUpIndex));
    }
    String indexName = backupIndexRequest.getIndexName();
    SnapshotId snapshotId = null;
    try {
      if (wasBackupPotentiallyInterrupted()) {
        LOCK.unlock();
        throw new IllegalStateException(
            String.format(
                "A backup is ongoing for index %s, please try again after the current backup is finished",
                readBackupIndicatorDetails().indexName));
      }

      lastBackedUpIndex = indexName;

      // only upload metadata in case we are replica
      if (indexState.getShard(0).isReplica()) {
        uploadMetadata(
            backupIndexRequest.getServiceName(),
            backupIndexRequest.getResourceName(),
            indexState,
            backupIndexResponseBuilder,
            backupIndexRequest.getStream());
      } else {
        indexState.commit(false);

        CreateSnapshotRequest createSnapshotRequest =
            CreateSnapshotRequest.newBuilder().setIndexName(indexName).build();

        snapshotId =
            new CreateSnapshotHandler()
                .createSnapshot(indexState, createSnapshotRequest)
                .getSnapshotId();

        createBackupIndicator(indexName, snapshotId);
        LOCK.unlock();

        Collection<String> segmentFiles;
        List<String> stateDirectory;

        if (backupIndexRequest.getCompleteDirectory()) {
          segmentFiles = Collections.emptyList();
          stateDirectory = Collections.emptyList();
        } else {
          segmentFiles = getSegmentFilesInSnapshot(indexState, snapshotId);
          stateDirectory = Collections.singletonList(indexState.getStateDirectoryPath().toString());
        }

        uploadArtifacts(
            backupIndexRequest.getServiceName(),
            backupIndexRequest.getResourceName(),
            indexState,
            backupIndexResponseBuilder,
            segmentFiles,
            stateDirectory,
            backupIndexRequest.getStream(),
            snapshotId);
      }

    } catch (IOException e) {
      logger.error(
          String.format(
              "Error while trying to backup index %s with serviceName %s, resourceName %s",
              indexName, backupIndexRequest.getServiceName(), backupIndexRequest.getResourceName()),
          e);
      return backupIndexResponseBuilder.build();
    } finally {
      if (LOCK.isHeldByCurrentThread()) {
        LOCK.unlock();
      }
      if (snapshotId != null) {
        releaseSnapshot(indexState, indexName, snapshotId);
        deleteBackupIndicator();
      }
    }

    return backupIndexResponseBuilder.build();
  }

  public static Collection<String> getSegmentFilesInSnapshot(
      IndexState indexState, SnapshotId snapshotId) throws IOException {
    String snapshotIdAsString = CreateSnapshotHandler.getSnapshotIdAsString(snapshotId);
    IndexState.Gens snapshot = new IndexState.Gens(snapshotIdAsString);
    if (indexState.getShards().size() != 1) {
      throw new IllegalStateException(
          String.format(
              "%s shards found index %s instead of exactly 1",
              indexState.getShards().size(), indexState.getName()));
    }
    ShardState state = indexState.getShards().entrySet().iterator().next().getValue();
    SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy = null;
    IndexReader indexReader = null;
    try {
      searcherAndTaxonomy = state.acquire();
      indexReader =
          DirectoryReader.openIfChanged(
              (DirectoryReader) searcherAndTaxonomy.searcher.getIndexReader(),
              state.snapshots.getIndexCommit(snapshot.indexGen));
      if (!(indexReader instanceof StandardDirectoryReader)) {
        throw new IllegalStateException("Unable to find segments to backup");
      }
      StandardDirectoryReader standardDirectoryReader = (StandardDirectoryReader) indexReader;
      return standardDirectoryReader.getSegmentInfos().files(true);
    } finally {
      if (searcherAndTaxonomy != null) {
        state.release(searcherAndTaxonomy);
      }
      if (indexReader != null) {
        indexReader.close();
      }
    }
  }

  public boolean wasBackupPotentiallyInterrupted() {
    return Files.exists(backupIndicatorFilePath);
  }

  public String getIndexNameOfInterruptedBackup() {
    return readBackupIndicatorDetails().indexName;
  }

  private void createBackupIndicator(String indexName, SnapshotId snapshotId) {
    if (wasBackupPotentiallyInterrupted()) {
      throw new IllegalStateException(
          String.format(
              "A backup is ongoing for index %s, please try again after the current backup is finished",
              getIndexNameOfInterruptedBackup()));
    }

    if (!Files.exists(backupIndicatorFilePath.getParent())) {
      try {
        Files.createDirectories(backupIndicatorFilePath.getParent());
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
      IOUtils.fsync(backupIndicatorFilePath, false);
      IOUtils.fsync(backupIndicatorFilePath.getParent(), true);
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
      BackupIndexResponse.Builder backupIndexResponseBuilder,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude,
      boolean stream,
      SnapshotId snapshotId)
      throws IOException {
    String resourceData = IndexBackupUtils.getResourceData(resourceName);
    String versionHash;
    if (!disableLegacy) {
      versionHash =
          archiver.upload(
              serviceName,
              resourceData,
              indexState.getRootDir(),
              filesToInclude,
              parentDirectoriesToInclude,
              stream);
      archiver.blessVersion(serviceName, resourceData, versionHash);
    } else {
      // incremental backup needs to know current files/segments on disk
      if (filesToInclude.isEmpty()) {
        filesToInclude = getSegmentFilesInSnapshot(indexState, snapshotId);
      }
      versionHash =
          incrementalArchiver.upload(
              serviceName,
              resourceData,
              indexState.getRootDir(),
              filesToInclude,
              parentDirectoriesToInclude,
              stream);
      incrementalArchiver.blessVersion(serviceName, resourceData, versionHash);
    }
    backupIndexResponseBuilder.setDataVersionHash(versionHash);
  }

  public void uploadMetadata(
      String serviceName,
      String resourceName,
      IndexState indexState,
      BackupIndexResponse.Builder backupIndexResponseBuilder,
      boolean stream)
      throws IOException {
    String resourceMetadata = IndexBackupUtils.getResourceMetadata(resourceName);
    String versionHash;
    if (!disableLegacy) {
      versionHash =
          archiver.upload(
              serviceName,
              resourceMetadata,
              indexState.getGlobalState().getStateDir(),
              Collections.emptyList(),
              Collections.emptyList(),
              stream);
      archiver.blessVersion(serviceName, resourceMetadata, versionHash);

    } else {
      versionHash =
          incrementalArchiver.upload(
              serviceName,
              resourceMetadata,
              indexState.getGlobalState().getStateDir(),
              Collections.emptyList(),
              Collections.emptyList(),
              stream);
      incrementalArchiver.blessVersion(serviceName, resourceMetadata, versionHash);
    }
    backupIndexResponseBuilder.setMetadataVersionHash(versionHash);
  }

  public static void releaseSnapshot(
      IndexState indexState, String indexName, SnapshotId snapshotId) {
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
