/*
 * Copyright 2021 Yelp Inc.
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
package com.yelp.nrtsearch.server.backup;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupDiffComputer {
  private final ContentDownloader contentDownloader;
  private final VersionManager versionManager;
  private final Path archiverDirectory;
  private static final Logger logger = LoggerFactory.getLogger(BackupDiffComputer.class);

  public static class BackupDiffInfo {
    private final Set<String> alreadyUploaded;
    private final Set<String> toBeAdded;
    private final Set<String> toBeRemoved;

    public BackupDiffInfo(
        ImmutableSet<String> alreadyUploaded,
        ImmutableSet<String> toBeAdded,
        ImmutableSet<String> toBeRemoved) {
      this.alreadyUploaded = alreadyUploaded;
      this.toBeAdded = toBeAdded;
      this.toBeRemoved = toBeRemoved;
    }

    public static BackupDiffInfo generateBackupDiffInfo(
        Set<String> oldFileNames, Set<String> currentFileNames) {
      return new BackupDiffInfo(
          Sets.intersection(oldFileNames, currentFileNames).immutableCopy(),
          Sets.difference(currentFileNames, oldFileNames).immutableCopy(),
          Sets.difference(oldFileNames, currentFileNames).immutableCopy());
    }

    public Set<String> getToBeAdded() {
      return toBeAdded;
    }

    public Set<String> getAlreadyUploaded() {
      return alreadyUploaded;
    }

    public Set<String> getToBeRemoved() {
      return toBeRemoved;
    }
  }

  public static class TempDirManager implements AutoCloseable {
    private static final String TMP_SUFFIX = ".tmp";
    private final Path tmpDir;

    public TempDirManager(Path baseDir) {
      this.tmpDir = baseDir.resolve(getTmpName());
    }

    private String getTmpName() {
      return UUID.randomUUID() + TMP_SUFFIX;
    }

    private void deleteTempDir() throws IOException {
      if (Files.exists(tmpDir)) {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(tmpDir)) {
          for (Path file : ds) {
            Files.delete(file);
          }
          Files.delete(tmpDir);
        }
      }
    }

    public Path getPath() {
      return tmpDir;
    }

    @Override
    public void close() throws Exception {
      deleteTempDir();
    }
  }

  public static class BackupDiffDirValidator {
    public static Path validateMetaFile(Path downloadedDir) throws IOException {
      List<Path> files = new ArrayList<>();
      if (!Files.exists(downloadedDir)) {
        throw new RuntimeException(
            String.format("File Info Directory %s does not exist locally", downloadedDir));
      }
      try (DirectoryStream<Path> ds = Files.newDirectoryStream(downloadedDir)) {
        for (Path file : ds) {
          if (Files.isDirectory(file)) {
            throw new RuntimeException(
                String.format(
                    "File Info Directory %s cannot contain subdirs: %s", downloadedDir, file));
          }
          files.add(file.getFileName());
        }
        if (files.size() != 1) {
          throw new RuntimeException(
              String.format(
                  "File Info Directory %s: cannot contain multiple files %s",
                  downloadedDir, files.size()));
        }
        return Paths.get(downloadedDir.toString(), files.get(0).toString());
      }
    }
  }

  public static class BackupDiffMarshaller {
    public static List<String> deserializeFileNames(Path backupDiffFile) throws IOException {
      String indexFileName;
      List<String> indexFileNames = new LinkedList<>();
      try (BufferedReader br =
          new BufferedReader(new InputStreamReader(new FileInputStream(backupDiffFile.toFile())))) {
        while ((indexFileName = br.readLine()) != null) {
          indexFileNames.add(indexFileName);
        }
      }
      return indexFileNames;
    }

    public static void serializeFileNames(List<String> indexFileNames, Path destBackupDiffFile)
        throws IOException {
      try (BufferedWriter bw =
          new BufferedWriter(
              new OutputStreamWriter(new FileOutputStream(destBackupDiffFile.toFile())))) {
        for (String indexFileName : indexFileNames) {
          bw.write(indexFileName);
          bw.newLine();
        }
      }
    }
  }

  public BackupDiffComputer(
      final ContentDownloader contentDownloader,
      final VersionManager versionManager,
      final Path archiverDirectory) {
    this.contentDownloader = contentDownloader;
    this.versionManager = versionManager;
    this.archiverDirectory = archiverDirectory;
  }

  public BackupDiffInfo generateDiff(
      String serviceName, String indexName, List<String> currentIndexFileNames) throws IOException {
    // get the latest backup file names
    List<String> backupIndexFileNames = getLatestBackupIdxFileNames(serviceName, indexName);
    BackupDiffInfo backupInfo =
        BackupDiffInfo.generateBackupDiffInfo(
            new HashSet<>(backupIndexFileNames), new HashSet<>(currentIndexFileNames));
    return backupInfo;
  }

  private List<String> getLatestBackupIdxFileNames(String serviceName, String indexName)
      throws IOException {
    final String latestVersion =
        versionManager.getVersionString(serviceName, indexName, "_latest_version");
    final String versionHash =
        versionManager.getVersionString(serviceName, indexName, latestVersion);
    try (TempDirManager tmpDir = new TempDirManager(archiverDirectory)) {
      Path tmpPath = tmpDir.getPath();
      logger.info(
          "Downloading latest file info: index {}, service: {}, version: {} to directory {}",
          indexName,
          serviceName,
          versionHash,
          tmpPath);
      contentDownloader.getVersionContent(serviceName, indexName, versionHash, tmpPath);
      // confirm there is only 1 file within this directory
      Path metaFile = BackupDiffDirValidator.validateMetaFile(tmpPath);
      return BackupDiffMarshaller.deserializeFileNames(metaFile);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
