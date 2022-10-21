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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.yelp.nrtsearch.server.luceneserver.IndexBackupUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexArchiver implements Archiver {
  private static final Logger logger = LoggerFactory.getLogger(IndexArchiver.class);
  public static final String STATE = "state";
  public static final String SHARD_0 = "shard0";
  public static final String INDEX = "index";
  private static final String TMP_SUFFIX = ".tmp";
  private final BackupDiffManager backupDiffManager;
  private final FileCompressAndUploader fileCompressAndUploader;
  private final ContentDownloader contentDownloader;
  private final VersionManager versionManager;
  private final Path archiverDirectory;

  @Inject
  public IndexArchiver(
      BackupDiffManager backupDiffManager,
      FileCompressAndUploader fileCompressAndUploader,
      ContentDownloader contentDownloader,
      VersionManager versionManager,
      Path archiverDirectory) {
    this.backupDiffManager = backupDiffManager;
    this.fileCompressAndUploader = fileCompressAndUploader;
    this.contentDownloader = contentDownloader;
    this.versionManager = versionManager;
    this.archiverDirectory = archiverDirectory;
  }

  @VisibleForTesting
  IndexArchiver() {
    this(null, null, null, null, null);
  }

  private String getVersionHash(String serviceName, String resource) throws IOException {
    final String latestVersion =
        versionManager.getVersionString(serviceName, resource, "_latest_version");
    final String versionHash =
        versionManager.getVersionString(serviceName, resource, latestVersion);
    return versionHash;
  }

  @Override
  public Path download(String serviceName, String resource) throws IOException {
    final Path resourceDestDirectory = archiverDirectory.resolve(resource);
    final Path currentDirectory = resourceDestDirectory.resolve("current");
    final Path tempCurrentLink = resourceDestDirectory.resolve(getTmpName());
    Path relativeVersionDirectory;

    if (IndexBackupUtils.isMetadata(resource)
        || IndexBackupUtils.isBackendGlobalState(resource)
        || IndexBackupUtils.isIndexState(resource)) {
      if (versionManager.getLatestVersionNumber(serviceName, resource) < 0) {
        logger.warn(
            String.format(
                "No prior backups found for service: %s, resource: %s. Nothing to download",
                serviceName, resource));
        return null;
      }
      String versionHash = getVersionHash(serviceName, resource);
      final Path versionDirectory = resourceDestDirectory.resolve(versionHash);
      relativeVersionDirectory = Paths.get(versionHash);
      contentDownloader.getVersionContent(serviceName, resource, versionHash, versionDirectory);

    } else {
      String indexDataResource = getIndexDataResourceName(resource);
      if (versionManager.getLatestVersionNumber(serviceName, indexDataResource) < 0) {
        logger.warn(
            String.format(
                "No prior backups found for service: %s, resource: %s. Nothing to download",
                serviceName, getIndexDataResourceName(resource)));
        return null;
      }
      String versionHash = getVersionHash(serviceName, indexDataResource);
      final Path versionDirectory = resourceDestDirectory.resolve(versionHash);
      relativeVersionDirectory = Paths.get(versionHash);

      final Path downloadedIndexDir = getIndexDataDir(versionDirectory.resolve(resource));
      Files.createDirectories(downloadedIndexDir);
      // get index_data
      Path tmpIndexDir = backupDiffManager.download(serviceName, indexDataResource);
      try (DirectoryStream<Path> ds = Files.newDirectoryStream(tmpIndexDir)) {
        for (Path file : ds) {
          Files.move(
              file, downloadedIndexDir.resolve(file.getFileName()), StandardCopyOption.ATOMIC_MOVE);
        }
        Files.delete(tmpIndexDir);
      }
    }

    try {
      logger.info("Point current version symlink to new resource {}", resource);
      Files.createSymbolicLink(tempCurrentLink, relativeVersionDirectory);
      Files.move(tempCurrentLink, currentDirectory, StandardCopyOption.REPLACE_EXISTING);
    } finally {
      if (Files.exists(tempCurrentLink)) {
        FileUtils.deleteDirectory(tempCurrentLink.toFile());
      }
    }
    return currentDirectory;
  }

  private String getTmpName() {
    return UUID.randomUUID() + TMP_SUFFIX;
  }

  @Override
  public String upload(
      String serviceName,
      String resource,
      Path path,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude,
      boolean stream)
      throws IOException {
    if ((validIndexDir(path))) {
      return backupDiffManager.upload(
          serviceName,
          getIndexDataResourceName(resource),
          getIndexDataDir(path),
          filesToInclude,
          parentDirectoriesToInclude,
          true);
    } else if (validGlobalStateDir(path)
        || IndexBackupUtils.isBackendGlobalState(resource)
        || IndexBackupUtils.isIndexState(resource)) {
      String versionHash = UUID.randomUUID().toString();
      fileCompressAndUploader.upload(serviceName, resource, versionHash, path, true);
      return versionHash;
    } else {
      throw new UnsupportedOperationException(
          "IndexArchiver only supports archiving valid index directory and valid state directory");
    }
  }

  @VisibleForTesting
  boolean validGlobalStateDir(Path stateDir) throws IOException {
    if (Files.exists(stateDir)) {
      try (DirectoryStream<Path> ds = Files.newDirectoryStream(stateDir)) {
        for (Path file : ds) {
          if (Files.isDirectory(file)) {
            return false;
          }
          String fileName = file.getFileName().toString();
          if (!fileName.contains("indices")) {
            return false;
          }
        }
        return true;
      }
    } else {
      return false;
    }
  }

  @VisibleForTesting
  boolean validIndexDir(Path indexRootDir) {
    return Files.exists(getIndexDataDir(indexRootDir));
  }

  public static Path getIndexDataDir(Path indexRootDir) {
    return indexRootDir.resolve(SHARD_0).resolve(INDEX);
  }

  @Override
  public boolean blessVersion(String serviceName, String resource, String versionHash)
      throws IOException {
    if (IndexBackupUtils.isMetadata(resource)
        || IndexBackupUtils.isBackendGlobalState(resource)
        || IndexBackupUtils.isIndexState(resource)) {
      return versionManager.blessVersion(serviceName, resource, versionHash);
    } else {
      return backupDiffManager.blessVersion(
          serviceName, getIndexDataResourceName(resource), versionHash);
    }
  }

  public static String getIndexDataResourceName(String resource) {
    return resource + "_index_data";
  }

  public static String getIndexStateResourceName(String resource) {
    return resource + "_index_state";
  }

  @Override
  public boolean deleteVersion(String serviceName, String resource, String versionHash)
      throws IOException {
    if (IndexBackupUtils.isMetadata(resource)) {
      return versionManager.deleteVersion(serviceName, resource, versionHash);
    } else {
      return versionManager.deleteVersion(
              serviceName, getIndexDataResourceName(resource), versionHash)
          && versionManager.deleteVersion(
              serviceName, getIndexStateResourceName(resource), versionHash);
    }
  }

  @Override
  public boolean deleteLocalFiles(String resource) {
    return deleteLocalResourceFiles(resource, archiverDirectory);
  }

  public static boolean deleteLocalResourceFiles(String resource, Path archiverDirectory) {
    final File resourceDestDirectory = archiverDirectory.resolve(resource).toFile();
    if (!resourceDestDirectory.exists()) {
      return true;
    }
    if (!resourceDestDirectory.isDirectory()) {
      logger.warn("Local resource destination is not a directory: " + resourceDestDirectory);
      return false;
    }
    try {
      // clean up the symlink first, since it isn't always handled properly by deleteDirectory
      Path currentPath = archiverDirectory.resolve(resource).resolve("current");
      if (Files.exists(currentPath, LinkOption.NOFOLLOW_LINKS)) {
        Files.delete(currentPath);
      }
      FileUtils.deleteDirectory(resourceDestDirectory);
    } catch (IOException e) {
      logger.error("Error deleting local files:", e);
      return false;
    }
    return true;
  }

  @Override
  public List<String> getResources(String serviceName) {
    return backupDiffManager.getResources(serviceName);
  }

  @Override
  public List<VersionedResource> getVersionedResource(String serviceName, String resource) {
    return backupDiffManager.getVersionedResource(serviceName, getIndexDataResourceName(resource));
  }
}
