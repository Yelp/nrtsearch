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

import static org.apache.lucene.util.IOUtils.rm;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.annotations.VisibleForTesting;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiPartArchiverImpl implements Archiver {
  private static final String MULTI_PART_SUFFIX = "_multi";
  private static final String MULTI_PART_META_SUFFIX = "_meta";
  private static final int NUM_MULTI_PART_THREADS = 20;
  private static final Logger logger = LoggerFactory.getLogger(MultiPartArchiverImpl.class);
  public static final String META_VERSIONHASH_FNAME = "meta_versionhash";
  private final Archiver baseArchiver;
  private Path archiverDirectory;
  private AmazonS3 s3;
  private String BUCKET_NAME;
  private final ThreadPoolExecutor executor =
      (ThreadPoolExecutor) Executors.newFixedThreadPool(NUM_MULTI_PART_THREADS);

  private static String getMultiPartResourceName(String resource) {
    return resource + MULTI_PART_SUFFIX;
  }

  @VisibleForTesting
  static String getMultiPartMetaResourceName(String resource) {
    return getMultiPartResourceName(resource) + MULTI_PART_META_SUFFIX;
  }

  public MultiPartArchiverImpl(
      Archiver archiver, Path archiverDirectory, AmazonS3 s3, String BUCKET_NAME) {
    this.baseArchiver = archiver;
    this.archiverDirectory = archiverDirectory;
    this.s3 = s3;
    this.BUCKET_NAME = BUCKET_NAME;
  }

  @Override
  public Path download(String serviceName, String resource) throws IOException {
    Path metaVersionHash =
        baseArchiver.download(serviceName, getMultiPartMetaResourceName(resource));
    Collection<String> versionHashes =
        getVersionHashes(Paths.get(metaVersionHash.toString(), META_VERSIONHASH_FNAME));
    final LinkedList<Future<Path>> futuresOfDownloadedPaths = new LinkedList<>();
    for (String versionHash : versionHashes) {
      futuresOfDownloadedPaths.add(
          executor.submit(
              () -> {
                try {
                  return downloadVersionHash(
                      serviceName, getMultiPartResourceName(resource), versionHash);
                } catch (IOException e) {
                  throw new RuntimeException();
                }
              }));
    }
    List<Path> versionHashDirs = new LinkedList<>();
    while (!futuresOfDownloadedPaths.isEmpty()) {
      try {
        versionHashDirs.add(futuresOfDownloadedPaths.pollFirst().get());
      } catch (Exception e) {
        throw new RuntimeException("Error downloading versionHash", e);
      }
    }
    return null;
  }

  private Path downloadVersionHash(String serviceName, String resource, String versionHash)
      throws IOException {
    if (!Files.exists(archiverDirectory)) {
      logger.info("Archiver directory doesn't exist: " + archiverDirectory + " creating new ");
      Files.createDirectories(archiverDirectory);
    }
    final Path resourceDestDirectory = archiverDirectory.resolve(resource);
    final Path versionDirectory = resourceDestDirectory.resolve(versionHash);
    logger.info(
        "Downloading resource {} for service {} version {} to directory {}",
        resource,
        serviceName,
        versionHash,
        versionDirectory);
    // baseArchiver.getVersionContent(serviceName, resource, versionHash, versionDirectory);
    return versionDirectory;
  }

  private Collection<String> getVersionHashes(Path metaVersionHash) throws IOException {
    // read file in List<String> versionHashes for multi_parts
    String versionHash;
    Collection<String> versionHashes = new LinkedList<>();
    try (BufferedReader br =
        new BufferedReader(new InputStreamReader(new FileInputStream(metaVersionHash.toFile())))) {
      while ((versionHash = br.readLine()) != null) {
        versionHashes.add(versionHash);
      }
    }
    return versionHashes;
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
    // Group files by size
    Collection<Collection<String>> filesGroupBySize =
        getFilesBySizes(filesToInclude, parentDirectoriesToInclude);
    final LinkedList<Future<String>> futures = new LinkedList<>();
    List<String> versionHashes = new ArrayList<>();
    for (Collection<String> fileGroupBySize : filesGroupBySize) {
      futures.add(
          executor.submit(
              () -> {
                try {
                  return baseArchiver.upload(
                      serviceName,
                      getMultiPartResourceName(resource),
                      path,
                      fileGroupBySize,
                      parentDirectoriesToInclude,
                      stream);
                } catch (IOException e) {
                  // TODO: need to catch this upstream and handle appropriately
                  throw new RuntimeException(e);
                }
              }));
    }
    while (!futures.isEmpty()) {
      try {
        versionHashes.add(futures.pollFirst().get());
      } catch (Exception e) {
        throw new RuntimeException("Error downloading file part", e);
      }
    }
    // create InputStream containing one versionHash per line and upload to s3
    File metaVersionHashFile = Paths.get(String.valueOf(path), META_VERSIONHASH_FNAME).toFile();
    try (BufferedWriter bw =
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(metaVersionHashFile)))) {
      for (String versionHash : versionHashes) {
        bw.write(versionHash);
        bw.newLine();
      }
    }
    Collection<String> toInclude = new ArrayList<>();
    toInclude.add(metaVersionHashFile.getName());

    String metaVersionHash =
        baseArchiver.upload(
            serviceName,
            getMultiPartMetaResourceName(resource),
            metaVersionHashFile.toPath(),
            toInclude,
            Collections.emptyList(),
            stream);
    rm(metaVersionHashFile.toPath());

    // return the versionHash of this final metaVersionHash
    return metaVersionHash;
  }

  private Collection<Collection<String>> getFilesBySizes(
      Collection<String> filesToInclude, Collection<String> parentDirectoriesToInclude) {
    // TODO: add real code for grouping files by size
    Collection<Collection<String>> filesBySize = new ArrayList<>();
    for (String fileName : filesToInclude) {
      Collection<String> filesOfSameSize = new ArrayList<>();
      filesOfSameSize.add(fileName);
      filesBySize.add(filesOfSameSize);
    }
    return filesBySize;
  }

  @Override
  public boolean blessVersion(String serviceName, String resource, String versionHash)
      throws IOException {
    return baseArchiver.blessVersion(
        serviceName, getMultiPartMetaResourceName(resource), versionHash);
  }

  @Override
  public boolean deleteVersion(String serviceName, String resource, String versionHash)
      throws IOException {
    return false;
  }

  @Override
  public List<String> getResources(String serviceName) {
    return null;
  }

  @Override
  public List<VersionedResource> getVersionedResource(String serviceName, String resource) {
    return null;
  }
}
