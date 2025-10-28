/*
 * Copyright 2025 Yelp Inc.
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A hybrid {@link Directory} implementation that combines local disk and S3 storage. This directory
 * reads the segments file (segments_*) from local disk and streams all other files (*.si, *.cfs,
 * *.cfe, etc.) from S3.
 *
 * <p>This approach enables remote-only index mode where:
 *
 * <ul>
 *   <li>Bootstrap metadata (segments_N) is stored locally for fast access
 *   <li>Actual segment data files are streamed from S3 on-demand
 *   <li>No local disk space is consumed for large index files
 * </ul>
 *
 * <p><b>Thread Safety:</b> This class is thread-safe if both underlying directories are
 * thread-safe.
 */
public class HybridDirectory extends Directory {
  private static final Logger logger = LoggerFactory.getLogger(HybridDirectory.class);

  private final Directory localDir;
  private final S3Directory s3Dir;
  private final String indexName;

  /**
   * Constructor for HybridDirectory.
   *
   * @param localDir Local directory containing the segments file (segments_*)
   * @param s3Dir S3Directory for streaming all other index files
   * @param indexName Index name for logging
   */
  public HybridDirectory(Directory localDir, S3Directory s3Dir, String indexName) {
    this.localDir = localDir;
    this.s3Dir = s3Dir;
    this.indexName = indexName;
    logger.info("Created HybridDirectory for index: {}", indexName);
  }

  /**
   * Determine if a file should be read from local directory or S3.
   *
   * @param name File name
   * @return true if file should be read from local directory
   */
  private boolean isLocalFile(String name) {
    // Only the segments_* file is stored locally
    // All other files (.si, .cfs, .cfe, etc.) are in S3
    return name.startsWith("segments_");
  }

  @Override
  public String[] listAll() throws IOException {
    Set<String> allFiles = new HashSet<>();
    allFiles.addAll(Arrays.asList(localDir.listAll()));
    allFiles.addAll(Arrays.asList(s3Dir.listAll()));
    return allFiles.toArray(new String[0]);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    // Read-only mode: no deletions allowed
    logger.debug("Ignoring deleteFile request in read-only HybridDirectory: {}", name);
  }

  @Override
  public long fileLength(String name) throws IOException {
    if (isLocalFile(name)) {
      return localDir.fileLength(name);
    } else {
      return s3Dir.fileLength(name);
    }
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    throw new UnsupportedOperationException("HybridDirectory is read-only, cannot create: " + name);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    throw new UnsupportedOperationException(
        "HybridDirectory is read-only, cannot create temp output: " + prefix + suffix);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    // No-op: read-only directory
  }

  @Override
  public void syncMetaData() throws IOException {
    // No-op: read-only directory
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    throw new UnsupportedOperationException(
        "HybridDirectory is read-only, cannot rename: " + source + " to " + dest);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    if (isLocalFile(name)) {
      logger.debug("Opening file from local directory: {}", name);
      return localDir.openInput(name, context);
    } else {
      logger.debug("Opening file from S3: {}", name);
      return s3Dir.openInput(name, context);
    }
  }

  @Override
  public Lock obtainLock(String name) throws IOException {
    // Use local directory's lock
    return localDir.obtainLock(name);
  }

  @Override
  public void close() throws IOException {
    logger.info("Closing HybridDirectory for index: {}", indexName);
    localDir.close();
    s3Dir.close();
  }

  @Override
  public Set<String> getPendingDeletions() throws IOException {
    // No pending deletions in read-only directory
    return Set.of();
  }

  /**
   * Get the local directory.
   *
   * @return local directory
   */
  public Directory getLocalDirectory() {
    return localDir;
  }

  /**
   * Get the S3 directory.
   *
   * @return S3 directory
   */
  public S3Directory getS3Directory() {
    return s3Dir;
  }

  @Override
  public String toString() {
    return "HybridDirectory(index=" + indexName + ", local=" + localDir + ", s3=" + s3Dir + ")";
  }
}
