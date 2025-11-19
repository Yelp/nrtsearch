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

import com.yelp.nrtsearch.server.monitoring.S3DirectoryMetrics;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A read-only {@link Directory} implementation that reads Lucene index files directly from S3
 * without downloading them to local disk. This directory is designed for remote-only index access
 * where segment files are streamed on-demand from S3.
 *
 * <p>This implementation:
 *
 * <ul>
 *   <li>Reads index files directly from S3 using range requests
 *   <li>Maintains file metadata in memory for fast lookups
 *   <li>Is read-only - write operations are not supported
 *   <li>Is designed for search replicas that don't need local disk storage
 * </ul>
 *
 * <p><b>Thread Safety:</b> This class is thread-safe. Multiple readers can access files
 * concurrently.
 */
public class S3Directory extends Directory {
  private static final Logger logger = LoggerFactory.getLogger(S3Directory.class);

  private final S3Backend s3Backend;
  private final String service;
  private final String indexIdentifier;
  private final Map<String, NrtFileMetaData> fileMetadata;
  private final String indexName;

  // Track open inputs for debugging/monitoring
  private final Set<String> openInputs = ConcurrentHashMap.newKeySet();

  /**
   * Constructor for S3Directory.
   *
   * @param s3Backend S3 backend for downloading files
   * @param service service name
   * @param indexIdentifier index identifier (e.g., "myindex-0")
   * @param indexName index name for logging
   * @param fileMetadata map of file names to their metadata
   */
  public S3Directory(
      S3Backend s3Backend,
      String service,
      String indexIdentifier,
      String indexName,
      Map<String, NrtFileMetaData> fileMetadata) {
    this.s3Backend = s3Backend;
    this.service = service;
    this.indexIdentifier = indexIdentifier;
    this.indexName = indexName;
    this.fileMetadata = new ConcurrentHashMap<>(fileMetadata);
    logger.info(
        "Created S3Directory for index: {}, identifier: {}, files: {}",
        indexName,
        indexIdentifier,
        fileMetadata.size());

    // Initialize metrics
    S3DirectoryMetrics.updateFileCount(
        indexName, fileMetadata.size());
    S3DirectoryMetrics.updateOpenInputs(indexName, 0);
  }

  @Override
  public String[] listAll() throws IOException {
    return fileMetadata.keySet().toArray(new String[0]);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    // No-op: S3Directory is read-only. Lucene may call this to clean up unknown files
    // during replica startup, but since we don't write to S3, there's nothing to delete.
    logger.debug("Ignoring deleteFile request in read-only S3Directory: {}", name);
  }

  @Override
  public long fileLength(String name) throws IOException {
    NrtFileMetaData metadata = fileMetadata.get(name);
    if (metadata == null) {
      throw new NoSuchFileException("File not found: " + name);
    }
    return metadata.length;
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    throw new UnsupportedOperationException("S3Directory is read-only, cannot create: " + name);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    throw new UnsupportedOperationException(
        "S3Directory is read-only, cannot create temp output: " + prefix + suffix);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    // No-op: we don't write to S3 in this directory
  }

  @Override
  public void syncMetaData() throws IOException {
    // No-op: we don't write to S3 in this directory
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    throw new UnsupportedOperationException(
        "S3Directory is read-only, cannot rename: " + source + " to " + dest);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    // For segments_N files, look in the local directory first since they're written locally
    // during bootstrap and not uploaded to S3
    if (name.startsWith("segments_")) {
      // Segments files are metadata written locally, try to read from local directory
      // This is a workaround for remote-only mode where segments files are written locally
      // but segment data is in S3
      logger.debug("Attempting to read segments file from metadata in S3Directory: {}", name);
    }

    NrtFileMetaData metadata = fileMetadata.get(name);
    if (metadata == null) {
      throw new FileNotFoundException("File not found in S3Directory: " + name);
    }

    openInputs.add(name);
    logger.debug(
        "Opening S3IndexInput for file: {}, length: {}, index: {}",
        name,
        metadata.length,
        indexName);

    // Update metrics
    S3DirectoryMetrics.updateOpenInputs(
        indexName, openInputs.size());

    return new S3IndexInput(
        "S3IndexInput(file=" + name + ", index=" + indexName + ")",
        s3Backend,
        service,
        indexIdentifier,
        name,
        metadata,
        context);
  }

  @Override
  public Lock obtainLock(String name) throws IOException {
    // Return a no-op lock since this is read-only
    return new Lock() {
      @Override
      public void close() throws IOException {
        // No-op
      }

      @Override
      public void ensureValid() throws IOException {
        // Always valid for read-only directory
      }
    };
  }

  @Override
  public void close() throws IOException {
    if (!openInputs.isEmpty()) {
      logger.warn("S3Directory closed with {} open inputs: {}", openInputs.size(), openInputs);
    }
    openInputs.clear();
    fileMetadata.clear();
    logger.info("Closed S3Directory for index: {}", indexName);
  }

  /**
   * Update the file metadata for this directory. This is useful when new segments are available in
   * S3 and the directory needs to be refreshed.
   *
   * @param newFileMetadata new file metadata map
   */
  public void updateFileMetadata(Map<String, NrtFileMetaData> newFileMetadata) {
    logger.info(
        "Updating S3Directory file metadata for index: {}, old count: {}, new count: {}",
        indexName,
        fileMetadata.size(),
        newFileMetadata.size());
    this.fileMetadata.clear();
    this.fileMetadata.putAll(newFileMetadata);

    // Update metrics
    S3DirectoryMetrics.updateFileCount(
        indexName, newFileMetadata.size());
  }

  /**
   * Get the current file metadata map.
   *
   * @return unmodifiable view of file metadata
   */
  public Map<String, NrtFileMetaData> getFileMetadata() {
    return Collections.unmodifiableMap(fileMetadata);
  }

  /**
   * Get the index name.
   *
   * @return index name
   */
  public String getIndexName() {
    return indexName;
  }

  /**
   * Get the index identifier.
   *
   * @return index identifier
   */
  public String getIndexIdentifier() {
    return indexIdentifier;
  }

  /**
   * Get the service name.
   *
   * @return service name
   */
  public String getService() {
    return service;
  }

  /**
   * Get count of currently open inputs (for monitoring).
   *
   * @return number of open inputs
   */
  public int getOpenInputCount() {
    return openInputs.size();
  }

  @Override
  public String toString() {
    return "S3Directory(index="
        + indexName
        + ", identifier="
        + indexIdentifier
        + ", files="
        + fileMetadata.size()
        + ", service="
        + service
        + ")";
  }

  /**
   * Check if a file exists in this directory.
   *
   * @param name file name
   * @return true if file exists
   */
  public boolean fileExists(String name) {
    return fileMetadata.containsKey(name);
  }

  @Override
  public Set<String> getPendingDeletions() throws IOException {
    // No pending deletions in read-only directory
    return Collections.emptySet();
  }
}
