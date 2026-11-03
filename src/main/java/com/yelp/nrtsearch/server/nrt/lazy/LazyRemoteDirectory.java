/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.nrt.lazy;

import com.yelp.nrtsearch.server.config.LazyRemoteDataConfig;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Directory for replica index data where some files stay in the remote backend. A lazy file {@code
 * _0.vec} is stored locally only as a stub ({@code _0.vec.lazy}, see {@link LazyFileStub}), and
 * this directory makes it look like a normal file: it is listed, has its real length, and opening
 * it returns a {@link LazyRemoteIndexInput} that reads from the remote backend.
 *
 * <p>Only reads are lazy. Files are only written by copying from the remote backend, which uses
 * {@link #writeStub} for lazy files instead of writing the data.
 */
public class LazyRemoteDirectory extends FilterDirectory {
  private static final Logger logger = LoggerFactory.getLogger(LazyRemoteDirectory.class);

  private final Path path;
  private final String service;
  private final RemoteBackend remoteBackend;
  private final RemoteBlockCache cache;
  private final LazyRemoteDataConfig config;

  /**
   * @param in directory to wrap, holding the local files and stubs
   * @param path file system path of the directory
   * @param service service name, used to find files in the remote backend
   * @param remoteBackend backend to read lazy files from
   * @param cache block cache shared by all lazy directories on the node
   * @param config lazy loading config
   */
  public LazyRemoteDirectory(
      Directory in,
      Path path,
      String service,
      RemoteBackend remoteBackend,
      RemoteBlockCache cache,
      LazyRemoteDataConfig config) {
    super(in);
    this.path = path;
    this.service = service;
    this.remoteBackend = remoteBackend;
    this.cache = cache;
    this.config = config;
  }

  /**
   * Find a lazy directory in a chain of wrapped directories.
   *
   * @return the lazy directory, or null if there is none
   */
  public static LazyRemoteDirectory unwrap(Directory dir) {
    while (dir != null) {
      if (dir instanceof LazyRemoteDirectory lazy) {
        return lazy;
      }
      if (dir instanceof FilterDirectory filter) {
        dir = filter.getDelegate();
      } else {
        return null;
      }
    }
    return null;
  }

  /** File system path of the directory. */
  public Path getPath() {
    return path;
  }

  public LazyRemoteDataConfig getConfig() {
    return config;
  }

  /** If a file should be stored as a stub instead of copied. */
  public boolean isLazyFile(String fileName) {
    return config.isLazyFile(fileName);
  }

  /** If the directory has a stub for a file. */
  public boolean hasStub(String fileName) {
    return Files.exists(path.resolve(LazyFileStub.stubName(fileName)));
  }

  /**
   * Store a file as a stub instead of copying its data.
   *
   * @param fileName index file name
   * @param indexIdentifier remote index identifier
   * @param fileMetaData metadata of the file in the remote backend
   */
  public void writeStub(String fileName, String indexIdentifier, NrtFileMetaData fileMetaData)
      throws IOException {
    if (fileMetaData.compressionType != null && !"NONE".equals(fileMetaData.compressionType)) {
      throw new IllegalArgumentException(
          "Cannot lazy load compressed file " + fileName + ": " + fileMetaData.compressionType);
    }
    new LazyFileStub(indexIdentifier, fileMetaData).write(path, fileName);
  }

  /**
   * Write stubs for the lazy files of a restored index version, before the directory is opened.
   *
   * @param dir local index directory
   * @param config lazy loading config
   * @param indexIdentifier remote index identifier
   * @param files all files of the index version
   * @return names of the files written as stubs, which must not be downloaded
   */
  public static Set<String> writeStubsForRestore(
      Path dir,
      LazyRemoteDataConfig config,
      String indexIdentifier,
      java.util.Map<String, NrtFileMetaData> files)
      throws IOException {
    Set<String> stubbed = new LinkedHashSet<>();
    for (java.util.Map.Entry<String, NrtFileMetaData> entry : files.entrySet()) {
      NrtFileMetaData meta = entry.getValue();
      boolean compressed = meta.compressionType != null && !"NONE".equals(meta.compressionType);
      if (config.isLazyFile(entry.getKey()) && !compressed) {
        new LazyFileStub(indexIdentifier, meta).write(dir, entry.getKey());
        stubbed.add(entry.getKey());
      }
    }
    return stubbed;
  }

  @Override
  public String[] listAll() throws IOException {
    String[] files = in.listAll();
    Set<String> result = new LinkedHashSet<>();
    for (String file : files) {
      if (LazyFileStub.isStubName(file)) {
        result.add(LazyFileStub.fileNameForStub(file));
      } else if (!file.endsWith(LazyFileStub.STUB_SUFFIX + ".tmp")) {
        result.add(file);
      }
    }
    String[] out = result.toArray(new String[0]);
    Arrays.sort(out);
    return out;
  }

  @Override
  public long fileLength(String name) throws IOException {
    LazyFileStub stub = LazyFileStub.read(path, name);
    if (stub != null) {
      return stub.fileMetaData.length;
    }
    return in.fileLength(name);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    LazyFileStub stub;
    try {
      stub = LazyFileStub.read(path, name);
    } catch (NoSuchFileException | FileNotFoundException e) {
      stub = null;
    }
    if (stub == null) {
      return in.openInput(name, context);
    }
    NrtFileMetaData meta = stub.fileMetaData;
    String indexIdentifier = stub.indexIdentifier;
    String fileId = indexIdentifier + "/" + S3Backend.getIndexBackendFileName(name, meta);
    return new LazyRemoteIndexInput(
        "LazyRemoteIndexInput(" + name + ")",
        fileId,
        meta.length,
        meta.header,
        meta.footer,
        (offset, length) ->
            remoteBackend.readIndexFileRange(service, indexIdentifier, name, meta, offset, length),
        cache);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    Path stubPath = path.resolve(LazyFileStub.stubName(name));
    LazyFileStub stub = Files.exists(stubPath) ? LazyFileStub.read(path, name) : null;
    if (stub != null) {
      in.deleteFile(LazyFileStub.stubName(name));
      cache.invalidate(
          stub.indexIdentifier + "/" + S3Backend.getIndexBackendFileName(name, stub.fileMetaData));
      // a real file may also exist if the file was copied before lazy loading was enabled
      if (Arrays.asList(in.listAll()).contains(name)) {
        in.deleteFile(name);
      }
      return;
    }
    in.deleteFile(name);
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    // a real file replaces a stub of the same name
    if (hasStub(dest)) {
      in.deleteFile(LazyFileStub.stubName(dest));
    }
    in.rename(source, dest);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    java.util.List<String> real = new java.util.ArrayList<>();
    for (String name : names) {
      real.add(hasStub(name) ? LazyFileStub.stubName(name) : name);
    }
    in.sync(real);
  }

  @Override
  public Set<String> getPendingDeletions() throws IOException {
    return in.getPendingDeletions();
  }

  @Override
  public String toString() {
    return "LazyRemoteDirectory(" + in + ", extensions=" + config.getExtensions() + ")";
  }

  /** Log cache statistics, for debugging. */
  public void logStats() {
    logger.info(
        "Block cache: {} bytes, hits={}, misses={}, remoteBytes={}, evictions={}",
        cache.getCurrentBytes(),
        cache.getHits(),
        cache.getMisses(),
        cache.getRemoteBytes(),
        cache.getEvictions());
  }
}
