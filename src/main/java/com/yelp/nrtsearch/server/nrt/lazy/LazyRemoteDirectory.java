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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Directory for replica index data where some files stay in the remote backend. A lazy file {@code
 * _0.vec} is stored locally only as a stub ({@code _0.vec.lazy}, see {@link LazyFileStub}), and
 * this directory makes it look like a normal file: it is listed, has its real length, and opening
 * it returns a {@link LazyRemoteIndexInput} that reads from the remote backend.
 *
 * <p>Stubs are kept in memory while the directory is open, loaded from the stub files when it is
 * opened and updated as stubs are written and deleted, so looking up a lazy file needs no file
 * system access.
 *
 * <p>Copy jobs write stubs like any other copied file: to a temp file ({@link #createStubTemp}),
 * which is renamed to the stub when the job finishes, or deleted if the job is cancelled.
 */
public class LazyRemoteDirectory extends FilterDirectory {
  private final Path path;
  private final String service;
  private final RemoteBackend remoteBackend;
  private final RemoteBlockCache cache;
  private final LazyRemoteDataConfig config;
  // index file name -> stub, for every stub file in the directory
  private final Map<String, LazyFileStub> stubs = new ConcurrentHashMap<>();
  // temp file name -> stub, for stubs written by copy jobs and not yet renamed
  private final Map<String, LazyFileStub> pendingStubs = new ConcurrentHashMap<>();

  /**
   * @param in directory to wrap, holding the local files and stubs
   * @param path file system path of the directory
   * @param service service name, used to find files in the remote backend
   * @param remoteBackend backend to read lazy files from
   * @param cache block cache shared by all lazy directories on the node
   * @param config lazy loading config
   * @throws IOException if the existing stubs can't be read
   */
  public LazyRemoteDirectory(
      Directory in,
      Path path,
      String service,
      RemoteBackend remoteBackend,
      RemoteBlockCache cache,
      LazyRemoteDataConfig config)
      throws IOException {
    super(in);
    this.path = path;
    this.service = service;
    this.remoteBackend = remoteBackend;
    this.cache = cache;
    this.config = config;
    for (String file : in.listAll()) {
      if (LazyFileStub.isStubName(file)) {
        String fileName = LazyFileStub.fileNameForStub(file);
        stubs.put(fileName, LazyFileStub.read(path, fileName));
      }
    }
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
    return stubs.containsKey(fileName);
  }

  /**
   * Write a stub for a file to a temp file, as a copy job does for a copied file. Renaming the temp
   * file to the file name ({@link #rename}) makes the stub visible; deleting it discards the stub.
   *
   * @param fileName index file name
   * @param indexIdentifier remote index identifier
   * @param fileMetaData metadata of the file in the remote backend
   * @return temp file name
   */
  public String createStubTemp(
      String fileName, String indexIdentifier, NrtFileMetaData fileMetaData) throws IOException {
    if (fileMetaData.isCompressed()) {
      throw new IllegalArgumentException(
          "Cannot lazy load compressed file " + fileName + ": " + fileMetaData.compressionType);
    }
    LazyFileStub stub = new LazyFileStub(indexIdentifier, fileMetaData);
    byte[] bytes = stub.toBytes();
    String tmpName;
    try (IndexOutput out =
        in.createTempOutput(LazyFileStub.stubName(fileName), "copy", IOContext.DEFAULT)) {
      out.writeBytes(bytes, bytes.length);
      tmpName = out.getName();
    }
    pendingStubs.put(tmpName, stub);
    return tmpName;
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
      Map<String, NrtFileMetaData> files)
      throws IOException {
    Set<String> stubbed = new LinkedHashSet<>();
    for (Map.Entry<String, NrtFileMetaData> entry : files.entrySet()) {
      if (config.isLazyFile(entry.getKey()) && !entry.getValue().isCompressed()) {
        new LazyFileStub(indexIdentifier, entry.getValue()).write(dir, entry.getKey());
        stubbed.add(entry.getKey());
      }
    }
    return stubbed;
  }

  @Override
  public String[] listAll() throws IOException {
    Set<String> result = new LinkedHashSet<>();
    for (String file : in.listAll()) {
      if (LazyFileStub.isStubName(file)) {
        result.add(LazyFileStub.fileNameForStub(file));
      } else {
        // includes temp files, stub or not, which Lucene manages like other temp files
        result.add(file);
      }
    }
    String[] out = result.toArray(new String[0]);
    Arrays.sort(out);
    return out;
  }

  @Override
  public long fileLength(String name) throws IOException {
    LazyFileStub stub = stubs.get(name);
    if (stub != null) {
      return stub.fileMetaData.length;
    }
    return in.fileLength(name);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    LazyFileStub stub = stubs.get(name);
    if (stub == null) {
      return in.openInput(name, context);
    }
    NrtFileMetaData meta = stub.fileMetaData;
    String indexIdentifier = stub.indexIdentifier;
    return new LazyRemoteIndexInput(
        "LazyRemoteIndexInput(" + name + ")",
        stub.fileId(name),
        meta.length,
        meta.header,
        meta.footer,
        (offset, length) ->
            remoteBackend.readIndexFileRange(service, indexIdentifier, name, meta, offset, length),
        cache);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    LazyFileStub stub = stubs.remove(name);
    if (stub != null) {
      in.deleteFile(LazyFileStub.stubName(name));
      cache.invalidate(stub.fileId(name));
      // a real file can sit beside the stub, if it was copied before lazy loading was enabled
      try {
        in.deleteFile(name);
      } catch (NoSuchFileException | FileNotFoundException e) {
        // usual case, only the stub exists
      }
      return;
    }
    // a pending stub temp file deleted by a cancelled copy job
    pendingStubs.remove(name);
    in.deleteFile(name);
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    LazyFileStub pending = pendingStubs.remove(source);
    if (pending != null) {
      // a copy job finishing a stub: the temp file becomes the stub for dest
      in.rename(source, LazyFileStub.stubName(dest));
      LazyFileStub previous = stubs.put(dest, pending);
      if (previous != null) {
        cache.invalidate(previous.fileId(dest));
      }
      return;
    }
    // a real file replaces a stub of the same name
    LazyFileStub previous = stubs.remove(dest);
    if (previous != null) {
      in.deleteFile(LazyFileStub.stubName(dest));
      cache.invalidate(previous.fileId(dest));
    }
    in.rename(source, dest);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    List<String> real = new ArrayList<>(names.size());
    for (String name : names) {
      real.add(stubs.containsKey(name) ? LazyFileStub.stubName(name) : name);
    }
    in.sync(real);
  }

  @Override
  public String toString() {
    return "LazyRemoteDirectory(" + in + ", extensions=" + config.getExtensions() + ")";
  }
}
