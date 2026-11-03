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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Least recently used cache of file blocks fetched from the remote backend, stored on local disk.
 * Shared by every lazy file on the node. Blocks are identified by a file id, which must be unique
 * to the remote file contents (the backend file name is), and the block number.
 *
 * <p>Concurrent misses for the same block are merged into one remote read.
 */
public class RemoteBlockCache {
  private static final Logger logger = LoggerFactory.getLogger(RemoteBlockCache.class);

  /** Loads a block from the remote backend. */
  @FunctionalInterface
  public interface BlockLoader {
    byte[] load() throws IOException;
  }

  private record BlockKey(String fileId, long block) {}

  private final Path dir;
  private final long maxBytes;
  private final int blockSize;
  // access ordered map of cached blocks to their size, guarded by this
  private final LinkedHashMap<BlockKey, Integer> lru = new LinkedHashMap<>(16, 0.75f, true);
  private long currentBytes;
  private final Map<BlockKey, Object> loadLocks = new ConcurrentHashMap<>();

  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong misses = new AtomicLong();
  private final AtomicLong remoteBytes = new AtomicLong();
  private final AtomicLong evictions = new AtomicLong();

  /**
   * @param dir directory to store blocks in, emptied on creation
   * @param maxBytes maximum total size of cached blocks
   * @param blockSize size of each block
   */
  public RemoteBlockCache(Path dir, long maxBytes, int blockSize) throws IOException {
    this.dir = dir;
    this.maxBytes = maxBytes;
    this.blockSize = blockSize;
    Files.createDirectories(dir);
    // blocks from a previous run may be for files that changed, start empty
    try (Stream<Path> files = Files.list(dir)) {
      for (Path file : (Iterable<Path>) files::iterator) {
        Files.deleteIfExists(file);
      }
    }
  }

  public int getBlockSize() {
    return blockSize;
  }

  /**
   * Get a block, loading it if it is not cached.
   *
   * @param fileId unique id of the remote file
   * @param block block number
   * @param loader loads the block from the remote backend on a miss
   * @return block contents
   */
  public byte[] getBlock(String fileId, long block, BlockLoader loader) throws IOException {
    BlockKey key = new BlockKey(fileId, block);
    byte[] cached = readCached(key);
    if (cached != null) {
      hits.incrementAndGet();
      return cached;
    }
    Object lock = loadLocks.computeIfAbsent(key, k -> new Object());
    try {
      synchronized (lock) {
        // another thread may have loaded it while we waited
        cached = readCached(key);
        if (cached != null) {
          hits.incrementAndGet();
          return cached;
        }
        misses.incrementAndGet();
        byte[] data = loader.load();
        remoteBytes.addAndGet(data.length);
        store(key, data);
        return data;
      }
    } finally {
      loadLocks.remove(key, lock);
    }
  }

  private byte[] readCached(BlockKey key) throws IOException {
    synchronized (this) {
      if (lru.get(key) == null) {
        return null;
      }
    }
    try {
      return Files.readAllBytes(blockPath(key));
    } catch (java.nio.file.NoSuchFileException e) {
      // evicted between the check and the read
      return null;
    }
  }

  private void store(BlockKey key, byte[] data) throws IOException {
    if (data.length > maxBytes) {
      return;
    }
    Path path = blockPath(key);
    Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
    Files.write(tmp, data);
    Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    synchronized (this) {
      Integer previous = lru.put(key, data.length);
      currentBytes += data.length - (previous == null ? 0 : previous);
      evictIfNeeded();
    }
  }

  private void evictIfNeeded() {
    Iterator<Map.Entry<BlockKey, Integer>> it = lru.entrySet().iterator();
    while (currentBytes > maxBytes && it.hasNext()) {
      Map.Entry<BlockKey, Integer> eldest = it.next();
      it.remove();
      currentBytes -= eldest.getValue();
      evictions.incrementAndGet();
      try {
        Files.deleteIfExists(blockPath(eldest.getKey()));
      } catch (IOException e) {
        logger.warn("Could not delete cached block {}", eldest.getKey(), e);
      }
    }
  }

  /** Remove all cached blocks of a file, used when the file is deleted. */
  public void invalidate(String fileId) {
    synchronized (this) {
      Iterator<Map.Entry<BlockKey, Integer>> it = lru.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<BlockKey, Integer> entry = it.next();
        if (entry.getKey().fileId().equals(fileId)) {
          it.remove();
          currentBytes -= entry.getValue();
          try {
            Files.deleteIfExists(blockPath(entry.getKey()));
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      }
    }
  }

  private Path blockPath(BlockKey key) {
    // file ids may contain characters that are not safe in file names
    return dir.resolve(
        Integer.toHexString(key.fileId().hashCode())
            + "_"
            + sanitize(key.fileId())
            + "_"
            + key.block());
  }

  private static String sanitize(String s) {
    StringBuilder sb = new StringBuilder(s.length());
    for (char c : s.toCharArray()) {
      sb.append(Character.isLetterOrDigit(c) || c == '.' || c == '-' ? c : '_');
    }
    return sb.length() > 150 ? sb.substring(sb.length() - 150) : sb.toString();
  }

  public synchronized long getCurrentBytes() {
    return currentBytes;
  }

  public long getHits() {
    return hits.get();
  }

  public long getMisses() {
    return misses.get();
  }

  public long getRemoteBytes() {
    return remoteBytes.get();
  }

  public long getEvictions() {
    return evictions.get();
  }
}
