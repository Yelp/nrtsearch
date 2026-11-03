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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
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
 * Least recently used cache of file blocks fetched from the remote backend. Shared by every lazy
 * file on the node. Blocks are identified by a file id, which must be unique to the remote file
 * contents (the backend file name is), and the block number.
 *
 * <p>Concurrent misses for the same block are merged into one remote read.
 *
 * <p>Use {@link Memory} to keep blocks on the heap, or {@link Disk} to keep them in local files.
 */
public abstract class RemoteBlockCache {
  private static final Logger logger = LoggerFactory.getLogger(RemoteBlockCache.class);

  /** Loads a block from the remote backend. */
  @FunctionalInterface
  public interface BlockLoader {
    byte[] load() throws IOException;
  }

  /** Key of a cached block. */
  protected record BlockKey(String fileId, long block) {}

  private final long maxBytes;
  private final int blockSize;
  // access ordered map of cached blocks to their size, guarded by this
  private final LinkedHashMap<BlockKey, Integer> lru = new LinkedHashMap<>(16, 0.75f, true);
  private long currentBytes;
  private final Map<BlockKey, Object> loadLocks = new ConcurrentHashMap<>();

  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong misses = new AtomicLong();
  private final AtomicLong remoteBytes = new AtomicLong();
  private final AtomicLong remoteLoadNanos = new AtomicLong();
  private final AtomicLong loadErrors = new AtomicLong();
  private final AtomicLong evictions = new AtomicLong();

  protected RemoteBlockCache(long maxBytes, int blockSize) {
    this.maxBytes = maxBytes;
    this.blockSize = blockSize;
  }

  /**
   * Create a cache from config.
   *
   * @param config lazy remote data config
   * @param defaultDiskDir directory for a disk cache when the config doesn't set one
   */
  public static RemoteBlockCache create(LazyRemoteDataConfig config, Path defaultDiskDir)
      throws IOException {
    return switch (config.getCacheType()) {
      case MEMORY -> new Memory(config.getCacheSizeBytes(), config.getBlockSizeBytes());
      case DISK ->
          new Disk(
              config.getCacheDir().isEmpty() ? defaultDiskDir : Path.of(config.getCacheDir()),
              config.getCacheSizeBytes(),
              config.getBlockSizeBytes());
    };
  }

  // -------------------------------------------------------------------------------------------
  // Storage, implemented by subclasses. Calls are made with the block tracked in the LRU.
  // -------------------------------------------------------------------------------------------

  /** Read a stored block, or null if it is not stored. */
  protected abstract byte[] readStored(BlockKey key) throws IOException;

  /** Store a block. Called outside the cache lock. */
  protected abstract void writeStored(BlockKey key, byte[] data) throws IOException;

  /** Remove a stored block. Called while holding the cache lock. */
  protected abstract void deleteStored(BlockKey key);

  /** Cache type name, for metrics and logs. */
  public abstract String getType();

  // -------------------------------------------------------------------------------------------

  public int getBlockSize() {
    return blockSize;
  }

  public long getMaxBytes() {
    return maxBytes;
  }

  /**
   * Get a block, loading it if it is not cached.
   *
   * @param fileId unique id of the remote file
   * @param block block number
   * @param loader loads the block from the remote backend on a miss
   * @return block contents, which must not be modified
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
        long start = System.nanoTime();
        byte[] data;
        try {
          data = loader.load();
        } catch (IOException | RuntimeException e) {
          loadErrors.incrementAndGet();
          throw e;
        } finally {
          remoteLoadNanos.addAndGet(System.nanoTime() - start);
        }
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
      // also marks the block as recently used
      if (lru.get(key) == null) {
        return null;
      }
    }
    return readStored(key);
  }

  private void store(BlockKey key, byte[] data) throws IOException {
    if (data.length > maxBytes) {
      return;
    }
    writeStored(key, data);
    synchronized (this) {
      Integer previous = lru.put(key, data.length);
      currentBytes += data.length - (previous == null ? 0 : previous);
      Iterator<Map.Entry<BlockKey, Integer>> it = lru.entrySet().iterator();
      while (currentBytes > maxBytes && it.hasNext()) {
        Map.Entry<BlockKey, Integer> eldest = it.next();
        it.remove();
        currentBytes -= eldest.getValue();
        evictions.incrementAndGet();
        deleteStored(eldest.getKey());
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
          deleteStored(entry.getKey());
        }
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // Stats
  // -------------------------------------------------------------------------------------------

  public synchronized long getCurrentBytes() {
    return currentBytes;
  }

  public synchronized int getBlockCount() {
    return lru.size();
  }

  public long getHits() {
    return hits.get();
  }

  public long getMisses() {
    return misses.get();
  }

  /** Bytes read from the remote backend. */
  public long getRemoteBytes() {
    return remoteBytes.get();
  }

  /** Total time spent loading blocks from the remote backend. */
  public long getRemoteLoadNanos() {
    return remoteLoadNanos.get();
  }

  public long getLoadErrors() {
    return loadErrors.get();
  }

  public long getEvictions() {
    return evictions.get();
  }

  // -------------------------------------------------------------------------------------------
  // Implementations
  // -------------------------------------------------------------------------------------------

  /** Keeps blocks on the heap. */
  public static class Memory extends RemoteBlockCache {
    private final Map<BlockKey, byte[]> blocks = new ConcurrentHashMap<>();

    public Memory(long maxBytes, int blockSize) {
      super(maxBytes, blockSize);
    }

    @Override
    protected byte[] readStored(BlockKey key) {
      return blocks.get(key);
    }

    @Override
    protected void writeStored(BlockKey key, byte[] data) {
      blocks.put(key, data);
    }

    @Override
    protected void deleteStored(BlockKey key) {
      blocks.remove(key);
    }

    @Override
    public String getType() {
      return "memory";
    }
  }

  /**
   * Keeps blocks in files in a local directory. Allows a larger cache than the heap would, at the
   * cost of a file read per hit (usually served by the OS page cache). The directory is emptied on
   * creation, since blocks from a previous run may be for files that changed.
   */
  public static class Disk extends RemoteBlockCache {
    private final Path dir;

    public Disk(Path dir, long maxBytes, int blockSize) throws IOException {
      super(maxBytes, blockSize);
      this.dir = dir;
      Files.createDirectories(dir);
      try (Stream<Path> files = Files.list(dir)) {
        for (Path file : (Iterable<Path>) files::iterator) {
          Files.deleteIfExists(file);
        }
      }
    }

    @Override
    protected byte[] readStored(BlockKey key) throws IOException {
      try {
        return Files.readAllBytes(blockPath(key));
      } catch (NoSuchFileException e) {
        // Evicted after the LRU check. A block being stored again can also race with the
        // eviction of its previous copy; either way the result is only a cache miss.
        return null;
      }
    }

    @Override
    protected void writeStored(BlockKey key, byte[] data) throws IOException {
      Path path = blockPath(key);
      Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
      Files.write(tmp, data);
      Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    protected void deleteStored(BlockKey key) {
      try {
        Files.deleteIfExists(blockPath(key));
      } catch (IOException e) {
        logger.warn("Could not delete cached block {}", key, e);
      }
    }

    @Override
    public String getType() {
      return "disk";
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
  }
}
