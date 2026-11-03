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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Least recently used cache of file blocks fetched from the remote backend. Shared by every lazy
 * file on the node. Blocks are identified by a file id, which must be unique to the remote file
 * contents (the backend file name is), and the block number.
 *
 * <p>Readers copy byte ranges out of a block with {@link #read}, so blocks can be stored anywhere:
 * on the heap ({@link Memory}), in native memory outside the heap ({@link OffHeap}), or in local
 * files ({@link Disk}). Concurrent misses for the same block are merged into one remote read.
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

  /** A cached block. {@code handle} locates the stored data and depends on the cache type. */
  protected record Entry(int length, int storedSize, Object handle) {}

  private final long maxBytes;
  private final int blockSize;
  // access ordered map of cached blocks, guarded by this
  private final LinkedHashMap<BlockKey, Entry> lru = new LinkedHashMap<>(16, 0.75f, true);
  // stored size of cached blocks plus blocks being stored, guarded by this
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
      case OFF_HEAP -> new OffHeap(config.getCacheSizeBytes(), config.getBlockSizeBytes());
      case DISK ->
          new Disk(
              config.getCacheDir().isEmpty() ? defaultDiskDir : Path.of(config.getCacheDir()),
              config.getCacheSizeBytes(),
              config.getBlockSizeBytes());
    };
  }

  // -------------------------------------------------------------------------------------------
  // Storage, implemented by subclasses
  // -------------------------------------------------------------------------------------------

  /**
   * Copy bytes out of a stored block.
   *
   * @return false if the block is no longer stored, which counts as a miss
   */
  protected abstract boolean copyStored(
      BlockKey key, Entry entry, int offsetInBlock, byte[] dest, int destOffset, int length)
      throws IOException;

  /**
   * Reserve storage for a block. Called while holding the cache lock, after making room for {@link
   * #storedSize}.
   *
   * @return reservation passed to {@link #writeStored}, or null if storage is not available
   */
  protected Object reserve() {
    return Boolean.TRUE;
  }

  /** Release a reservation that was not used. Called while holding the cache lock. */
  protected void release(Object reservation) {}

  /**
   * Store a block. Called without the cache lock.
   *
   * @return handle to keep in the block's {@link Entry}
   */
  protected abstract Object writeStored(BlockKey key, byte[] data, Object reservation)
      throws IOException;

  /** Remove a stored block. Called while holding the cache lock. */
  protected abstract void deleteStored(BlockKey key, Entry entry);

  /** Bytes of capacity a block of this length uses. */
  protected int storedSize(int length) {
    return length;
  }

  /** Bytes of memory or disk the cache holds, which may include unused capacity. */
  public synchronized long getAllocatedBytes() {
    return currentBytes;
  }

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
   * Copy bytes from a block, loading the block if it is not cached.
   *
   * @param fileId unique id of the remote file
   * @param block block number
   * @param offsetInBlock offset of the first byte to copy within the block
   * @param dest array to copy into
   * @param destOffset offset in dest
   * @param length number of bytes to copy, which must be within the block
   * @param loader loads the whole block from the remote backend on a miss
   */
  public void read(
      String fileId,
      long block,
      int offsetInBlock,
      byte[] dest,
      int destOffset,
      int length,
      BlockLoader loader)
      throws IOException {
    BlockKey key = new BlockKey(fileId, block);
    if (readCached(key, offsetInBlock, dest, destOffset, length)) {
      hits.incrementAndGet();
      return;
    }
    Object lock = loadLocks.computeIfAbsent(key, k -> new Object());
    try {
      synchronized (lock) {
        // another thread may have loaded it while we waited
        if (readCached(key, offsetInBlock, dest, destOffset, length)) {
          hits.incrementAndGet();
          return;
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
        if (offsetInBlock + length > data.length) {
          throw new IllegalArgumentException(
              String.format(
                  "Read of [%d, %d) is outside of block %s with length %d",
                  offsetInBlock, offsetInBlock + length, key, data.length));
        }
        System.arraycopy(data, offsetInBlock, dest, destOffset, length);
        store(key, data);
      }
    } finally {
      loadLocks.remove(key, lock);
    }
  }

  private boolean readCached(BlockKey key, int offsetInBlock, byte[] dest, int destOffset, int len)
      throws IOException {
    Entry entry;
    synchronized (this) {
      // also marks the block as recently used
      entry = lru.get(key);
    }
    return entry != null && copyStored(key, entry, offsetInBlock, dest, destOffset, len);
  }

  private void store(BlockKey key, byte[] data) {
    int size = storedSize(data.length);
    Object reservation;
    synchronized (this) {
      if (lru.containsKey(key) || !makeRoom(size)) {
        return;
      }
      reservation = reserve();
      if (reservation == null) {
        return;
      }
      currentBytes += size;
    }
    Object handle;
    try {
      handle = writeStored(key, data, reservation);
    } catch (IOException | RuntimeException e) {
      // the data was still read, just not cached
      logger.warn("Could not cache block {}", key, e);
      synchronized (this) {
        currentBytes -= size;
        release(reservation);
      }
      return;
    }
    synchronized (this) {
      lru.put(key, new Entry(data.length, size, handle));
    }
  }

  /** Evict blocks until there is room for size more bytes. Called while holding the lock. */
  private boolean makeRoom(int size) {
    if (size > maxBytes) {
      return false;
    }
    Iterator<Map.Entry<BlockKey, Entry>> it = lru.entrySet().iterator();
    while (currentBytes + size > maxBytes && it.hasNext()) {
      Map.Entry<BlockKey, Entry> eldest = it.next();
      it.remove();
      currentBytes -= eldest.getValue().storedSize();
      evictions.incrementAndGet();
      deleteStored(eldest.getKey(), eldest.getValue());
    }
    // can still be full if the space is reserved by blocks being stored
    return currentBytes + size <= maxBytes;
  }

  /** Remove all cached blocks of a file, used when the file is deleted. */
  public void invalidate(String fileId) {
    synchronized (this) {
      Iterator<Map.Entry<BlockKey, Entry>> it = lru.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<BlockKey, Entry> entry = it.next();
        if (entry.getKey().fileId().equals(fileId)) {
          it.remove();
          currentBytes -= entry.getValue().storedSize();
          deleteStored(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // Stats
  // -------------------------------------------------------------------------------------------

  /** Bytes of capacity used by cached blocks and blocks being stored. */
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

  /** Keeps blocks on the heap, one array per block. */
  public static class Memory extends RemoteBlockCache {
    public Memory(long maxBytes, int blockSize) {
      super(maxBytes, blockSize);
    }

    @Override
    protected boolean copyStored(
        BlockKey key, Entry entry, int offsetInBlock, byte[] dest, int destOffset, int length) {
      System.arraycopy((byte[]) entry.handle(), offsetInBlock, dest, destOffset, length);
      return true;
    }

    @Override
    protected Object writeStored(BlockKey key, byte[] data, Object reservation) {
      return data;
    }

    @Override
    protected void deleteStored(BlockKey key, Entry entry) {}

    @Override
    public String getType() {
      return "memory";
    }
  }

  /**
   * Keeps blocks in native memory outside the heap, so a large cache doesn't add to garbage
   * collection work or heap size. Memory is allocated in slabs of fixed size slots, one block per
   * slot. Slabs are allocated as the cache fills and are kept for the life of the cache, so memory
   * use grows up to the cache size and then stays constant.
   *
   * <p>Slabs are direct {@link ByteBuffer}s, so the JVM must allow at least the cache size of
   * direct memory ({@code -XX:MaxDirectMemorySize}, which defaults to the max heap size). If a slab
   * can't be allocated, blocks are read without being cached.
   *
   * <p>A slot can be evicted and reused for another block while a reader is copying from it. Each
   * slot has a read/write lock and records which block it holds, so a reader that finds its slot
   * holding a different block treats it as a miss.
   */
  public static class OffHeap extends RemoteBlockCache {
    public static final int DEFAULT_SLAB_BYTES = 64 * 1024 * 1024;

    private static final class Slot {
      final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      // guarded by lock
      BlockKey owner;
    }

    private final int slotsPerSlab;
    private final int totalSlots;
    // allocated as needed, guarded by the cache lock. Elements are read without the lock, after
    // getting the slot index under the lock, which makes them visible.
    private final ByteBuffer[] slabs;
    private final Slot[] slots;
    // guarded by the cache lock
    private final ArrayDeque<Integer> freeSlots = new ArrayDeque<>();
    private int allocatedSlots;
    private long allocatedBytes;
    private boolean allocationFailed;

    public OffHeap(long maxBytes, int blockSize) {
      this(maxBytes, blockSize, DEFAULT_SLAB_BYTES);
    }

    public OffHeap(long maxBytes, int blockSize, int slabBytes) {
      super(slotCount(maxBytes, blockSize) * (long) blockSize, blockSize);
      this.totalSlots = slotCount(maxBytes, blockSize);
      this.slotsPerSlab = Math.max(1, slabBytes / blockSize);
      this.slabs = new ByteBuffer[(totalSlots + slotsPerSlab - 1) / slotsPerSlab];
      this.slots = new Slot[totalSlots];
    }

    private static int slotCount(long maxBytes, int blockSize) {
      long count = maxBytes / blockSize;
      if (count < 1 || count > Integer.MAX_VALUE) {
        throw new IllegalArgumentException(
            "Off heap cache must hold between 1 and " + Integer.MAX_VALUE + " blocks");
      }
      return (int) count;
    }

    @Override
    protected int storedSize(int length) {
      // a short last block of a file still uses a whole slot
      return getBlockSize();
    }

    @Override
    protected Object reserve() {
      Integer free = freeSlots.poll();
      if (free != null) {
        return free;
      }
      if (allocatedSlots >= totalSlots) {
        // the cache accounting keeps this from happening
        return null;
      }
      int slot = allocatedSlots;
      int slab = slot / slotsPerSlab;
      if (slabs[slab] == null) {
        int slabSlots = Math.min(slotsPerSlab, totalSlots - slab * slotsPerSlab);
        try {
          slabs[slab] = ByteBuffer.allocateDirect(slabSlots * getBlockSize());
        } catch (OutOfMemoryError e) {
          if (!allocationFailed) {
            allocationFailed = true;
            logger.warn(
                "Could not allocate off heap block cache memory, caching only {} bytes. Increase"
                    + " -XX:MaxDirectMemorySize or reduce lazyRemoteData.cacheSizeMB",
                allocatedBytes,
                e);
          }
          return null;
        }
        allocatedBytes += (long) slabSlots * getBlockSize();
      }
      slots[slot] = new Slot();
      allocatedSlots++;
      return slot;
    }

    @Override
    protected void release(Object reservation) {
      freeSlots.push((Integer) reservation);
    }

    @Override
    protected Object writeStored(BlockKey key, byte[] data, Object reservation) {
      int slot = (Integer) reservation;
      Slot s = slots[slot];
      s.lock.writeLock().lock();
      try {
        s.owner = key;
        // absolute bulk operations don't change the buffer state, so they are safe to use
        // concurrently on different regions
        slabs[slot / slotsPerSlab].put(slotOffset(slot), data, 0, data.length);
      } finally {
        s.lock.writeLock().unlock();
      }
      return slot;
    }

    @Override
    protected boolean copyStored(
        BlockKey key, Entry entry, int offsetInBlock, byte[] dest, int destOffset, int length) {
      int slot = (Integer) entry.handle();
      Slot s = slots[slot];
      s.lock.readLock().lock();
      try {
        if (!key.equals(s.owner)) {
          // evicted and reused for another block
          return false;
        }
        slabs[slot / slotsPerSlab].get(slotOffset(slot) + offsetInBlock, dest, destOffset, length);
        return true;
      } finally {
        s.lock.readLock().unlock();
      }
    }

    @Override
    protected void deleteStored(BlockKey key, Entry entry) {
      // the slot keeps its data until it is reused, which readers detect from the owner
      freeSlots.push((Integer) entry.handle());
    }

    private int slotOffset(int slot) {
      return (slot % slotsPerSlab) * getBlockSize();
    }

    @Override
    public synchronized long getAllocatedBytes() {
      return allocatedBytes;
    }

    @Override
    public String getType() {
      return "off_heap";
    }
  }

  /**
   * Keeps blocks in files in a local directory. Allows a cache larger than memory, at the cost of a
   * file read per hit (usually served by the OS page cache). The directory is emptied on creation,
   * since blocks from a previous run may be for files that changed.
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
    protected boolean copyStored(
        BlockKey key, Entry entry, int offsetInBlock, byte[] dest, int destOffset, int length)
        throws IOException {
      try (FileChannel channel = FileChannel.open(blockPath(key), StandardOpenOption.READ)) {
        ByteBuffer buffer = ByteBuffer.wrap(dest, destOffset, length);
        long position = offsetInBlock;
        while (buffer.hasRemaining()) {
          int read = channel.read(buffer, position);
          if (read < 0) {
            return false;
          }
          position += read;
        }
        return true;
      } catch (NoSuchFileException e) {
        // Evicted after the LRU check. A block being stored again can also race with the
        // eviction of its previous copy; either way the result is only a cache miss.
        return false;
      }
    }

    @Override
    protected Object writeStored(BlockKey key, byte[] data, Object reservation) throws IOException {
      Path path = blockPath(key);
      Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
      Files.write(tmp, data);
      Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      return null;
    }

    @Override
    protected void deleteStored(BlockKey key, Entry entry) {
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
