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
package com.yelp.nrtsearch.server.config;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Configuration for lazy loading of index files on replicas. Index files with one of the configured
 * extensions are not downloaded when a replica restores or copies an index version. Instead, a
 * small stub file is written locally, and reads of the file fetch byte ranges from the remote
 * backend through a block cache, kept in memory by default.
 *
 * <p>The intended use is full precision vectors ({@code .vec}) of quantized vector fields. Searches
 * use the quantized vectors ({@code .veq}) and the HNSW graph ({@code .vex}), which stay local, and
 * only read the full precision vectors to rescore the top hits.
 *
 * <p>Only applies to replicas. Files inside compound files ({@code .cfs}) are not lazy, so indexes
 * should disable compound files to benefit.
 */
public class LazyRemoteDataConfig {
  public static final String CONFIG_PREFIX = "lazyRemoteData.";
  public static final List<String> DEFAULT_EXTENSIONS = List.of("vec");
  public static final long DEFAULT_CACHE_SIZE_MB = 512;
  public static final int DEFAULT_BLOCK_SIZE_KB = 64;

  /** Where cached blocks are kept. */
  public enum CacheType {
    /** On the heap. Simplest, sized against the JVM heap. */
    MEMORY,
    /**
     * In native memory outside the heap, for large caches that shouldn't add to garbage collection
     * work. Needs direct memory ({@code -XX:MaxDirectMemorySize}) of at least the cache size.
     */
    OFF_HEAP,
    /** In files on local disk, for caches larger than memory allows. */
    DISK
  }

  private final boolean enabled;
  private final Set<String> extensions;
  private final CacheType cacheType;
  private final String cacheDir;
  private final long cacheSizeBytes;
  private final int blockSizeBytes;

  public static LazyRemoteDataConfig fromConfig(YamlConfigReader configReader) {
    Objects.requireNonNull(configReader);
    boolean enabled = configReader.getBoolean(CONFIG_PREFIX + "enabled", false);
    List<String> extensions =
        configReader.getStringList(CONFIG_PREFIX + "extensions", DEFAULT_EXTENSIONS);
    String cacheTypeName = configReader.getString(CONFIG_PREFIX + "cacheType", "MEMORY");
    CacheType cacheType;
    try {
      cacheType = CacheType.valueOf(cacheTypeName.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "lazyRemoteData.cacheType must be MEMORY, OFF_HEAP or DISK, got: " + cacheTypeName);
    }
    String cacheDir = configReader.getString(CONFIG_PREFIX + "cacheDir", "");
    long cacheSizeMB = configReader.getLong(CONFIG_PREFIX + "cacheSizeMB", DEFAULT_CACHE_SIZE_MB);
    int blockSizeKB = configReader.getInteger(CONFIG_PREFIX + "blockSizeKB", DEFAULT_BLOCK_SIZE_KB);
    return new LazyRemoteDataConfig(
        enabled, extensions, cacheType, cacheDir, cacheSizeMB, blockSizeKB);
  }

  public static LazyRemoteDataConfig disabled() {
    return new LazyRemoteDataConfig(
        false,
        DEFAULT_EXTENSIONS,
        CacheType.MEMORY,
        "",
        DEFAULT_CACHE_SIZE_MB,
        DEFAULT_BLOCK_SIZE_KB);
  }

  /**
   * @param enabled whether lazy loading is enabled
   * @param extensions file extensions to load lazily, without the leading dot
   * @param cacheType where to keep cached blocks
   * @param cacheDir directory for a disk block cache, or empty to use a directory under the node's
   *     index directory
   * @param cacheSizeMB maximum size of the block cache in MB
   * @param blockSizeKB size of each block read from the remote backend in KB
   */
  public LazyRemoteDataConfig(
      boolean enabled,
      List<String> extensions,
      CacheType cacheType,
      String cacheDir,
      long cacheSizeMB,
      int blockSizeKB) {
    this.enabled = enabled;
    this.extensions =
        extensions.stream()
            .map(e -> e.startsWith(".") ? e.substring(1) : e)
            .map(e -> e.toLowerCase(Locale.ROOT))
            .collect(Collectors.toUnmodifiableSet());
    this.cacheType = Objects.requireNonNull(cacheType);
    this.cacheDir = cacheDir == null ? "" : cacheDir;
    if (cacheSizeMB <= 0) {
      throw new IllegalArgumentException(
          "lazyRemoteData.cacheSizeMB must be positive, got: " + cacheSizeMB);
    }
    if (blockSizeKB < 4 || blockSizeKB > 64 * 1024) {
      throw new IllegalArgumentException(
          "lazyRemoteData.blockSizeKB must be between 4 and 65536, got: " + blockSizeKB);
    }
    this.cacheSizeBytes = cacheSizeMB * 1024 * 1024;
    this.blockSizeBytes = blockSizeKB * 1024;
    if (cacheSizeBytes < blockSizeBytes) {
      throw new IllegalArgumentException("lazyRemoteData.cacheSizeMB must hold at least one block");
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  public Set<String> getExtensions() {
    return extensions;
  }

  public CacheType getCacheType() {
    return cacheType;
  }

  public String getCacheDir() {
    return cacheDir;
  }

  public long getCacheSizeBytes() {
    return cacheSizeBytes;
  }

  public int getBlockSizeBytes() {
    return blockSizeBytes;
  }

  /** If a file with this name should be loaded lazily. */
  public boolean isLazyFile(String fileName) {
    if (!enabled) {
      return false;
    }
    int dot = fileName.lastIndexOf('.');
    if (dot < 0) {
      return false;
    }
    return extensions.contains(fileName.substring(dot + 1).toLowerCase(Locale.ROOT));
  }
}
