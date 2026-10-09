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
package com.yelp.nrtsearch.server.remote;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.config.YamlConfigReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * {@link FileCompressor} implementation using the Zstandard frame format. The compression level and
 * number of worker threads only affect compression; any level can be decompressed regardless of the
 * current settings.
 */
public class ZstdFileCompressor implements FileCompressor {
  static final String CONFIG_PREFIX = "remoteConfig.s3.zstd.";
  public static final int DEFAULT_LEVEL = 3;
  public static final int DEFAULT_WORKERS = 0;

  private final int level;
  private final int workers;

  /**
   * Constructor.
   *
   * @param level compression level, negative values enable the fast compression levels
   * @param workers number of worker threads used to compress each stream, 0 for single-threaded
   * @throws IllegalArgumentException if level or workers is out of range
   */
  public ZstdFileCompressor(int level, int workers) {
    if (level < Zstd.minCompressionLevel() || level > Zstd.maxCompressionLevel()) {
      throw new IllegalArgumentException(
          "zstd level must be in ["
              + Zstd.minCompressionLevel()
              + ", "
              + Zstd.maxCompressionLevel()
              + "], got: "
              + level);
    }
    if (workers < 0) {
      throw new IllegalArgumentException("zstd workers must be >= 0, got: " + workers);
    }
    this.level = level;
    this.workers = workers;
  }

  /**
   * Create a ZstdFileCompressor from the {@code remoteConfig.s3.zstd.*} settings.
   *
   * @param configuration server configuration
   * @return compressor
   */
  public static ZstdFileCompressor fromConfig(NrtsearchConfig configuration) {
    YamlConfigReader configReader = configuration.getConfigReader();
    int level = configReader.getInteger(CONFIG_PREFIX + "level", DEFAULT_LEVEL);
    int workers = configReader.getInteger(CONFIG_PREFIX + "workers", DEFAULT_WORKERS);
    return new ZstdFileCompressor(level, workers);
  }

  @Override
  public OutputStream compressStream(OutputStream output) throws IOException {
    ZstdOutputStreamNoFinalizer zstdOutput =
        new ZstdOutputStreamNoFinalizer(output, RecyclingBufferPool.INSTANCE);
    zstdOutput.setLevel(level);
    if (workers > 0) {
      zstdOutput.setWorkers(workers);
    }
    return zstdOutput;
  }

  @Override
  public InputStream decompressStream(InputStream compressed) throws IOException {
    return new ZstdInputStreamNoFinalizer(compressed, RecyclingBufferPool.INSTANCE);
  }

  /** Get compression level. */
  public int getLevel() {
    return level;
  }

  /** Get number of compression worker threads. */
  public int getWorkers() {
    return workers;
  }
}
