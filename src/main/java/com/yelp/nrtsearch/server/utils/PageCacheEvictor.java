/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.utils;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evicts file data from the OS page cache using {@code posix_fadvise(POSIX_FADV_DONTNEED)}.
 *
 * <p>Only supported on Linux. On other platforms {@link #isSupported()} returns false and all
 * eviction methods are no-ops.
 */
public class PageCacheEvictor {
  private static final Logger logger = LoggerFactory.getLogger(PageCacheEvictor.class);

  // Linux values
  private static final int O_RDONLY = 0;
  private static final int POSIX_FADV_DONTNEED = 4;

  private final MethodHandle mhOpen;
  private final MethodHandle mhFadvise;
  private final MethodHandle mhClose;
  private final boolean supported;

  public PageCacheEvictor() {
    MethodHandle open = null;
    MethodHandle fadvise = null;
    MethodHandle close = null;
    boolean ok = false;

    String os = System.getProperty("os.name", "").toLowerCase();
    if (os.contains("linux")) {
      try {
        Linker linker = Linker.nativeLinker();
        SymbolLookup lookup = linker.defaultLookup();

        open =
            linker.downcallHandle(
                lookup.find("open").orElseThrow(() -> new UnsatisfiedLinkError("open")),
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

        fadvise =
            linker.downcallHandle(
                lookup
                    .find("posix_fadvise")
                    .orElseThrow(() -> new UnsatisfiedLinkError("posix_fadvise")),
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_INT));

        close =
            linker.downcallHandle(
                lookup.find("close").orElseThrow(() -> new UnsatisfiedLinkError("close")),
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));

        ok = true;
      } catch (Exception e) {
        logger.warn("Failed to initialize native page cache eviction: {}", e.getMessage());
      }
    } else {
      logger.info(
          "Page cache eviction via posix_fadvise is not supported on this OS ({}); eviction will be a no-op",
          os);
    }

    this.mhOpen = open;
    this.mhFadvise = fadvise;
    this.mhClose = close;
    this.supported = ok;
  }

  /** Returns true if posix_fadvise is available on this platform. */
  public boolean isSupported() {
    return supported;
  }

  /**
   * Evicts the entire file from the OS page cache.
   *
   * @param file path to the file
   * @throws IOException on I/O error
   */
  public void evictFile(Path file) throws IOException {
    if (!supported) {
      return;
    }
    long size = Files.size(file);
    evictFileRange(file, 0, size);
  }

  /**
   * Evicts a byte range within a file from the OS page cache.
   *
   * @param file path to the file
   * @param offset byte offset within the file
   * @param length number of bytes
   * @throws IOException on I/O error
   */
  public void evictFileRange(Path file, long offset, long length) throws IOException {
    if (!supported || length <= 0) {
      return;
    }
    String pathStr = file.toAbsolutePath().toString();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment pathSegment = arena.allocateUtf8String(pathStr);
      int fd = (int) mhOpen.invokeExact(pathSegment, O_RDONLY);
      if (fd < 0) {
        throw new IOException("Failed to open file for fadvise: " + file);
      }
      try {
        int ret = (int) mhFadvise.invokeExact(fd, offset, length, POSIX_FADV_DONTNEED);
        if (ret != 0) {
          logger.warn(
              "posix_fadvise returned {} for file {} offset={} length={}",
              ret,
              file,
              offset,
              length);
        }
      } finally {
        mhClose.invokeExact(fd);
      }
    } catch (IOException e) {
      throw e;
    } catch (Throwable e) {
      throw new IOException("Native call failed for file " + file, e);
    }
  }
}
