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
package com.yelp.nrtsearch.server.utils;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.apache.lucene.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for managing the OS page cache residency of files, using native calls. These are only
 * supported on 64-bit Linux, see {@link #isSupported()}.
 */
public final class PageCacheUtils {
  private static final Logger logger = LoggerFactory.getLogger(PageCacheUtils.class);

  private static final int O_RDONLY = 0;
  // Same value on x86_64 and aarch64
  private static final int O_CLOEXEC = 0x80000;
  private static final int POSIX_FADV_DONTNEED = 4;
  private static final int SYNC_FILE_RANGE_WAIT_BEFORE = 1;
  private static final int SYNC_FILE_RANGE_WRITE = 2;
  private static final int SYNC_FILE_RANGE_WAIT_AFTER = 4;

  private static final NativeCalls NATIVE_CALLS = loadNativeCalls();

  /**
   * A range of bytes in a file.
   *
   * @param offset start offset
   * @param length number of bytes
   */
  public record FileRange(long offset, long length) {
    public FileRange {
      if (offset < 0 || length < 0) {
        throw new IllegalArgumentException(
            "Invalid file range, offset: " + offset + ", length: " + length);
      }
    }
  }

  /**
   * Time spent in each step of dropping file ranges from the page cache.
   *
   * @param syncNanos time spent writing back dirty pages
   * @param dropNanos time spent dropping pages
   */
  public record DropStats(long syncNanos, long dropNanos) {}

  private PageCacheUtils() {}

  /** Get if page cache operations are supported on this platform. */
  public static boolean isSupported() {
    return NATIVE_CALLS != null;
  }

  /**
   * Drop the given file ranges from the OS page cache. Dirty pages in each range are first written
   * back, since only clean pages can be dropped. Pages that are currently mapped into a process
   * address space are not dropped. Ranges are expanded to the containing pages for write back, but
   * only pages fully contained in a range are dropped. Empty ranges are ignored.
   *
   * @param path file path
   * @param ranges file ranges to drop
   * @return time spent in each step
   * @throws IOException on error, or if page cache operations are not supported
   */
  public static DropStats dropFromPageCache(Path path, List<FileRange> ranges) throws IOException {
    NativeCalls calls = getNativeCalls();
    long syncNanos = 0;
    long dropNanos = 0;
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment callState = arena.allocate(NativeCalls.CALL_STATE_LAYOUT);
      int fd =
          (int)
              calls.open.invokeExact(
                  callState, arena.allocateFrom(path.toString()), O_RDONLY | O_CLOEXEC, 0);
      if (fd < 0) {
        throw new IOException("open failed for " + path + ", errno: " + errno(callState));
      }
      try {
        for (FileRange range : ranges) {
          // A zero length means to the end of the file for both calls
          if (range.length() == 0) {
            continue;
          }
          long startNanos = System.nanoTime();
          int ret =
              (int)
                  calls.syncFileRange.invokeExact(
                      callState,
                      fd,
                      range.offset(),
                      range.length(),
                      SYNC_FILE_RANGE_WAIT_BEFORE
                          | SYNC_FILE_RANGE_WRITE
                          | SYNC_FILE_RANGE_WAIT_AFTER);
          if (ret != 0) {
            throw new IOException(
                "sync_file_range failed for " + path + ", errno: " + errno(callState));
          }
          long syncedNanos = System.nanoTime();
          syncNanos += syncedNanos - startNanos;
          // posix_fadvise returns the error number directly
          ret =
              (int)
                  calls.posixFadvise.invokeExact(
                      fd, range.offset(), range.length(), POSIX_FADV_DONTNEED);
          if (ret != 0) {
            throw new IOException("posix_fadvise failed for " + path + ", errno: " + ret);
          }
          dropNanos += System.nanoTime() - syncedNanos;
        }
      } finally {
        int unused = (int) calls.close.invokeExact(fd);
      }
      return new DropStats(syncNanos, dropNanos);
    } catch (IOException | RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  /**
   * Get the number of pages in a file range that are resident in the OS page cache.
   *
   * @param path file path
   * @param range file range, which must not extend past the end of the file
   * @return number of pages in the range that are resident
   * @throws IOException on error, or if page cache operations are not supported
   */
  public static long residentPages(Path path, FileRange range) throws IOException {
    NativeCalls calls = getNativeCalls();
    if (range.length() == 0) {
      return 0;
    }
    long alignedOffset = range.offset() - (range.offset() % calls.pageSize);
    long alignedLength = range.offset() + range.length() - alignedOffset;
    long numPages = (alignedLength + calls.pageSize - 1) / calls.pageSize;
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        Arena arena = Arena.ofConfined()) {
      // Mapping the file does not fault in any pages
      MemorySegment mapped =
          channel.map(FileChannel.MapMode.READ_ONLY, alignedOffset, alignedLength, arena);
      MemorySegment residency = arena.allocate(numPages);
      MemorySegment callState = arena.allocate(NativeCalls.CALL_STATE_LAYOUT);
      int ret = (int) calls.mincore.invokeExact(callState, mapped, alignedLength, residency);
      if (ret != 0) {
        throw new IOException("mincore failed for " + path + ", errno: " + errno(callState));
      }
      long resident = 0;
      for (long i = 0; i < numPages; ++i) {
        if ((residency.get(JAVA_BYTE, i) & 1) != 0) {
          resident++;
        }
      }
      return resident;
    } catch (IOException | RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  /** Get the OS page size, or throw if page cache operations are not supported. */
  public static int getPageSize() throws IOException {
    return getNativeCalls().pageSize;
  }

  private static NativeCalls getNativeCalls() throws IOException {
    if (NATIVE_CALLS == null) {
      throw new IOException("Page cache operations are not supported on this platform");
    }
    return NATIVE_CALLS;
  }

  private static int errno(MemorySegment callState) {
    return (int) NativeCalls.ERRNO_HANDLE.get(callState, 0L);
  }

  private static NativeCalls loadNativeCalls() {
    // Native function descriptors assume a 64-bit off_t
    if (!Constants.LINUX || !Constants.JRE_IS_64BIT) {
      return null;
    }
    try {
      return new NativeCalls();
    } catch (Throwable t) {
      logger.warn("Unable to load native page cache functions", t);
      return null;
    }
  }

  private static final class NativeCalls {
    static final StructLayout CALL_STATE_LAYOUT = Linker.Option.captureStateLayout();
    static final VarHandle ERRNO_HANDLE =
        CALL_STATE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("errno"));

    final MethodHandle open;
    final MethodHandle close;
    final MethodHandle syncFileRange;
    final MethodHandle posixFadvise;
    final MethodHandle mincore;
    final int pageSize;

    NativeCalls() throws Throwable {
      Linker linker = Linker.nativeLinker();
      SymbolLookup lookup = linker.defaultLookup();
      Linker.Option captureErrno = Linker.Option.captureCallState("errno");
      // int open(const char *pathname, int flags, ...)
      open =
          linker.downcallHandle(
              lookup.find("open").orElseThrow(),
              FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT),
              captureErrno,
              Linker.Option.firstVariadicArg(2));
      // int close(int fd)
      close =
          linker.downcallHandle(
              lookup.find("close").orElseThrow(), FunctionDescriptor.of(JAVA_INT, JAVA_INT));
      // int sync_file_range(int fd, off64_t offset, off64_t nbytes, unsigned int flags)
      syncFileRange =
          linker.downcallHandle(
              lookup.find("sync_file_range").orElseThrow(),
              FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_LONG, JAVA_LONG, JAVA_INT),
              captureErrno);
      // int posix_fadvise(int fd, off_t offset, off_t len, int advice)
      posixFadvise =
          linker.downcallHandle(
              lookup.find("posix_fadvise").orElseThrow(),
              FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_LONG, JAVA_LONG, JAVA_INT));
      // int mincore(void *addr, size_t length, unsigned char *vec)
      mincore =
          linker.downcallHandle(
              lookup.find("mincore").orElseThrow(),
              FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS),
              captureErrno);
      // int getpagesize(void)
      MethodHandle getPageSize =
          linker.downcallHandle(
              lookup.find("getpagesize").orElseThrow(), FunctionDescriptor.of(JAVA_INT));
      pageSize = (int) getPageSize.invokeExact();
    }
  }
}
