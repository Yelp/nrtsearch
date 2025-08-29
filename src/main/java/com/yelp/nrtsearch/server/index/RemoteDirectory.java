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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;
import org.apache.lucene.store.*;

public class RemoteDirectory extends Directory {
  protected final RemoteFileContainer container;

  public RemoteDirectory(RemoteFileContainer container) {
    this.container = container;
  }

  @Override
  public String[] listAll() throws IOException {
    return new String[0];
  }

  @Override
  public void deleteFile(String name) throws IOException {}

  @Override
  public long fileLength(String name) throws IOException {
    return container.fileLength(name);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return null;
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    return null;
  }

  @Override
  public void sync(Collection<String> names) throws IOException {}

  @Override
  public void syncMetaData() throws IOException {}

  @Override
  public void rename(String source, String dest) throws IOException {}

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    if (!container.fileExists(name)) {
      throw new java.nio.file.NoSuchFileException(name);
    }

    return new BufferedIndexInput(name) {

      long currPos = 0;
      long fileLength = -1;

      @Override
      protected void readInternal(ByteBuffer byteBuffer) throws IOException {
        int numBytesToRead = byteBuffer.remaining();
        byte[] tempBuffer = new byte[numBytesToRead];

        try (InputStream inputStream = container.readFile(name)) {
          long toSkip = currPos;
          while (toSkip > 0) {
            long skipped = inputStream.skip(toSkip);
            if (skipped <= 0) {
              throw new IOException("Failed to skip to position " + currPos + " in file " + name);
            }
            toSkip -= skipped;
          }

          int numBytesRead = 0;
          while (numBytesRead < numBytesToRead) {
            int read = inputStream.read(tempBuffer, numBytesRead, numBytesToRead - numBytesRead);
            if (read == -1) {
              throw new IOException(
                  "Reached end of file "
                      + name
                      + " before reading "
                      + numBytesToRead
                      + " bytes at position "
                      + currPos);
            }
            numBytesRead += read;
          }
          byteBuffer.put(tempBuffer);
          currPos += numBytesToRead;
        }
      }

      @Override
      protected void seekInternal(long pos) throws IOException {
        this.currPos = pos;
      }

      @Override
      public void close() throws IOException {}

      @Override
      public long length() {
        if (fileLength >= 0) return fileLength;
        try {
          fileLength = container.fileLength(name);
        } catch (IOException e) {
          throw new RuntimeException("Failed to get file length for " + name, e);
        }
        return fileLength;
      }
    };
  }

  @Override
  public Lock obtainLock(String name) throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public Set<String> getPendingDeletions() throws IOException {
    return Set.of();
  }
}
