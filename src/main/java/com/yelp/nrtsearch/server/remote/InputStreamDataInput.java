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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import org.apache.lucene.store.DataInput;

/** A DataInput implementation that wraps an InputStream. */
public class InputStreamDataInput extends DataInput implements Closeable {
  private final InputStream wrapped;

  public InputStreamDataInput(InputStream wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public byte readByte() throws IOException {
    int value = wrapped.read();
    if (value == -1) {
      throw new IOException("End of stream reached");
    }
    return (byte) value;
  }

  @Override
  public void readBytes(byte[] bytes, int offset, int length) throws IOException {
    int read = 0;
    while (read < length) {
      int result = wrapped.read(bytes, offset + read, length - read);
      if (result == -1) {
        throw new IOException("End of stream reached before reading all bytes");
      }
      read += result;
    }
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {
    long skipped = 0;
    while (skipped < numBytes) {
      long result = wrapped.skip(numBytes - skipped);
      if (result == 0) {
        if (wrapped.read() == -1) {
          throw new IOException("End of stream reached while skipping bytes");
        }
        skipped++;
      } else {
        skipped += result;
      }
    }
  }

  @Override
  public void close() throws IOException {
    wrapped.close();
  }
}
