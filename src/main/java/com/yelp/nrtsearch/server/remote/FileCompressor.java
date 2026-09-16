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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Interface for streaming compression and decompression of index data files. */
public interface FileCompressor {

  /**
   * Wrap an output stream to produce compressed output. Data written to the returned stream is
   * compressed and written to the underlying {@code output}. The returned stream must be closed to
   * flush and finalize the compressed output.
   *
   * @param output underlying output stream to write compressed bytes to
   * @return wrapping output stream that compresses data on write
   * @throws IOException on error creating the compressing stream
   */
  OutputStream compressStream(OutputStream output) throws IOException;

  /**
   * Wrap an input stream to produce decompressed output. Data read from the returned stream is
   * decompressed from the underlying {@code compressed} stream.
   *
   * @param compressed underlying input stream containing compressed bytes
   * @return wrapping input stream that decompresses data on read
   * @throws IOException on error creating the decompressing stream
   */
  InputStream decompressStream(InputStream compressed) throws IOException;
}
