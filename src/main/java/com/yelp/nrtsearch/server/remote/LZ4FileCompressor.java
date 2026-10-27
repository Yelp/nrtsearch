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
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;

/** {@link FileCompressor} implementation using the LZ4 frame format. */
public class LZ4FileCompressor implements FileCompressor {

  @Override
  public OutputStream compressStream(OutputStream output) throws IOException {
    return new LZ4FrameOutputStream(output);
  }

  @Override
  public InputStream decompressStream(InputStream compressed) throws IOException {
    return new LZ4FrameInputStream(compressed);
  }
}
