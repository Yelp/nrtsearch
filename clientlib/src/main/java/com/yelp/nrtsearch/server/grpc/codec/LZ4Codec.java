/*
 * Copyright 2021 Yelp Inc.
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
package com.yelp.nrtsearch.server.grpc.codec;

import io.grpc.Codec;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;

/** Codec to provide LZ4 compression support to gRPC. */
public class LZ4Codec implements Codec {
  public static final LZ4Codec INSTANCE = new LZ4Codec();

  @Override
  public String getMessageEncoding() {
    return "lz4";
  }

  @Override
  public InputStream decompress(InputStream inputStream) throws IOException {
    return new LZ4FrameInputStream(inputStream);
  }

  @Override
  public OutputStream compress(OutputStream outputStream) throws IOException {
    return new LZ4FrameOutputStream(outputStream);
  }
}
