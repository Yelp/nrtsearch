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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.junit.Test;

public class LZ4CodecTest {

  @Test
  public void testInstance() {
    assertNotNull(LZ4Codec.INSTANCE);
  }

  @Test
  public void testCompressWrapper() throws IOException {
    OutputStream outputStream = new ByteArrayOutputStream();
    OutputStream wrappedStream = LZ4Codec.INSTANCE.compress(outputStream);
    assertTrue(wrappedStream instanceof LZ4FrameOutputStream);
  }

  @Test
  public void testDecompressWrapper() throws IOException {
    InputStream inputStream = new ByteArrayInputStream(new byte[0]);
    InputStream wrappedStream = LZ4Codec.INSTANCE.decompress(inputStream);
    assertTrue(wrappedStream instanceof LZ4FrameInputStream);
  }
}
