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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.plugins.FileCompressorPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class FileCompressorCreatorTest {

  @Before
  public void setup() {
    FileCompressorCreator.initialize(getEmptyConfig(), List.of());
  }

  private static NrtsearchConfig getEmptyConfig() {
    return getConfig("nodeName: \"server_foo\"");
  }

  private static NrtsearchConfig getConfig(String config) {
    return new NrtsearchConfig(new ByteArrayInputStream(config.getBytes()));
  }

  @Test
  public void testLZ4CompressorRegisteredByDefault() {
    FileCompressor compressor = FileCompressorCreator.getInstance().getCompressor("LZ4");
    assertNotNull(compressor);
  }

  @Test
  public void testZstdCompressorRegisteredByDefault() {
    FileCompressor compressor = FileCompressorCreator.getInstance().getCompressor("ZSTD");
    assertTrue(compressor instanceof ZstdFileCompressor);
    ZstdFileCompressor zstdCompressor = (ZstdFileCompressor) compressor;
    assertEquals(ZstdFileCompressor.DEFAULT_LEVEL, zstdCompressor.getLevel());
    assertEquals(ZstdFileCompressor.DEFAULT_WORKERS, zstdCompressor.getWorkers());
  }

  @Test
  public void testZstdCompressorUsesConfig() {
    FileCompressorCreator.initialize(
        getConfig(
            "nodeName: \"server_foo\"\nremoteConfig:\n  s3:\n    zstd:\n      level: 9\n      workers: 2"),
        List.of());
    ZstdFileCompressor compressor =
        (ZstdFileCompressor) FileCompressorCreator.getInstance().getCompressor("ZSTD");
    assertEquals(9, compressor.getLevel());
    assertEquals(2, compressor.getWorkers());
  }

  @Test
  public void testZstdInvalidConfigThrows() {
    try {
      FileCompressorCreator.initialize(
          getConfig("nodeName: \"server_foo\"\nremoteConfig:\n  s3:\n    zstd:\n      level: 100"),
          List.of());
      fail("Expected IllegalArgumentException for invalid zstd level");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("zstd level"));
    }
  }

  @Test
  public void testGetCompressorReturnsNullForNone() {
    assertNull(FileCompressorCreator.getInstance().getCompressor("NONE"));
  }

  @Test
  public void testGetCompressorReturnsNullForNull() {
    assertNull(FileCompressorCreator.getInstance().getCompressor(null));
  }

  @Test
  public void testGetCompressorReturnsNullForEmpty() {
    assertNull(FileCompressorCreator.getInstance().getCompressor(""));
  }

  @Test
  public void testPluginCanRegisterCustomCompressor() {
    FileCompressor customCompressor =
        new FileCompressor() {
          @Override
          public OutputStream compressStream(OutputStream output) {
            return output;
          }

          @Override
          public InputStream decompressStream(InputStream compressed) {
            return compressed;
          }
        };

    class CustomPlugin extends Plugin implements FileCompressorPlugin {
      @Override
      public Map<String, FileCompressor> getFileCompressors() {
        return Map.of("CUSTOM", customCompressor);
      }
    }

    FileCompressorCreator.initialize(getEmptyConfig(), List.of(new CustomPlugin()));
    assertEquals(customCompressor, FileCompressorCreator.getInstance().getCompressor("CUSTOM"));
    // LZ4 still registered
    assertNotNull(FileCompressorCreator.getInstance().getCompressor("LZ4"));
  }

  @Test
  public void testDuplicateNameThrows() {
    class DuplicatePlugin extends Plugin implements FileCompressorPlugin {
      @Override
      public Map<String, FileCompressor> getFileCompressors() {
        return Map.of("LZ4", new LZ4FileCompressor());
      }
    }
    try {
      FileCompressorCreator.initialize(getEmptyConfig(), List.of(new DuplicatePlugin()));
      fail("Expected IllegalArgumentException for duplicate compressor name");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testGetUnknownCompressorThrows() {
    try {
      FileCompressorCreator.getInstance().getCompressor("UNKNOWN");
      fail("Expected IllegalArgumentException for unknown compressor name");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
