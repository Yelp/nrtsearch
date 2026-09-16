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
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.plugins.FileCompressorPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class FileCompressorCreatorTest {

  @Before
  public void setup() {
    FileCompressorCreator.initialize(List.of());
  }

  @Test
  public void testLZ4CompressorRegisteredByDefault() {
    FileCompressor compressor = FileCompressorCreator.getInstance().getCompressor("LZ4");
    assertNotNull(compressor);
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
          public java.io.OutputStream compressStream(java.io.OutputStream output) {
            return output;
          }

          @Override
          public java.io.InputStream decompressStream(java.io.InputStream compressed) {
            return compressed;
          }
        };

    class CustomPlugin extends Plugin implements FileCompressorPlugin {
      @Override
      public Map<String, FileCompressor> getFileCompressors() {
        return Map.of("CUSTOM", customCompressor);
      }
    }

    FileCompressorCreator.initialize(List.of(new CustomPlugin()));
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
      FileCompressorCreator.initialize(List.of(new DuplicatePlugin()));
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
