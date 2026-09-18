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
package com.yelp.nrtsearch.plugins.onnx;

import static org.junit.Assert.*;

import java.nio.file.Paths;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests for {@link OnnxEmbeddingProvider} using the all-MiniLM-L6-v2 ONNX model. */
public class OnnxEmbeddingProviderTest {
  private static OnnxEmbeddingProvider provider;

  @BeforeClass
  public static void setUp() {
    String modelDir = System.getProperty("user.dir") + "/../models/all-MiniLM-L6-v2";
    provider =
        new OnnxEmbeddingProvider(
            Paths.get(modelDir, "model.onnx").toString(),
            Paths.get(modelDir, "tokenizer.json").toString(),
            384,
            256);
  }

  @Test
  public void testDimensions() {
    assertEquals(384, provider.dimensions());
  }

  @Test
  public void testEmbedReturnsCorrectDimensions() {
    assertEquals(384, provider.embed("hello world").length);
  }

  @Test
  public void testEmbedDeterministic() {
    assertArrayEquals(provider.embed("test sentence"), provider.embed("test sentence"), 0.0001f);
  }

  @Test
  public void testDifferentTextProducesDifferentVectors() {
    float[] r1 = provider.embed("hello world");
    float[] r2 = provider.embed("completely different sentence about cars");
    boolean different = false;
    for (int i = 0; i < r1.length; i++) {
      if (Math.abs(r1[i] - r2[i]) > 0.001f) {
        different = true;
        break;
      }
    }
    assertTrue(different);
  }
}
