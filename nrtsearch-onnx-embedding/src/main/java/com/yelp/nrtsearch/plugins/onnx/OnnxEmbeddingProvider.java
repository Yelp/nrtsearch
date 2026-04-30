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

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import com.yelp.nrtsearch.server.embedding.EmbeddingProvider;
import java.io.IOException;
import java.nio.LongBuffer;
import java.nio.file.Paths;
import java.util.Map;

/** ONNX Runtime based embedding provider using DJL HuggingFace Tokenizer for preprocessing. */
public class OnnxEmbeddingProvider implements EmbeddingProvider {
  private final OrtEnvironment env;
  private final OrtSession session;
  private final HuggingFaceTokenizer tokenizer;
  private final int dimensions;
  private final int maxTokenLength;

  /**
   * Create a new ONNX embedding provider.
   *
   * @param modelPath path to the ONNX model file
   * @param tokenizerPath path to the HuggingFace tokenizer JSON file
   * @param dimensions output embedding dimensions
   * @param maxTokenLength maximum token length for the tokenizer
   */
  public OnnxEmbeddingProvider(
      String modelPath, String tokenizerPath, int dimensions, int maxTokenLength) {
    this.dimensions = dimensions;
    this.maxTokenLength = maxTokenLength;
    try {
      this.env = OrtEnvironment.getEnvironment();
      this.session = env.createSession(modelPath);
      this.tokenizer =
          HuggingFaceTokenizer.newInstance(
              Paths.get(tokenizerPath),
              Map.of(
                  "maxLength",
                  String.valueOf(maxTokenLength),
                  "truncation",
                  "true",
                  "padding",
                  "true"));
    } catch (OrtException | IOException e) {
      throw new RuntimeException("Failed to initialize ONNX embedding provider", e);
    }
  }

  @Override
  public float[] embed(String text) {
    try {
      Encoding encoding = tokenizer.encode(text);
      long[] inputIds = encoding.getIds();
      long[] attentionMask = encoding.getAttentionMask();
      long[] tokenTypeIds = encoding.getTypeIds();
      long[] shape = {1, inputIds.length};

      OnnxTensor inputIdsTensor = OnnxTensor.createTensor(env, LongBuffer.wrap(inputIds), shape);
      OnnxTensor attentionMaskTensor =
          OnnxTensor.createTensor(env, LongBuffer.wrap(attentionMask), shape);
      OnnxTensor tokenTypeIdsTensor =
          OnnxTensor.createTensor(env, LongBuffer.wrap(tokenTypeIds), shape);

      Map<String, OnnxTensor> inputs =
          Map.of(
              "input_ids", inputIdsTensor,
              "attention_mask", attentionMaskTensor,
              "token_type_ids", tokenTypeIdsTensor);

      try (OrtSession.Result result = session.run(inputs)) {
        float[][][] output = (float[][][]) result.get(0).getValue();
        return meanPool(output[0], attentionMask);
      } finally {
        inputIdsTensor.close();
        attentionMaskTensor.close();
        tokenTypeIdsTensor.close();
      }
    } catch (OrtException e) {
      throw new RuntimeException("Failed to run ONNX inference", e);
    }
  }

  private float[] meanPool(float[][] tokenEmbeddings, long[] attentionMask) {
    float[] result = new float[dimensions];
    float maskSum = 0;
    for (int i = 0; i < attentionMask.length; i++) {
      if (attentionMask[i] == 1) {
        maskSum += 1;
        for (int j = 0; j < dimensions; j++) {
          result[j] += tokenEmbeddings[i][j];
        }
      }
    }
    if (maskSum > 0) {
      for (int j = 0; j < dimensions; j++) {
        result[j] /= maskSum;
      }
    }
    // L2 normalize
    float norm = 0;
    for (int j = 0; j < dimensions; j++) {
      norm += result[j] * result[j];
    }
    norm = (float) Math.sqrt(norm);
    if (norm > 0) {
      for (int j = 0; j < dimensions; j++) {
        result[j] /= norm;
      }
    }
    return result;
  }

  @Override
  public int dimensions() {
    return dimensions;
  }

  @Override
  public void close() {
    try {
      session.close();
    } catch (OrtException e) {
      // best effort
    }
    tokenizer.close();
  }
}
