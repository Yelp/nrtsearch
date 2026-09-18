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
package com.yelp.nrtsearch.server.embedding;

/**
 * Converts text into a vector embedding. Implementations must be thread-safe, as {@link
 * #embed(String)} may be called concurrently from multiple index and search threads.
 *
 * <p>Providers may support one or more output dimensions. Use {@link #supportsDimensions(int)} to
 * check compatibility with a field's vector dimensions before use.
 */
public abstract class EmbeddingProvider implements AutoCloseable {

  /**
   * Check whether this provider supports the given output dimensions.
   *
   * @param dimensions the desired output vector dimensions
   * @return true if this provider can produce vectors of the given size
   */
  public abstract boolean supportsDimensions(int dimensions);

  /**
   * Produce a float vector embedding for the given text. Implementations must be thread-safe.
   *
   * @param text input text to embed
   * @return float array of embedding values
   */
  public abstract float[] embed(String text);

  /**
   * Produce a byte vector embedding for the given text. Implementations must be thread-safe. The
   * default implementation throws {@link UnsupportedOperationException}; providers that support
   * byte vector fields must override this method.
   *
   * @param text input text to embed
   * @return byte array of embedding values
   * @throws UnsupportedOperationException if this provider does not support byte embeddings
   */
  public byte[] embedBytes(String text) {
    throw new UnsupportedOperationException(
        "This embedding provider does not support byte vector embeddings");
  }

  /**
   * Release any resources held by this provider. Default implementation does nothing for providers
   * that do not hold native resources.
   */
  @Override
  public void close() {}
}
