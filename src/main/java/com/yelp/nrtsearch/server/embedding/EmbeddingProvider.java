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
 * Converts text into a float vector embedding. Implementations must be thread-safe, as {@link
 * #embed(String)} may be called concurrently from multiple index and search threads.
 *
 * <p>Subclasses implement {@link #doEmbed(String)} (and optionally {@link #doEmbedBytes(String)})
 * to produce raw vectors. The public {@link #embed(String)} and {@link #embedBytes(String)} methods
 * are final and automatically validate that the returned vector length matches {@link
 * #dimensions()}.
 */
public abstract class EmbeddingProvider implements AutoCloseable {

  /** Returns the output vector dimensions of this provider. */
  public abstract int dimensions();

  /**
   * Produce a float vector embedding for the given text. Implementations must be thread-safe.
   *
   * @param text input text to embed
   * @return float array of embedding values; length must equal {@link #dimensions()}
   */
  protected abstract float[] doEmbed(String text);

  /**
   * Produce a byte vector embedding for the given text. Implementations must be thread-safe. The
   * default implementation throws {@link UnsupportedOperationException}; providers that support
   * byte vector fields must override this method.
   *
   * @param text input text to embed
   * @return byte array of embedding values; length must equal {@link #dimensions()}
   * @throws UnsupportedOperationException if this provider does not support byte embeddings
   */
  protected byte[] doEmbedBytes(String text) {
    throw new UnsupportedOperationException(
        "This embedding provider does not support byte vector embeddings");
  }

  /**
   * Convert text to a float vector embedding, validating that the result length matches {@link
   * #dimensions()}.
   *
   * @param text input text to embed
   * @return float array of embedding values
   * @throws IllegalArgumentException if the returned vector length does not match {@link
   *     #dimensions()}
   */
  public final float[] embed(String text) {
    float[] result = doEmbed(text);
    if (result.length != dimensions()) {
      throw new IllegalArgumentException(
          "Embedding provider returned vector of size "
              + result.length
              + " but provider dimensions() is "
              + dimensions());
    }
    return result;
  }

  /**
   * Convert text to a byte vector embedding, validating that the result length matches {@link
   * #dimensions()}.
   *
   * @param text input text to embed
   * @return byte array of embedding values
   * @throws IllegalArgumentException if the returned vector length does not match {@link
   *     #dimensions()}
   * @throws UnsupportedOperationException if this provider does not support byte embeddings
   */
  public final byte[] embedBytes(String text) {
    byte[] result = doEmbedBytes(text);
    if (result.length != dimensions()) {
      throw new IllegalArgumentException(
          "Embedding provider returned vector of size "
              + result.length
              + " but provider dimensions() is "
              + dimensions());
    }
    return result;
  }

  /**
   * Release any resources held by this provider. Default implementation does nothing for providers
   * that do not hold native resources.
   */
  @Override
  public void close() {}
}
