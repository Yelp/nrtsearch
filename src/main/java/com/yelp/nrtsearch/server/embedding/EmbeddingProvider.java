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
 */
public interface EmbeddingProvider extends AutoCloseable {
  /**
   * Convert text to a float vector embedding. Implementations must be thread-safe.
   *
   * @param text input text to embed
   * @return float array of embedding values
   */
  float[] embed(String text);

  /**
   * Convert text to a byte vector embedding. Implementations must be thread-safe. The default
   * implementation throws {@link UnsupportedOperationException}; providers that support byte vector
   * fields must override this method.
   *
   * @param text input text to embed
   * @return byte array of embedding values
   * @throws UnsupportedOperationException if this provider does not support byte embeddings
   */
  default byte[] embedBytes(String text) {
    throw new UnsupportedOperationException(
        "This embedding provider does not support byte vector embeddings");
  }

  /** Returns the output vector dimensions of this provider. */
  int dimensions();

  /**
   * Release any resources held by this provider. Default implementation does nothing for providers
   * that do not hold native resources.
   */
  @Override
  default void close() {}
}
