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
package com.yelp.nrtsearch.vectors;

/**
 * Converts nrtsearch (Lucene) vector scores into S3 Vectors distances, where a smaller distance
 * means more similar.
 *
 * <p>Lucene scores are:
 *
 * <ul>
 *   <li>cosine: {@code score = (1 + cos) / 2}
 *   <li>euclidean: {@code score = 1 / (1 + d^2)}
 * </ul>
 *
 * S3 Vectors returns cosine distance {@code 1 - cos} and euclidean distance {@code d}.
 */
public final class DistanceConverter {
  private DistanceConverter() {}

  public static float scoreToDistance(double score, IndexConfig.DistanceMetric metric) {
    return switch (metric) {
      case COSINE -> (float) Math.max(0.0, 2.0 - 2.0 * score);
      case EUCLIDEAN -> {
        if (score <= 0) {
          yield Float.POSITIVE_INFINITY;
        }
        yield (float) Math.sqrt(Math.max(0.0, 1.0 / score - 1.0));
      }
    };
  }

  /** Exact distance between two vectors, used for testing. */
  public static float distance(float[] a, float[] b, IndexConfig.DistanceMetric metric) {
    return switch (metric) {
      case COSINE -> {
        double dot = 0, na = 0, nb = 0;
        for (int i = 0; i < a.length; i++) {
          dot += a[i] * b[i];
          na += a[i] * a[i];
          nb += b[i] * b[i];
        }
        yield (float) (1.0 - dot / (Math.sqrt(na) * Math.sqrt(nb)));
      }
      case EUCLIDEAN -> {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
          double d = a[i] - b[i];
          sum += d * d;
        }
        yield (float) Math.sqrt(sum);
      }
    };
  }
}
