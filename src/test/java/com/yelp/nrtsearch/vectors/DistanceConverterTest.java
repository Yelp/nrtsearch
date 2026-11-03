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

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.vectors.IndexConfig.DistanceMetric;
import org.junit.Test;

public class DistanceConverterTest {

  @Test
  public void testCosine() {
    // identical: cos = 1, score = 1, distance = 0
    assertEquals(0f, DistanceConverter.scoreToDistance(1.0, DistanceMetric.COSINE), 1e-6);
    // orthogonal: cos = 0, score = 0.5, distance = 1
    assertEquals(1f, DistanceConverter.scoreToDistance(0.5, DistanceMetric.COSINE), 1e-6);
    // opposite: cos = -1, score = 0, distance = 2
    assertEquals(2f, DistanceConverter.scoreToDistance(0.0, DistanceMetric.COSINE), 1e-6);
  }

  @Test
  public void testEuclidean() {
    float[] a = {0, 0};
    float[] b = {3, 4};
    float d = DistanceConverter.distance(a, b, DistanceMetric.EUCLIDEAN);
    assertEquals(5f, d, 1e-6);
    double score = 1.0 / (1.0 + d * d);
    assertEquals(5f, DistanceConverter.scoreToDistance(score, DistanceMetric.EUCLIDEAN), 1e-4);
    assertEquals(0f, DistanceConverter.scoreToDistance(1.0, DistanceMetric.EUCLIDEAN), 1e-6);
  }

  @Test
  public void testExactCosine() {
    assertEquals(
        1f,
        DistanceConverter.distance(new float[] {1, 0}, new float[] {0, 1}, DistanceMetric.COSINE),
        1e-6);
  }
}
