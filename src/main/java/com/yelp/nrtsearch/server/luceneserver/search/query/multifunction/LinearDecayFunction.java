/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.search.query.multifunction;

import org.apache.lucene.search.Explanation;

public class LinearDecayFunction implements DecayFunction {
  @Override
  public double computeScore(double distance, double offset, double scale) {
    return Math.max(0.0, (scale - Math.max(0.0, distance - offset)) / scale);
  }

  @Override
  public double computeScale(double scale, double decay) {
    return scale / (1.0 - decay);
  }

  @Override
  public Explanation explainComputeScore(double distance, double offset, double scale) {
    return Explanation.match(
        (float) computeScore(distance, offset, scale),
        "max(0.0, (" + scale + " - max(0.0, " + distance + " - " + offset + ")) / " + scale + ")");
  }
}
