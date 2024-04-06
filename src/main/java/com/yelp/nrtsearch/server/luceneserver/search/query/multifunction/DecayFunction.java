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

public interface DecayFunction {
  /**
   * Computes the decayed score based on the provided distance, offset, and scale.
   *
   * @param distance the distance from a given origin point.
   * @param offset the point after which the decay starts.
   * @param scale scale factor that influences the rate of decay. This scale value is computed from
   *     the user given scale using the computeScale() method.
   * @return the decayed score after applying the decay function
   */
  double computeScore(double distance, double offset, double scale);

  /**
   * Computes the adjusted scale based on a user given scale and decay rate.
   *
   * @param scale user given scale.
   * @param decay decay rate that decides how the score decreases.
   * @return adjusted scale which will be used by the computeScore() method.
   */
  double computeScale(double scale, double decay);

  /**
   * Provides an explanation for the computed score based on the given distance, offset, and scale.
   *
   * @param distance the distance from a given origin point.
   * @param offset the point after which the decay starts.
   * @param scale scale factor that influences the rate of decay. This scale value is computed from
   *     the user given scale using the computeScale() method.
   * @return Explanation object that details the calculations involved in computing the decayed
   *     score.
   */
  Explanation explainComputeScore(double distance, double offset, double scale);
}
