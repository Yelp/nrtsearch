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

import com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery;
import org.apache.lucene.search.Query;

public abstract class DecayFilterFunction extends FilterFunction {

  /**
   * Constructor.
   *
   * @param filterQuery filter to use when applying this function, or null if none
   * @param weight weight multiple to scale the function score
   * @param decayFunction to score a document with a function that decays depending on the distance
   *     between an origin point and a numeric doc field value
   */
  public DecayFilterFunction(
      Query filterQuery, float weight, MultiFunctionScoreQuery.DecayFunction decayFunction) {
    super(filterQuery, weight);
    if (decayFunction.getDecay() <= 0 || decayFunction.getDecay() >= 1) {
      throw new IllegalArgumentException(
          "decay rate should be between (0, 1) but is " + decayFunction.getDecay());
    }
  }

  protected DecayFunction getDecayType(MultiFunctionScoreQuery.DecayType decayType) {
    return switch (decayType) {
      case DECAY_TYPE_GUASSIAN -> new GuassianDecayFunction();
      case DECAY_TYPE_EXPONENTIAL -> new ExponentialDecayFunction();
      case DECAY_TYPE_LINEAR -> new LinearDecayFunction();
      default -> throw new IllegalArgumentException(
          decayType
              + " not supported. Only exponential, guassian and linear decay functions are supported");
    };
  }
}
