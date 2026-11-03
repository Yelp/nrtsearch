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
package com.yelp.nrtsearch.server.nrt;

import java.io.IOException;

/**
 * Exception thrown when a replication request comes from a primary with a generation lower than one
 * already seen, meaning the sender has been superseded by a newer primary.
 */
public class StalePrimaryGenException extends IOException {
  private final long requestPrimaryGen;
  private final long currentPrimaryGen;

  public StalePrimaryGenException(
      String indexName, long requestPrimaryGen, long currentPrimaryGen) {
    super(
        String.format(
            "Stale primary generation for index %s: request primaryGen %d < current primaryGen %d",
            indexName, requestPrimaryGen, currentPrimaryGen));
    this.requestPrimaryGen = requestPrimaryGen;
    this.currentPrimaryGen = currentPrimaryGen;
  }

  public long getRequestPrimaryGen() {
    return requestPrimaryGen;
  }

  public long getCurrentPrimaryGen() {
    return currentPrimaryGen;
  }
}
