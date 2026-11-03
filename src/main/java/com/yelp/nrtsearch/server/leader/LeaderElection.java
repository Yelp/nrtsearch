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
package com.yelp.nrtsearch.server.leader;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Components for multi-primary leader election, created during server bootstrap when {@code
 * leaderElection} is enabled. The same {@link ConditionalObjectStore} is shared by the pointer
 * fence (given to the remote backend at construction) and the elector.
 *
 * @param conditionalStore store supporting compare-and-set writes in the service bucket
 * @param fence fence for remote state pointer writes, also given to the remote backend
 */
public record LeaderElection(ConditionalObjectStore conditionalStore, EpochPointerFence fence) {

  public LeaderElection {
    Objects.requireNonNull(conditionalStore);
    Objects.requireNonNull(fence);
  }

  /**
   * Create leader election components if enabled by config.
   *
   * @param config server config
   * @param conditionalStore conditional store for the service bucket
   * @return components, or empty if leader election is disabled
   */
  public static Optional<LeaderElection> fromConfig(
      NrtsearchConfig config, Supplier<ConditionalObjectStore> conditionalStore) {
    if (!config.getLeaderElectionConfig().isEnabled()) {
      return Optional.empty();
    }
    ConditionalObjectStore store = conditionalStore.get();
    return Optional.of(new LeaderElection(store, new EpochPointerFence(store)));
  }
}
