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

import java.io.Closeable;
import java.util.Optional;

/**
 * Elects a single active primary among the primary capable nodes of a cluster (all nodes sharing a
 * serviceName). Implementations may be provided by plugins through {@link
 * com.yelp.nrtsearch.server.plugins.PrimaryElectorPlugin}.
 *
 * <p>Contract:
 *
 * <ul>
 *   <li>Epochs must strictly increase each time leadership changes hands, across all nodes.
 *   <li>{@link #isLeader()} must be conservative, returning false as soon as leadership may have
 *       been lost.
 * </ul>
 *
 * <p>The elector provides liveness only. Correctness does not depend on it: the server fences all
 * remote state writes using the epoch, so two nodes that both believe they are leader cannot both
 * make durable progress. A faulty elector can stall failover, but cannot corrupt index data.
 */
public interface PrimaryElector extends Closeable {

  /**
   * Start participating in leader election.
   *
   * @param listener listener for local leadership changes
   */
  void start(LeaderListener listener);

  /** Get the last known leader of the cluster, which may be this node. */
  Optional<LeaderInfo> currentLeader();

  /** If this node currently holds valid leadership. */
  boolean isLeader();

  /**
   * Give up leadership if held. The node may be elected again later. If leadership was held, {@link
   * LeaderListener#onLost(LeaderInfo)} is invoked.
   */
  void resign();
}
