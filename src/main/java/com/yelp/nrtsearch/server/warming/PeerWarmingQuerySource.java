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
package com.yelp.nrtsearch.server.warming;

import java.util.Optional;

/** Source of the warming corpus of a running peer replica, used by the PEER warming strategy. */
@FunctionalInterface
public interface PeerWarmingQuerySource {

  /**
   * Warming queries fetched from a peer.
   *
   * @param peer description of the peer, for logging
   * @param content queries as JSON lines, in the format stored in S3
   */
  record PeerWarmingQueries(String peer, byte[] content) {}

  /**
   * Fetch the warming corpus of a qualifying peer.
   *
   * @return the fetched queries, or empty if no peer could provide them
   */
  Optional<PeerWarmingQueries> fetch();
}
