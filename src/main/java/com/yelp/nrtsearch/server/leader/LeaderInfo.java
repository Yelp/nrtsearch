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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Information about the node holding cluster leadership (the active primary).
 *
 * @param leaderId unique id of the leader process (the ephemeral id of its GlobalState)
 * @param nodeName configured node name of the leader
 * @param host host name of the leader
 * @param port client gRPC port of the leader
 * @param replicationPort replication gRPC port of the leader
 * @param epoch leadership epoch, strictly increasing across leaders, used as the primaryGen
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record LeaderInfo(
    @JsonProperty("leaderId") String leaderId,
    @JsonProperty("nodeName") String nodeName,
    @JsonProperty("host") String host,
    @JsonProperty("port") int port,
    @JsonProperty("replicationPort") int replicationPort,
    @JsonProperty("epoch") long epoch) {

  /** Create a copy of this info with a different epoch. */
  public LeaderInfo withEpoch(long newEpoch) {
    return new LeaderInfo(leaderId, nodeName, host, port, replicationPort, newEpoch);
  }

  /** If this info describes the same node as other, ignoring epoch. */
  public boolean sameNode(LeaderInfo other) {
    return other != null && leaderId.equals(other.leaderId);
  }
}
