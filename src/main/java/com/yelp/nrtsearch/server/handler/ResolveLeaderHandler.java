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
package com.yelp.nrtsearch.server.handler;

import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ResolveLeaderRequest;
import com.yelp.nrtsearch.server.grpc.ResolveLeaderResponse;
import com.yelp.nrtsearch.server.leader.LeaderInfo;
import com.yelp.nrtsearch.server.leader.LeaderManager;
import com.yelp.nrtsearch.server.state.BackendGlobalState;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import java.util.Optional;

/**
 * Handler for resolveLeader, on both the client and replication services. Reports the active
 * primary as known by this node.
 *
 * <ul>
 *   <li>With leader election: the last known leader from the elector, and whether this node is the
 *       active primary. UNAVAILABLE if no leader is known yet.
 *   <li>Without leader election: a PRIMARY node reports itself (epoch 0); other nodes return
 *       UNAVAILABLE.
 * </ul>
 */
public class ResolveLeaderHandler extends Handler<ResolveLeaderRequest, ResolveLeaderResponse> {

  public ResolveLeaderHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public ResolveLeaderResponse handle(ResolveLeaderRequest request) throws Exception {
    GlobalState globalState = getGlobalState();
    Optional<LeaderManager> leaderManager = globalState.getLeaderManager();
    if (leaderManager.isPresent()) {
      LeaderManager manager = leaderManager.get();
      LeaderInfo leader =
          manager
              .currentLeader()
              .orElseThrow(
                  () ->
                      Status.UNAVAILABLE
                          .withDescription("No leader is known to this node yet")
                          .asRuntimeException());
      return toResponse(leader, manager.isActive() && isSelf(globalState, leader));
    }
    if (globalState.getConfiguration().getIndexStartConfig().getMode() == Mode.PRIMARY) {
      // an unreachable address would make every resolver dial itself
      if (BackendGlobalState.isLoopbackHost(globalState.getHostName())) {
        throw Status.UNAVAILABLE
            .withDescription(
                "This primary's hostName is a loopback address, which other nodes can not reach: "
                    + globalState.getHostName())
            .asRuntimeException();
      }
      LeaderInfo self =
          new LeaderInfo(
              globalState.getEphemeralId(),
              globalState.getNodeName(),
              globalState.getHostName(),
              globalState.getBoundPort(),
              globalState.getReplicationPort(),
              0);
      return toResponse(self, true);
    }
    throw Status.UNAVAILABLE
        .withDescription("Leader election is not enabled and this node is not a primary")
        .asRuntimeException();
  }

  private static boolean isSelf(GlobalState globalState, LeaderInfo leader) {
    return globalState.getEphemeralId().equals(leader.leaderId());
  }

  static ResolveLeaderResponse toResponse(LeaderInfo leader, boolean isLeader) {
    return ResolveLeaderResponse.newBuilder()
        .setIsLeader(isLeader)
        .setLeaderId(leader.leaderId())
        .setNodeName(leader.nodeName())
        .setHost(leader.host())
        .setPort(leader.port())
        .setReplicationPort(leader.replicationPort())
        .setEpoch(leader.epoch())
        .build();
  }
}
