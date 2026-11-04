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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.leader.LeaderInfo;
import com.yelp.nrtsearch.server.leader.LeaderManager;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.junit.Test;

public class VerifyActivePrimaryTest {

  @Test
  public void testNoLeaderElection() {
    GlobalState globalState = mock(GlobalState.class);
    when(globalState.getLeaderManager()).thenReturn(Optional.empty());
    Handler.verifyActivePrimary(globalState, "op");
  }

  @Test
  public void testActive() {
    GlobalState globalState = mock(GlobalState.class);
    LeaderManager manager = mock(LeaderManager.class);
    when(manager.isActive()).thenReturn(true);
    when(globalState.getLeaderManager()).thenReturn(Optional.of(manager));
    Handler.verifyActivePrimary(globalState, "op");
  }

  /** A node that is demoting (index still primary, writer open) must reject writes. */
  @Test
  public void testDemotingRejected() {
    GlobalState globalState = mock(GlobalState.class);
    LeaderManager manager = mock(LeaderManager.class);
    when(manager.isActive()).thenReturn(false);
    when(manager.getRole()).thenReturn(LeaderManager.Role.DEMOTING);
    when(manager.currentLeader())
        .thenReturn(Optional.of(new LeaderInfo("b", "node_b", "host_b", 1, 2, 7)));
    when(globalState.getLeaderManager()).thenReturn(Optional.of(manager));
    StatusRuntimeException e =
        assertThrows(
            StatusRuntimeException.class,
            () -> Handler.verifyActivePrimary(globalState, "Adding documents"));
    assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    assertTrue(e.getMessage(), e.getMessage().contains("this node is DEMOTING"));
    assertTrue(e.getMessage(), e.getMessage().contains("node_b (host_b:1, epoch 7)"));
  }

  @Test
  public void testStatusMapping() {
    // top level status is reported as is
    StatusRuntimeException notFound = Status.NOT_FOUND.asRuntimeException();
    assertEquals(notFound, Handler.findStatusException(notFound));
    // wrapped unrelated status (e.g. from a call to another node) is not unwrapped
    assertEquals(null, Handler.findStatusException(new RuntimeException(notFound)));
    // fenced writes and leadership errors are reported as FAILED_PRECONDITION when wrapped
    assertEquals(
        Status.Code.FAILED_PRECONDITION,
        Handler.findStatusException(
                new RuntimeException(
                    new java.util.concurrent.ExecutionException(
                        new com.yelp.nrtsearch.server.leader.FencedException("fenced", 3))))
            .getStatus()
            .getCode());
    com.yelp.nrtsearch.server.leader.NotActivePrimaryException notActive =
        new com.yelp.nrtsearch.server.leader.NotActivePrimaryException("not leader");
    assertEquals(
        notActive,
        Handler.findStatusException(
            new RuntimeException(new java.util.concurrent.ExecutionException(notActive))));
  }
}
