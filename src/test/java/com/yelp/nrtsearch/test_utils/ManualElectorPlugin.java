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
package com.yelp.nrtsearch.test_utils;

import com.yelp.nrtsearch.server.leader.LeaderInfo;
import com.yelp.nrtsearch.server.leader.LeaderListener;
import com.yelp.nrtsearch.server.leader.PrimaryElector;
import com.yelp.nrtsearch.server.leader.PrimaryElectorProvider;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.PrimaryElectorPlugin;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test elector plugin where leadership is assigned by the test. All servers that share a {@link
 * Cluster} see the same leader. Epochs start at the current time so they are newer than any
 * existing primaryGen.
 */
public class ManualElectorPlugin extends Plugin implements PrimaryElectorPlugin {
  public static final String TYPE = "manual";

  /** Shared leadership state for a set of servers. */
  public static class Cluster {
    private final AtomicLong epoch = new AtomicLong(System.currentTimeMillis());
    private final Map<String, ManualElector> electors = new ConcurrentHashMap<>();
    private volatile LeaderInfo leader;

    /** Make the node with the given node name leader, demoting any current leader. */
    public synchronized long elect(String nodeName) {
      ManualElector next = electors.get(nodeName);
      if (next == null) {
        throw new IllegalArgumentException(
            "Unknown node: " + nodeName + " in " + electors.keySet());
      }
      revokeCurrent();
      LeaderInfo info = next.localInfo.withEpoch(epoch.incrementAndGet());
      leader = info;
      next.grant(info);
      return info.epoch();
    }

    /** If any node currently holds leadership. */
    public boolean hasLeader() {
      return leader != null;
    }

    /** Revoke leadership from the current leader, if any. */
    public synchronized void revokeCurrent() {
      LeaderInfo current = leader;
      if (current != null) {
        ManualElector holder = electors.get(current.nodeName());
        if (holder != null) {
          holder.revoke();
        }
      }
      leader = null;
    }

    /**
     * Assign leadership to a node without telling the current leader, simulating a paused leader
     * that still believes it holds leadership.
     */
    public synchronized long electWithoutRevoking(String nodeName) {
      ManualElector next = electors.get(nodeName);
      LeaderInfo info = next.localInfo.withEpoch(epoch.incrementAndGet());
      leader = info;
      next.grant(info);
      return info.epoch();
    }
  }

  private static class ManualElector implements PrimaryElector {
    private final Cluster cluster;
    private final LeaderInfo localInfo;
    private volatile LeaderListener listener;
    private volatile LeaderInfo held;

    ManualElector(Cluster cluster, LeaderInfo localInfo) {
      this.cluster = cluster;
      this.localInfo = localInfo;
    }

    synchronized void grant(LeaderInfo info) {
      held = info;
      listener.onElected(info);
    }

    synchronized void revoke() {
      LeaderInfo lost = held;
      if (lost != null) {
        held = null;
        listener.onLost(lost);
      }
    }

    @Override
    public void start(LeaderListener listener) {
      this.listener = listener;
      cluster.electors.put(localInfo.nodeName(), this);
    }

    @Override
    public Optional<LeaderInfo> currentLeader() {
      return Optional.ofNullable(cluster.leader);
    }

    @Override
    public boolean isLeader() {
      return held != null;
    }

    @Override
    public void resign(long epoch) {
      synchronized (cluster) {
        LeaderInfo current = held;
        if (current == null || current.epoch() != epoch) {
          return;
        }
        if (cluster.leader != null && cluster.leader.sameNode(current)) {
          cluster.leader = null;
        }
        revoke();
      }
    }

    @Override
    public void close() {
      synchronized (cluster) {
        LeaderInfo current = held;
        if (current != null) {
          resign(current.epoch());
        }
        cluster.electors.remove(localInfo.nodeName(), this);
      }
    }
  }

  private final Cluster cluster;

  public ManualElectorPlugin(Cluster cluster) {
    this.cluster = cluster;
  }

  @Override
  public Map<String, PrimaryElectorProvider> getPrimaryElectors() {
    return Map.of(TYPE, (context, params) -> new ManualElector(cluster, context.localInfo()));
  }
}
