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
package com.yelp.nrtsearch.clientlib.leader;

import com.yelp.nrtsearch.clientlib.Node;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Channels to seed and leader nodes used to call resolveLeader, reused across polls. Channels to
 * nodes no longer in use are closed by {@link #retain}; all are closed by {@link #close}, after
 * which no new channels are created.
 */
public class ProbeChannels implements Closeable {
  // guarded by this
  private final Map<Node, ManagedChannel> channels = new HashMap<>();
  private boolean closed = false;

  /**
   * Get the channel for a node, creating it if needed.
   *
   * @throws IllegalStateException if closed
   */
  public synchronized ManagedChannel get(Node node) {
    if (closed) {
      throw new IllegalStateException("Probe channels are closed");
    }
    return channels.computeIfAbsent(
        node,
        n -> ManagedChannelBuilder.forAddress(n.getHost(), n.getPort()).usePlaintext().build());
  }

  /**
   * Close channels to nodes not in {@code inUse}, e.g. seeds removed from a discovery file. An
   * empty set is ignored, since it more likely means a seed source was briefly unreadable than that
   * every node is gone.
   */
  public void retain(Collection<Node> inUse) {
    if (inUse.isEmpty()) {
      return;
    }
    Set<Node> keep = Set.copyOf(inUse);
    java.util.List<ManagedChannel> toClose = new java.util.ArrayList<>();
    synchronized (this) {
      channels
          .entrySet()
          .removeIf(
              e -> {
                if (keep.contains(e.getKey())) {
                  return false;
                }
                toClose.add(e.getValue());
                return true;
              });
    }
    toClose.forEach(ManagedChannel::shutdownNow);
  }

  /** Number of open channels, for tests. */
  public synchronized int size() {
    return channels.size();
  }

  @Override
  public void close() {
    java.util.List<ManagedChannel> toClose;
    synchronized (this) {
      closed = true;
      toClose = new java.util.ArrayList<>(channels.values());
      channels.clear();
    }
    toClose.forEach(ManagedChannel::shutdownNow);
  }
}
