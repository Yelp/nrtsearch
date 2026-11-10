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
package com.yelp.nrtsearch.server.grpc;

import com.yelp.nrtsearch.clientlib.Node;
import com.yelp.nrtsearch.clientlib.leader.LeaderNameResolver;
import com.yelp.nrtsearch.clientlib.leader.LeaderTracker;
import com.yelp.nrtsearch.server.grpc.discovery.LeaderRpcNameResolverProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leader following replication clients for replica indices. All indices of a server using the same
 * seeds share one {@link LeaderTracker}, so the leader is resolved once (one set of resolveLeader
 * calls per poll, not one per index) and all indices follow a failover together. Each index still
 * gets its own channel, so replication traffic of one index (e.g. a large merge copy) does not
 * delay another's. The tracker is reference counted and closed with the last client.
 */
public final class SharedLeaderClients {
  private static final Logger logger = LoggerFactory.getLogger(SharedLeaderClients.class);

  private static final class Shared {
    final LeaderTracker tracker;
    final String description;
    int refs = 0;

    Shared(LeaderTracker tracker, String description) {
      this.tracker = tracker;
      this.description = description;
    }
  }

  // guarded by itself
  private static final Map<String, Shared> SHARED = new HashMap<>();

  private SharedLeaderClients() {}

  /**
   * Get a client following the leader found from the given seeds. Close the client when done.
   *
   * @param key identifies the seed source (e.g. discovery file path, or host list and port)
   * @param seeds seed nodes, with replication ports
   * @param pollIntervalMs how often to re-resolve the leader
   */
  public static ReplicationServerClient acquire(
      String key, Supplier<Collection<Node>> seeds, long pollIntervalMs) {
    String fullKey = key + "#" + pollIntervalMs;
    Shared shared;
    synchronized (SHARED) {
      shared = SHARED.get(fullKey);
      if (shared == null) {
        LeaderTracker tracker = LeaderRpcNameResolverProvider.newTracker(seeds, pollIntervalMs);
        tracker.start();
        shared = new Shared(tracker, key);
        SHARED.put(fullKey, shared);
      }
      shared.refs++;
    }
    Shared finalShared = shared;
    ManagedChannel channel;
    try {
      channel =
          ManagedChannelBuilder.forTarget(LeaderRpcNameResolverProvider.SCHEME + ":///leader")
              .usePlaintext()
              .maxInboundMessageSize(ReplicationServerClient.MAX_MESSAGE_BYTES_SIZE)
              .nameResolverFactory(new SubscriberProvider(shared.tracker))
              .build();
    } catch (RuntimeException e) {
      release(fullKey, finalShared);
      throw e;
    }
    return new ReplicationServerClient(channel, "", 0, key) {
      private boolean released = false;

      @Override
      public void close() {
        synchronized (this) {
          if (released) {
            return;
          }
          released = true;
        }
        try {
          super.close();
        } finally {
          release(fullKey, finalShared);
        }
      }
    };
  }

  private static void release(String fullKey, Shared shared) {
    boolean last;
    synchronized (SHARED) {
      shared.refs--;
      last = shared.refs == 0;
      if (last) {
        SHARED.remove(fullKey, shared);
      }
    }
    if (last) {
      shared.tracker.close();
      logger.info("Closed shared leader tracker for {}", shared.description);
    }
  }

  /** Number of shared trackers, for tests. */
  static int size() {
    synchronized (SHARED) {
      return SHARED.size();
    }
  }

  /** Name resolver provider subscribing a channel to a shared tracker it does not own. */
  private static final class SubscriberProvider extends NameResolverProvider {
    private final LeaderTracker tracker;

    SubscriberProvider(LeaderTracker tracker) {
      this.tracker = tracker;
    }

    @Override
    protected boolean isAvailable() {
      return true;
    }

    @Override
    protected int priority() {
      return 6;
    }

    @Override
    public String getDefaultScheme() {
      return LeaderRpcNameResolverProvider.SCHEME;
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
      if (!LeaderRpcNameResolverProvider.SCHEME.equals(targetUri.getScheme())) {
        return null;
      }
      return new LeaderNameResolver(tracker, false, "");
    }
  }
}
