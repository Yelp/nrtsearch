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
import com.yelp.nrtsearch.server.grpc.discovery.LeaderRpcNameResolverProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leader following replication clients shared by all replica indices of a server that use the same
 * seeds, so the leader is resolved once (one set of resolveLeader calls per poll, not one per
 * index) and all indices follow a failover at the same time. Reference counted: the shared channel
 * is shut down when the last index using it closes its client.
 */
public final class SharedLeaderClients {
  private static final Logger logger = LoggerFactory.getLogger(SharedLeaderClients.class);

  private static final class Shared {
    final ManagedChannel channel;
    final String description;
    int refs = 0;

    Shared(ManagedChannel channel, String description) {
      this.channel = channel;
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
      shared =
          SHARED.computeIfAbsent(
              fullKey,
              k ->
                  new Shared(
                      ManagedChannelBuilder.forTarget(
                              LeaderRpcNameResolverProvider.SCHEME + ":///leader")
                          .usePlaintext()
                          .maxInboundMessageSize(ReplicationServerClient.MAX_MESSAGE_BYTES_SIZE)
                          .nameResolverFactory(
                              LeaderRpcNameResolverProvider.create(seeds, pollIntervalMs))
                          .build(),
                      key));
      shared.refs++;
    }
    Shared finalShared = shared;
    return new ReplicationServerClient(shared.channel, "", 0, key) {
      private boolean released = false;

      @Override
      public void close() {
        synchronized (this) {
          if (released) {
            return;
          }
          released = true;
        }
        release(fullKey, finalShared);
      }
    };
  }

  private static void release(String fullKey, Shared shared) {
    boolean shutdown;
    synchronized (SHARED) {
      shared.refs--;
      shutdown = shared.refs == 0;
      if (shutdown) {
        SHARED.remove(fullKey, shared);
      }
    }
    if (shutdown) {
      shared.channel.shutdown();
      try {
        if (!shared.channel.awaitTermination(10, TimeUnit.SECONDS)) {
          shared.channel.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        shared.channel.shutdownNow();
      }
      logger.info("Closed shared leader client for {}", shared.description);
    }
  }

  /** Number of shared channels, for tests. */
  static int size() {
    synchronized (SHARED) {
      return SHARED.size();
    }
  }
}
