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
package com.yelp.nrtsearch.tools.vector_bench;

import com.google.protobuf.Empty;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Lazy remote block cache metrics from an nrtsearch node's prometheus endpoint, used to report what
 * a query run read from S3.
 */
public class CacheMetrics {
  private static final String PREFIX = "nrt_lazy_remote_cache_";

  /** A scrape of the cache metrics, empty if the node has no lazy files. */
  public record Snapshot(Map<String, Double> values) {
    public double get(String name) {
      return values.getOrDefault(name, 0.0);
    }

    public boolean isEmpty() {
      return values.isEmpty();
    }

    /** Change from an earlier snapshot, summarized for a report. */
    public String formatDelta(Snapshot before, double seconds) {
      if (isEmpty()) {
        return "no lazy remote cache on this node";
      }
      double hits = get("hits_total") - before.get("hits_total");
      double misses = get("misses_total") - before.get("misses_total");
      double bytes = get("remote_bytes_total") - before.get("remote_bytes_total");
      double loadSeconds =
          get("remote_load_seconds_total") - before.get("remote_load_seconds_total");
      double errors = get("load_errors_total") - before.get("load_errors_total");
      double lookups = hits + misses;
      return String.format(
          Locale.ROOT,
          "hit rate %.1f%% (%,.0f hits, %,.0f misses), S3 %.1f MB (%.1f MB/s), %.0f GETs/s,"
              + " avg block load %.1fms, %,.0f load errors, cache %.1f / %.1f MB",
          lookups == 0 ? 0 : 100 * hits / lookups,
          hits,
          misses,
          bytes / 1e6,
          bytes / 1e6 / seconds,
          misses / seconds,
          misses == 0 ? 0 : 1000 * loadSeconds / misses,
          errors,
          get("size_bytes") / 1e6,
          get("max_bytes") / 1e6);
    }
  }

  /** Scrape the cache metrics from a node, or an empty snapshot if it has none. */
  public static Snapshot scrape(String host, int port) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    try {
      String text =
          LuceneServerGrpc.newBlockingStub(channel)
              .withDeadlineAfter(10, TimeUnit.SECONDS)
              .metrics(Empty.getDefaultInstance())
              .getData()
              .toStringUtf8();
      return parse(text);
    } finally {
      channel.shutdownNow();
    }
  }

  /** Parse prometheus text format, keeping lazy cache samples without their prefix and labels. */
  static Snapshot parse(String text) {
    Map<String, Double> values = new HashMap<>();
    for (String line : text.split("\n")) {
      if (!line.startsWith(PREFIX)) {
        continue;
      }
      int nameEnd = line.indexOf('{');
      int valueStart = line.lastIndexOf(' ');
      if (valueStart < 0) {
        continue;
      }
      String name = line.substring(PREFIX.length(), nameEnd >= 0 ? nameEnd : valueStart);
      try {
        values.merge(name, Double.parseDouble(line.substring(valueStart + 1)), Double::sum);
      } catch (NumberFormatException e) {
        // skip malformed sample
      }
    }
    return new Snapshot(values);
  }
}
