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

import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.StatsRequest;
import com.yelp.nrtsearch.server.grpc.StatsResponse;
import com.yelp.nrtsearch.vectors.IndexConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine;

@CommandLine.Command(
    name = "info",
    description =
        "Show a node's view of the index: vector count, searcher version, segments, size on disk,"
            + " and lazy remote cache stats. Run against the primary and the replica to check the"
            + " replica has caught up.")
public class InfoCommand implements Callable<Integer> {
  @CommandLine.Mixin BenchOptions.Target target;

  @CommandLine.Option(
      names = {"--watch"},
      description = "Print again every this many seconds, until stopped",
      defaultValue = "0")
  int watchSeconds;

  @Override
  public Integer call() throws Exception {
    String nrtIndex = IndexConfig.nrtsearchIndexName(target.bucket, target.index);
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(target.host, target.port).usePlaintext().build();
    try {
      do {
        StatsResponse stats =
            LuceneServerGrpc.newBlockingStub(channel)
                .withDeadlineAfter(10, TimeUnit.SECONDS)
                .stats(StatsRequest.newBuilder().setIndexName(nrtIndex).build());
        // stats has no current searcher version; every search response does
        long version =
            LuceneServerGrpc.newBlockingStub(channel)
                .withDeadlineAfter(10, TimeUnit.SECONDS)
                .search(SearchRequest.newBuilder().setIndexName(nrtIndex).setTopHits(0).build())
                .getSearchState()
                .getSearcherVersion();
        int vectors = stats.getCurrentSearcher().getNumDocs();
        // maxDoc and numDocs come from the index writer, so only primaries report them
        String deletes =
            stats.getMaxDoc() > 0
                ? String.format(
                    Locale.ROOT,
                    ", %,d deleted not yet merged",
                    stats.getMaxDoc() - stats.getNumDocs())
                : "";
        System.out.printf(
            Locale.ROOT,
            "%s:%d %s: %,d vectors%s, searcher version %d, %d segments, %.1f MB on disk, state %s%n",
            target.host,
            target.port,
            nrtIndex,
            vectors,
            deletes,
            version,
            stats.getCurrentSearcher().getNumSegments(),
            stats.getDirSize() / 1e6,
            stats.getState());
        CacheMetrics.Snapshot cache = CacheMetrics.scrape(target.host, target.port);
        if (!cache.isEmpty()) {
          System.out.printf(
              Locale.ROOT,
              "  lazy cache (%s): %.1f / %.1f MB, %,.0f hits, %,.0f misses, %.1f MB read from S3%n",
              "since start",
              cache.get("size_bytes") / 1e6,
              cache.get("max_bytes") / 1e6,
              cache.get("hits_total"),
              cache.get("misses_total"),
              cache.get("remote_bytes_total") / 1e6);
        }
        if (watchSeconds > 0) {
          Thread.sleep(TimeUnit.SECONDS.toMillis(watchSeconds));
        }
      } while (watchSeconds > 0);
    } finally {
      channel.shutdownNow();
    }
    return 0;
  }
}
