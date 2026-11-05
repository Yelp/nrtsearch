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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yelp.nrtsearch.server.grpc.ForceMergeRequest;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import com.yelp.nrtsearch.server.grpc.RefreshRequest;
import com.yelp.nrtsearch.server.grpc.StatsRequest;
import com.yelp.nrtsearch.vectors.IndexConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import picocli.CommandLine;

@CommandLine.Command(
    name = "load",
    description =
        "Bulk load dataset vectors with PutVectors. Vector id i gets key v000000000000 + i, so a"
            + " load can be resumed or split with --start and --num.")
public class LoadCommand implements Callable<Integer> {
  @CommandLine.Mixin BenchOptions.Target target;
  @CommandLine.Mixin BenchOptions.Dataset dataset;

  @CommandLine.Option(
      names = {"--start"},
      description = "First vector id to load (default: ${DEFAULT-VALUE})",
      defaultValue = "0")
  long start;

  @CommandLine.Option(
      names = {"--num"},
      description =
          "Number of vectors to load from --start, or 0 for the rest of the dataset"
              + " (default: ${DEFAULT-VALUE})",
      defaultValue = "0")
  long num;

  @CommandLine.Option(
      names = {"--batch"},
      description = "Vectors per PutVectors call, at most 500 (default: ${DEFAULT-VALUE})",
      defaultValue = "500")
  int batch;

  @CommandLine.Option(
      names = {"--threads"},
      description = "Concurrent PutVectors calls (default: ${DEFAULT-VALUE})",
      defaultValue = "8")
  int threads;

  @CommandLine.Option(
      names = {"--refreshEach"},
      description =
          "Refresh after every batch, as the S3 Vectors API does. Off by default, which is much"
              + " faster for bulk loads")
  boolean refreshEach;

  @CommandLine.Option(
      names = {"--forceMerge"},
      description =
          "After loading, merge the index down to at most this many segments and refresh, so"
              + " every run measures the same layout. 0 to skip (default: ${DEFAULT-VALUE})",
      defaultValue = "0")
  int forceMerge;

  @CommandLine.Option(
      names = {"--noMetadata"},
      description = "Load vectors without metadata")
  boolean noMetadata;

  @Override
  public Integer call() throws Exception {
    if (batch < 1 || batch > 500) {
      throw new CommandLine.ParameterException(
          new CommandLine(this), "--batch must be between 1 and 500");
    }
    try (VectorSource source = dataset.open();
        BenchClient client = target.client(threads, refreshEach)) {
      long end = num > 0 ? Math.min(source.size(), start + num) : source.size();
      if (start >= end) {
        throw new CommandLine.ParameterException(
            new CommandLine(this),
            "Nothing to load: --start " + start + " is past the dataset size " + source.size());
      }
      long total = end - start;
      System.out.printf(
          "Loading %,d vectors of dimension %d into %s/%s with %d threads%n",
          total, source.dimension(), target.bucket, target.index, threads);

      AtomicLong nextId = new AtomicLong(start);
      AtomicLong loaded = new AtomicLong();
      LatencyRecorder latency = new LatencyRecorder();
      long t0 = System.nanoTime();
      final long finalEnd = end;
      ExecutorService executor = Executors.newFixedThreadPool(threads);
      Thread progress = progressThread(loaded, total, t0);
      progress.start();
      try {
        Future<?>[] futures = new Future<?>[threads];
        for (int t = 0; t < threads; t++) {
          futures[t] =
              executor.submit(
                  () -> {
                    long batchStart;
                    while ((batchStart = nextId.getAndAdd(batch)) < finalEnd) {
                      long batchEnd = Math.min(finalEnd, batchStart + batch);
                      ObjectNode put = target.request();
                      ArrayNode vectors = put.putArray("vectors");
                      for (long id = batchStart; id < batchEnd; id++) {
                        ObjectNode vector = vectors.addObject();
                        vector.put("key", BenchOptions.key(id));
                        ArrayNode data = vector.putObject("data").putArray("float32");
                        for (float f : source.vector(id)) {
                          data.add(f);
                        }
                        if (!noMetadata) {
                          vector.set("metadata", BenchOptions.metadata(id));
                        }
                      }
                      long s = System.nanoTime();
                      client.call("PutVectors", put);
                      latency.record(System.nanoTime() - s);
                      loaded.addAndGet(batchEnd - batchStart);
                    }
                    return null;
                  });
        }
        for (Future<?> f : futures) {
          f.get();
        }
      } finally {
        executor.shutdownNow();
        progress.interrupt();
        progress.join();
      }
      double seconds = (System.nanoTime() - t0) / 1e9;
      System.out.printf(
          Locale.ROOT,
          "Loaded %,d vectors in %.1fs (%.0f vectors/s)%nPutVectors latency: %s%n",
          loaded.get(),
          seconds,
          loaded.get() / seconds,
          latency.summary().format());
    }
    if (forceMerge > 0) {
      forceMerge();
    }
    return 0;
  }

  /**
   * Merge to at most --forceMerge segments, then refresh so the primary publishes the merged
   * version (replicas pick it up from S3 on their next poll).
   */
  private void forceMerge() {
    if (target.gatewayUrl != null) {
      throw new CommandLine.ParameterException(
          new CommandLine(this), "--forceMerge needs direct mode (--host/--port of the primary)");
    }
    String nrtIndex = IndexConfig.nrtsearchIndexName(target.bucket, target.index);
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(target.host, target.port).usePlaintext().build();
    try {
      LuceneServerGrpc.LuceneServerBlockingStub stub = LuceneServerGrpc.newBlockingStub(channel);
      long t0 = System.nanoTime();
      stub.withDeadlineAfter(6, TimeUnit.HOURS)
          .forceMerge(
              ForceMergeRequest.newBuilder()
                  .setIndexName(nrtIndex)
                  .setMaxNumSegments(forceMerge)
                  .setDoWait(true)
                  .build());
      stub.withDeadlineAfter(10, TimeUnit.MINUTES)
          .refresh(RefreshRequest.newBuilder().setIndexName(nrtIndex).build());
      int segments =
          stub.withDeadlineAfter(1, TimeUnit.MINUTES)
              .stats(StatsRequest.newBuilder().setIndexName(nrtIndex).build())
              .getCurrentSearcher()
              .getNumSegments();
      System.out.printf(
          Locale.ROOT,
          "Force merged to %d segment%s in %.1fs%n",
          segments,
          segments == 1 ? "" : "s",
          (System.nanoTime() - t0) / 1e9);
    } finally {
      channel.shutdownNow();
    }
  }

  static Thread progressThread(AtomicLong done, long total, long t0) {
    Thread thread =
        new Thread(
            () -> {
              try {
                while (true) {
                  Thread.sleep(10_000);
                  double seconds = (System.nanoTime() - t0) / 1e9;
                  long d = done.get();
                  System.out.printf(
                      Locale.ROOT,
                      "  %,d / %,d (%.1f%%), %.0f vectors/s%n",
                      d,
                      total,
                      100.0 * d / Math.max(1, total),
                      d / seconds);
                }
              } catch (InterruptedException e) {
                // done
              }
            },
            "load-progress");
    thread.setDaemon(true);
    return thread;
  }
}
