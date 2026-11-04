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
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import picocli.CommandLine;

@CommandLine.Command(
    name = "stream",
    description =
        "Send a steady stream of writes, like a realtime indexing pipeline: inserts of new ids,"
            + " updates of existing ids, and deletes, at a target rate of vectors per second.")
public class StreamCommand implements Callable<Integer> {
  @CommandLine.Mixin BenchOptions.Target target;
  @CommandLine.Mixin BenchOptions.Dataset dataset;

  @CommandLine.Option(
      names = {"--rate"},
      description = "Target vectors written per second (default: ${DEFAULT-VALUE})",
      defaultValue = "100")
  double rate;

  @CommandLine.Option(
      names = {"--duration"},
      description =
          "How long to run, in seconds, or 0 to run until stopped (default: ${DEFAULT-VALUE})",
      defaultValue = "60")
  long durationSeconds;

  @CommandLine.Option(
      names = {"--batch"},
      description = "Vectors per write call, at most 500 (default: ${DEFAULT-VALUE})",
      defaultValue = "10")
  int batch;

  @CommandLine.Option(
      names = {"--insertPct"},
      description = "Percent of writes that insert new ids (default: ${DEFAULT-VALUE})",
      defaultValue = "50")
  int insertPct;

  @CommandLine.Option(
      names = {"--deletePct"},
      description =
          "Percent of writes that delete existing ids; the rest update existing ids"
              + " (default: ${DEFAULT-VALUE})",
      defaultValue = "10")
  int deletePct;

  @CommandLine.Option(
      names = {"--threads"},
      description = "Concurrent write calls (default: ${DEFAULT-VALUE})",
      defaultValue = "2")
  int threads;

  @CommandLine.Option(
      names = {"--refreshEach"},
      description =
          "Refresh after every write call, as the S3 Vectors API does, instead of relying on the"
              + " server's periodic refresh")
  boolean refreshEach;

  @Override
  public Integer call() throws Exception {
    if (batch < 1 || batch > 500) {
      throw new CommandLine.ParameterException(
          new CommandLine(this), "--batch must be between 1 and 500");
    }
    if (insertPct < 0 || deletePct < 0 || insertPct + deletePct > 100) {
      throw new CommandLine.ParameterException(
          new CommandLine(this), "--insertPct and --deletePct must add up to at most 100");
    }
    if (dataset.fvecs != null && insertPct > 0) {
      throw new CommandLine.ParameterException(
          new CommandLine(this),
          "Inserts generate new vectors, which needs synthetic data; use --insertPct 0 with"
              + " --fvecs");
    }
    // existing ids are [0, dataset size); inserts continue from there
    long existing = dataset.count;
    AtomicLong nextInsertId = new AtomicLong(existing);
    AtomicLong inserted = new AtomicLong();
    AtomicLong updated = new AtomicLong();
    AtomicLong deleted = new AtomicLong();
    LatencyRecorder putLatency = new LatencyRecorder();
    LatencyRecorder deleteLatency = new LatencyRecorder();
    // a batch is one write call, so the call rate is the vector rate divided by the batch size
    RateLimiter limiter = new RateLimiter(rate / batch);
    AtomicBoolean stop = new AtomicBoolean();
    long t0 = System.nanoTime();
    long deadline =
        durationSeconds > 0 ? t0 + TimeUnit.SECONDS.toNanos(durationSeconds) : Long.MAX_VALUE;

    // updates use vectors from a different seed, so an update changes the vector
    VectorSource.Synthetic updates =
        new VectorSource.Synthetic(
            dataset.dimension(),
            Long.MAX_VALUE,
            dataset.clusters,
            dataset.spread,
            dataset.seed + 1);
    System.out.printf(
        Locale.ROOT,
        "Streaming %.0f vectors/s (%d%% insert, %d%% update, %d%% delete) into %s/%s%n",
        rate,
        insertPct,
        100 - insertPct - deletePct,
        deletePct,
        target.bucket,
        target.index);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> stop.set(true)));
    // inserts are new generated vectors from the dataset's seed (checked above to be synthetic)
    VectorSource.Synthetic inserts =
        dataset.fvecs == null
            ? new VectorSource.Synthetic(
                dataset.dimension(), Long.MAX_VALUE, dataset.clusters, dataset.spread, dataset.seed)
            : null;
    try (BenchClient client = target.client(threads, refreshEach)) {
      ExecutorService executor = Executors.newFixedThreadPool(threads);
      try {
        Future<?>[] futures = new Future<?>[threads];
        for (int t = 0; t < threads; t++) {
          long threadSeed = dataset.seed * 1000 + t;
          futures[t] =
              executor.submit(
                  () -> {
                    SplittableRandom random = new SplittableRandom(threadSeed);
                    while (!stop.get() && System.nanoTime() < deadline) {
                      long scheduled = limiter.acquire();
                      int roll = random.nextInt(100);
                      try {
                        if (roll < deletePct) {
                          ObjectNode delete = target.request();
                          ArrayNode keys = delete.putArray("keys");
                          for (int i = 0; i < batch; i++) {
                            keys.add(BenchOptions.key(random.nextLong(existing)));
                          }
                          client.call("DeleteVectors", delete);
                          deleteLatency.record(System.nanoTime() - scheduled);
                          deleted.addAndGet(batch);
                        } else {
                          boolean insert = roll < deletePct + insertPct;
                          ObjectNode put = target.request();
                          ArrayNode vectors = put.putArray("vectors");
                          for (int i = 0; i < batch; i++) {
                            long id =
                                insert ? nextInsertId.getAndIncrement() : random.nextLong(existing);
                            float[] v = insert ? inserts.vector(id) : updates.vector(id);
                            ObjectNode vector = vectors.addObject();
                            vector.put("key", BenchOptions.key(id));
                            ArrayNode data = vector.putObject("data").putArray("float32");
                            for (float f : v) {
                              data.add(f);
                            }
                            vector.set("metadata", BenchOptions.metadata(id));
                          }
                          client.call("PutVectors", put);
                          putLatency.record(System.nanoTime() - scheduled);
                          (insert ? inserted : updated).addAndGet(batch);
                        }
                      } catch (Exception e) {
                        (roll < deletePct ? deleteLatency : putLatency).recordError();
                        System.err.println("Write failed: " + e.getMessage());
                      }
                    }
                    return null;
                  });
        }
        long lastReport = System.nanoTime();
        while (!stop.get() && System.nanoTime() < deadline) {
          Thread.sleep(200);
          if (System.nanoTime() - lastReport > TimeUnit.SECONDS.toNanos(10)) {
            lastReport = System.nanoTime();
            double seconds = (lastReport - t0) / 1e9;
            long total = inserted.get() + updated.get() + deleted.get();
            System.out.printf(
                Locale.ROOT,
                "  %.0fs: %,d written (%.0f/s), put p99 %.1fms, delete p99 %.1fms, next id %,d%n",
                seconds,
                total,
                total / seconds,
                putLatency.summary().percentileMs(99),
                deleteLatency.summary().percentileMs(99),
                nextInsertId.get());
          }
        }
        stop.set(true);
        for (Future<?> f : futures) {
          f.get();
        }
      } finally {
        executor.shutdownNow();
      }
    }
    double seconds = (System.nanoTime() - t0) / 1e9;
    long total = inserted.get() + updated.get() + deleted.get();
    System.out.printf(
        Locale.ROOT,
        "Wrote %,d vectors in %.1fs (%.0f/s): %,d inserted, %,d updated, %,d deleted%n"
            + "PutVectors latency:    %s%n"
            + "DeleteVectors latency: %s%n"
            + "Inserted ids go up to %,d; pass --count %d to groundtruth to include them%n",
        total,
        seconds,
        total / seconds,
        inserted.get(),
        updated.get(),
        deleted.get(),
        putLatency.summary().format(),
        deleteLatency.summary().format(),
        nextInsertId.get() - 1,
        nextInsertId.get());
    return 0;
  }
}
