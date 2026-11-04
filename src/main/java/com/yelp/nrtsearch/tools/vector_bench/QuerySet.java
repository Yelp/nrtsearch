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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Query vectors with their exact nearest neighbors, used to measure recall. Saved in a small binary
 * file so that the brute force search, which reads every vector, runs once per dataset.
 */
public class QuerySet {
  private static final int MAGIC = 0x56514753; // "VQGS"

  /** A query vector, an optional filter category, and the exact nearest neighbor ids. */
  public record Query(float[] vector, String category, long[] neighbors) {}

  private final String metric;
  private final int k;
  private final List<Query> queries;

  public QuerySet(String metric, int k, List<Query> queries) {
    this.metric = metric;
    this.k = k;
    this.queries = queries;
  }

  public String metric() {
    return metric;
  }

  public int k() {
    return k;
  }

  public List<Query> queries() {
    return queries;
  }

  /**
   * Compute exact neighbors by brute force over every vector in the source.
   *
   * @param source vectors in the index
   * @param vectors query vectors
   * @param categories filter category per query (vectors with metadata category equal to it), or
   *     null entries for no filter
   * @param metric cosine or euclidean
   * @param k number of neighbors
   * @param threads threads to scan with
   */
  public static QuerySet compute(
      VectorSource source,
      List<float[]> vectors,
      List<String> categories,
      String metric,
      int k,
      int threads,
      ProgressListener progress)
      throws Exception {
    boolean cosine = metric.equalsIgnoreCase("cosine");
    int q = vectors.size();
    float[][] queryVectors = vectors.toArray(new float[0][]);
    if (cosine) {
      for (int i = 0; i < q; i++) {
        queryVectors[i] = normalize(queryVectors[i]);
      }
    }
    long size = source.size();
    long chunk = (size + threads - 1) / threads;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    List<Future<List<PriorityQueue<long[]>>>> futures = new ArrayList<>();
    java.util.concurrent.atomic.AtomicLong scanned = new java.util.concurrent.atomic.AtomicLong();
    for (int t = 0; t < threads; t++) {
      long from = t * chunk;
      long to = Math.min(size, from + chunk);
      futures.add(
          executor.submit(
              () -> {
                // per query max-heap of (distance bits, id) holding the k smallest distances
                List<PriorityQueue<long[]>> heaps = new ArrayList<>(q);
                for (int i = 0; i < q; i++) {
                  heaps.add(
                      new PriorityQueue<>(
                          k + 1,
                          (a, b) ->
                              Double.compare(
                                  Double.longBitsToDouble(b[0]), Double.longBitsToDouble(a[0]))));
                }
                for (long id = from; id < to; id++) {
                  float[] v = source.vector(id);
                  if (cosine) {
                    v = normalize(v);
                  }
                  String category = "c" + Math.floorMod(id, 10);
                  for (int i = 0; i < q; i++) {
                    String filter = categories.get(i);
                    if (filter != null && !filter.equals(category)) {
                      continue;
                    }
                    double d = cosine ? -dot(queryVectors[i], v) : squaredL2(queryVectors[i], v);
                    PriorityQueue<long[]> heap = heaps.get(i);
                    if (heap.size() < k) {
                      heap.add(new long[] {Double.doubleToLongBits(d), id});
                    } else if (d < Double.longBitsToDouble(heap.peek()[0])) {
                      heap.poll();
                      heap.add(new long[] {Double.doubleToLongBits(d), id});
                    }
                  }
                  long n = scanned.incrementAndGet();
                  if (progress != null && n % 100_000 == 0) {
                    progress.scanned(n, size);
                  }
                }
                return heaps;
              }));
    }
    List<PriorityQueue<long[]>> merged = new ArrayList<>(q);
    for (int i = 0; i < q; i++) {
      merged.add(
          new PriorityQueue<>(
              k + 1,
              (a, b) ->
                  Double.compare(Double.longBitsToDouble(b[0]), Double.longBitsToDouble(a[0]))));
    }
    try {
      for (Future<List<PriorityQueue<long[]>>> f : futures) {
        List<PriorityQueue<long[]>> heaps = f.get();
        for (int i = 0; i < q; i++) {
          for (long[] entry : heaps.get(i)) {
            PriorityQueue<long[]> heap = merged.get(i);
            heap.add(entry);
            if (heap.size() > k) {
              heap.poll();
            }
          }
        }
      }
    } finally {
      executor.shutdownNow();
    }
    List<Query> queries = new ArrayList<>(q);
    for (int i = 0; i < q; i++) {
      PriorityQueue<long[]> heap = merged.get(i);
      long[] neighbors = new long[heap.size()];
      for (int j = neighbors.length - 1; j >= 0; j--) {
        neighbors[j] = heap.poll()[1];
      }
      queries.add(new Query(vectors.get(i), categories.get(i), neighbors));
    }
    return new QuerySet(metric, k, queries);
  }

  /** Reports brute force progress. */
  @FunctionalInterface
  public interface ProgressListener {
    void scanned(long done, long total);
  }

  public void save(Path path) throws IOException {
    try (OutputStream file = Files.newOutputStream(path);
        DataOutputStream out =
            new DataOutputStream(new java.io.BufferedOutputStream(file, 1 << 16))) {
      out.writeInt(MAGIC);
      out.writeUTF(metric);
      out.writeInt(k);
      out.writeInt(queries.size());
      for (Query query : queries) {
        out.writeInt(query.vector().length);
        for (float f : query.vector()) {
          out.writeFloat(f);
        }
        out.writeUTF(query.category() == null ? "" : query.category());
        out.writeInt(query.neighbors().length);
        for (long id : query.neighbors()) {
          out.writeLong(id);
        }
      }
    }
  }

  public static QuerySet load(Path path) throws IOException {
    try (InputStream file = Files.newInputStream(path);
        DataInputStream in = new DataInputStream(new java.io.BufferedInputStream(file, 1 << 16))) {
      if (in.readInt() != MAGIC) {
        throw new IOException("Not a vector_bench query file: " + path);
      }
      String metric = in.readUTF();
      int k = in.readInt();
      int count = in.readInt();
      List<Query> queries = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        float[] v = new float[in.readInt()];
        for (int d = 0; d < v.length; d++) {
          v[d] = in.readFloat();
        }
        String category = in.readUTF();
        long[] neighbors = new long[in.readInt()];
        for (int j = 0; j < neighbors.length; j++) {
          neighbors[j] = in.readLong();
        }
        queries.add(new Query(v, category.isEmpty() ? null : category, neighbors));
      }
      return new QuerySet(metric, k, queries);
    }
  }

  static float[] normalize(float[] v) {
    double norm = 0;
    for (float f : v) {
      norm += f * f;
    }
    norm = Math.sqrt(norm);
    float[] out = new float[v.length];
    if (norm == 0) {
      return out;
    }
    for (int i = 0; i < v.length; i++) {
      out[i] = (float) (v[i] / norm);
    }
    return out;
  }

  static double dot(float[] a, float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      sum += a[i] * b[i];
    }
    return sum;
  }

  static double squaredL2(float[] a, float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      double d = a[i] - b[i];
      sum += d * d;
    }
    return sum;
  }
}
