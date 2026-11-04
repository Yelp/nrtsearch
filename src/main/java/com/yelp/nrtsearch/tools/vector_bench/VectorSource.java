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

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.SplittableRandom;

/**
 * Source of vectors, addressed by id so that loads, query sets and exact neighbor computations all
 * see the same data.
 */
public interface VectorSource extends Closeable {

  int dimension();

  /** Number of vectors, or -1 if unbounded. */
  long size();

  /** Vector with the given id, in [0, size). */
  float[] vector(long id) throws IOException;

  @Override
  default void close() throws IOException {}

  /**
   * Generated vectors from a mixture of Gaussian clusters. Real embeddings are clustered, which
   * gives HNSW the neighbor structure it is built for; uniformly random vectors are not, and make
   * recall and cache locality unrealistic.
   *
   * <p>Vector {@code i} depends only on the seed and {@code i}, so any range can be generated in
   * any order, by any number of threads, and again later for exact search.
   */
  class Synthetic implements VectorSource {
    private final int dimension;
    private final long size;
    private final long seed;
    private final float[][] centers;
    private final float spread;

    /**
     * @param dimension vector dimension
     * @param size number of vectors
     * @param clusters number of clusters
     * @param spread standard deviation of each cluster, relative to the spread of the centers
     * @param seed random seed
     */
    public Synthetic(int dimension, long size, int clusters, float spread, long seed) {
      if (dimension < 1 || clusters < 1 || spread <= 0) {
        throw new IllegalArgumentException("dimension, clusters and spread must be positive");
      }
      this.dimension = dimension;
      this.size = size;
      this.seed = seed;
      this.spread = spread;
      SplittableRandom random = new SplittableRandom(seed);
      this.centers = new float[clusters][dimension];
      for (float[] center : centers) {
        for (int d = 0; d < dimension; d++) {
          center[d] = (float) gaussian(random);
        }
      }
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public long size() {
      return size;
    }

    @Override
    public float[] vector(long id) {
      // a generator per vector, derived from the seed and id
      SplittableRandom random = new SplittableRandom(mix(seed ^ (id * 0x9E3779B97F4A7C15L)));
      float[] center = centers[random.nextInt(centers.length)];
      float[] v = new float[dimension];
      for (int d = 0; d < dimension; d++) {
        v[d] = center[d] + spread * (float) gaussian(random);
      }
      return v;
    }

    /** A query near the data: a vector from the same clusters that is not in the index. */
    public float[] query(long queryId) {
      return vector(-1 - queryId);
    }

    private static double gaussian(SplittableRandom random) {
      // Box-Muller
      double u = 1.0 - random.nextDouble();
      double v = random.nextDouble();
      return Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2 * Math.PI * v);
    }

    private static long mix(long z) {
      z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
      z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
      return z ^ (z >>> 31);
    }
  }

  /**
   * Vectors from an {@code .fvecs} file, the format of the standard ANN benchmark sets (SIFT, GIST,
   * Deep1B). Each vector is a little endian int32 dimension followed by that many float32 values.
   * Vectors are read with positional reads, so the file can be larger than memory.
   */
  class Fvecs implements VectorSource {
    private final Path path;
    private final int dimension;
    private final long size;
    private final long recordBytes;
    private final java.nio.channels.FileChannel channel;

    public Fvecs(Path path, long limit) throws IOException {
      this.path = path;
      this.channel = java.nio.channels.FileChannel.open(path);
      ByteBuffer header = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
      if (channel.read(header, 0) != 4) {
        throw new IOException("Empty fvecs file: " + path);
      }
      this.dimension = header.getInt(0);
      if (dimension < 1 || dimension > 65536) {
        throw new IOException("Not an fvecs file (dimension " + dimension + "): " + path);
      }
      this.recordBytes = 4L + 4L * dimension;
      long fileSize = channel.size();
      if (fileSize % recordBytes != 0) {
        throw new IOException(
            "fvecs file size " + fileSize + " is not a multiple of " + recordBytes + ": " + path);
      }
      long count = fileSize / recordBytes;
      this.size = limit > 0 ? Math.min(limit, count) : count;
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public long size() {
      return size;
    }

    @Override
    public float[] vector(long id) throws IOException {
      if (id < 0 || id >= size) {
        throw new IllegalArgumentException("Vector id " + id + " out of range in " + path);
      }
      ByteBuffer buffer = ByteBuffer.allocate((int) recordBytes).order(ByteOrder.LITTLE_ENDIAN);
      long position = id * recordBytes;
      while (buffer.hasRemaining()) {
        int read = channel.read(buffer, position + buffer.position());
        if (read < 0) {
          throw new EOFException("Truncated fvecs file: " + path);
        }
      }
      buffer.flip();
      int d = buffer.getInt();
      if (d != dimension) {
        throw new IOException("Vector " + id + " has dimension " + d + ", expected " + dimension);
      }
      float[] v = new float[dimension];
      buffer.asFloatBuffer().get(v);
      return v;
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }

  /**
   * Read an {@code .ivecs} ground truth file: for each query, a little endian int32 count followed
   * by that many int32 neighbor ids, nearest first.
   */
  static int[][] readIvecs(Path path, int limit) throws IOException {
    java.util.List<int[]> rows = new java.util.ArrayList<>();
    try (InputStream in = new BufferedInputStream(Files.newInputStream(path), 1 << 20);
        DataInputStream data = new DataInputStream(in)) {
      byte[] intBytes = new byte[4];
      while (limit <= 0 || rows.size() < limit) {
        int read = data.read(intBytes);
        if (read < 0) {
          break;
        }
        if (read != 4) {
          data.readFully(intBytes, read, 4 - read);
        }
        int count = ByteBuffer.wrap(intBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
        byte[] rowBytes = new byte[4 * count];
        data.readFully(rowBytes);
        int[] row = new int[count];
        ByteBuffer.wrap(rowBytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(row);
        rows.add(row);
      }
    }
    return rows.toArray(new int[0][]);
  }
}
