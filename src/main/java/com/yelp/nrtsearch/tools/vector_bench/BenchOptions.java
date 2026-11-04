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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yelp.nrtsearch.vectors.VectorService;
import java.io.IOException;
import java.nio.file.Path;
import picocli.CommandLine;

/** Options shared by the vector_bench commands, used as picocli mixins. */
public final class BenchOptions {
  private BenchOptions() {}

  /** Where requests go, and which index. */
  public static class Target {
    @CommandLine.Option(
        names = {"--host"},
        description = "nrtsearch host to send requests to (default: ${DEFAULT-VALUE})",
        defaultValue = "localhost")
    public String host;

    @CommandLine.Option(
        names = {"--port"},
        description = "nrtsearch gRPC port (default: ${DEFAULT-VALUE})",
        defaultValue = "50051")
    public int port;

    @CommandLine.Option(
        names = {"--gateway"},
        description =
            "Send requests to a running vector gateway at this URL (for example"
                + " http://localhost:8080) instead of directly to nrtsearch")
    public String gatewayUrl;

    @CommandLine.Option(
        names = {"--registryFile"},
        description =
            "Bucket registry file used in direct mode. Shared by setup, load, stream and query;"
                + " point a gateway at the same file to use the index through it"
                + " (default: ${DEFAULT-VALUE})",
        defaultValue = "vector_bench_registry.json")
    public Path registryFile;

    @CommandLine.Option(
        names = {"--bucket"},
        description = "Vector bucket name (default: ${DEFAULT-VALUE})",
        defaultValue = "bench")
    public String bucket;

    @CommandLine.Option(
        names = {"--index"},
        description = "Vector index name (default: ${DEFAULT-VALUE})",
        defaultValue = "vectors")
    public String index;

    /** Create a client for this target. */
    public BenchClient client(int threads, boolean refreshOnWrite) {
      if (gatewayUrl != null) {
        return new BenchClient.Http(gatewayUrl, threads);
      }
      VectorService.Options options = new VectorService.Options();
      options.refreshOnWrite = refreshOnWrite;
      return new BenchClient.Direct(host, port, registryFile, options);
    }

    /** Request with the bucket and index names set. */
    public ObjectNode request() {
      ObjectNode node = BenchClient.MAPPER.createObjectNode();
      node.put("vectorBucketName", bucket);
      node.put("indexName", index);
      return node;
    }
  }

  /** The vectors to load and query. */
  public static class Dataset {
    @CommandLine.Option(
        names = {"--fvecs"},
        description =
            "Read vectors from this .fvecs file (SIFT, GIST, Deep1B format) instead of"
                + " generating them")
    public Path fvecs;

    @CommandLine.Option(
        names = {"--count"},
        description =
            "Dataset size: the number of vectors that exist, which load, stream and"
                + " groundtruth use as the id range. With --fvecs, at most this many are read from"
                + " the file, 0 for all (default: ${DEFAULT-VALUE})",
        defaultValue = "100000")
    public long count;

    @CommandLine.Option(
        names = {"--dim"},
        description = "Dimension of generated vectors (default: ${DEFAULT-VALUE})",
        defaultValue = "768")
    public int dim;

    @CommandLine.Option(
        names = {"--clusters"},
        description = "Number of clusters in generated data (default: ${DEFAULT-VALUE})",
        defaultValue = "1000")
    public int clusters;

    @CommandLine.Option(
        names = {"--spread"},
        description =
            "Spread of each generated cluster, relative to the spread of the cluster centers"
                + " (default: ${DEFAULT-VALUE})",
        defaultValue = "0.3")
    public float spread;

    @CommandLine.Option(
        names = {"--seed"},
        description = "Seed for generated data (default: ${DEFAULT-VALUE})",
        defaultValue = "42")
    public long seed;

    /** Open the vectors. */
    public VectorSource open() throws IOException {
      if (fvecs != null) {
        return new VectorSource.Fvecs(fvecs, count);
      }
      return new VectorSource.Synthetic(dim, count, clusters, spread, seed);
    }

    /** Dimension, reading the file header for fvecs. */
    public int dimension() throws IOException {
      if (fvecs != null) {
        try (VectorSource source = open()) {
          return source.dimension();
        }
      }
      return dim;
    }
  }

  /** Key of vector {@code id}. Zero padded so ListVectors returns ids in order. */
  public static String key(long id) {
    return String.format("v%012d", id);
  }

  /** Vector id from a key made by {@link #key}. */
  public static long id(String key) {
    return Long.parseLong(key.substring(1));
  }

  /**
   * Metadata for vector {@code id}, deterministic so filtered queries can be checked: a category
   * (one of 10), a numeric bucket (0 to 99), and a non-filterable text field.
   */
  public static ObjectNode metadata(long id) {
    ObjectNode metadata = BenchClient.MAPPER.createObjectNode();
    metadata.put("category", "c" + Math.floorMod(id, 10));
    metadata.put("bucket", Math.floorMod(id * 31, 100));
    metadata.put("text", "vector " + id);
    return metadata;
  }
}
