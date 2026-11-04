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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = "groundtruth",
    description =
        "Make a query set: query vectors and their exact nearest neighbors, found by brute force"
            + " over the dataset. Used by query to measure recall. Run once per dataset.")
public class GroundTruthCommand implements Callable<Integer> {
  @CommandLine.Mixin BenchOptions.Dataset dataset;

  @CommandLine.Option(
      names = {"--out"},
      description = "Query set file to write (default: ${DEFAULT-VALUE})",
      defaultValue = "queries.bin")
  Path out;

  @CommandLine.Option(
      names = {"--queries"},
      description = "Number of queries (default: ${DEFAULT-VALUE})",
      defaultValue = "1000")
  int numQueries;

  @CommandLine.Option(
      names = {"--k"},
      description = "Exact neighbors to keep per query (default: ${DEFAULT-VALUE})",
      defaultValue = "100")
  int k;

  @CommandLine.Option(
      names = {"--metric"},
      description = "Distance metric of the index: cosine or euclidean (default: ${DEFAULT-VALUE})",
      defaultValue = "cosine")
  String metric;

  @CommandLine.Option(
      names = {"--filteredPct"},
      description =
          "Percent of queries filtered to one metadata category (a tenth of the vectors)"
              + " (default: ${DEFAULT-VALUE})",
      defaultValue = "0")
  int filteredPct;

  @CommandLine.Option(
      names = {"--queryFvecs"},
      description = "Read query vectors from this .fvecs file, such as sift_query.fvecs")
  Path queryFvecs;

  @CommandLine.Option(
      names = {"--threads"},
      description = "Threads for the brute force scan (default: number of CPUs)",
      defaultValue = "0")
  int threads;

  @Override
  public Integer call() throws Exception {
    int scanThreads = threads > 0 ? threads : Runtime.getRuntime().availableProcessors();
    try (VectorSource source = dataset.open()) {
      List<float[]> vectors = new ArrayList<>(numQueries);
      if (queryFvecs != null) {
        try (VectorSource queries = new VectorSource.Fvecs(queryFvecs, numQueries)) {
          for (long i = 0; i < queries.size(); i++) {
            vectors.add(queries.vector(i));
          }
        }
      } else if (source instanceof VectorSource.Synthetic synthetic) {
        for (long i = 0; i < numQueries; i++) {
          vectors.add(synthetic.query(i));
        }
      } else {
        throw new CommandLine.ParameterException(
            new CommandLine(this), "--queryFvecs is needed with --fvecs datasets");
      }
      List<String> categories = new ArrayList<>(vectors.size());
      java.util.SplittableRandom random = new java.util.SplittableRandom(dataset.seed + 7);
      for (int i = 0; i < vectors.size(); i++) {
        categories.add(random.nextInt(100) < filteredPct ? "c" + random.nextInt(10) : null);
      }
      System.out.printf(
          Locale.ROOT,
          "Finding exact %d nearest neighbors of %,d queries in %,d vectors with %d threads%n",
          k,
          vectors.size(),
          source.size(),
          scanThreads);
      long t0 = System.nanoTime();
      QuerySet set =
          QuerySet.compute(
              source,
              vectors,
              categories,
              metric,
              k,
              scanThreads,
              (done, total) ->
                  System.out.printf(
                      Locale.ROOT,
                      "  scanned %,d / %,d (%.0f%%)%n",
                      done,
                      total,
                      100.0 * done / total));
      set.save(out);
      System.out.printf(Locale.ROOT, "Wrote %s in %.1fs%n", out, (System.nanoTime() - t0) / 1e9);
    }
    return 0;
  }
}
