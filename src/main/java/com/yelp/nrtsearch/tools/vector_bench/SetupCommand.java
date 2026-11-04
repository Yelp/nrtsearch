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
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = "setup",
    description = "Create the vector bucket and index. Deletes an existing index of the same name.")
public class SetupCommand implements Callable<Integer> {
  @CommandLine.Mixin BenchOptions.Target target;
  @CommandLine.Mixin BenchOptions.Dataset dataset;

  @CommandLine.Option(
      names = {"--metric"},
      description = "Distance metric: cosine or euclidean (default: ${DEFAULT-VALUE})",
      defaultValue = "cosine")
  String metric;

  @CommandLine.Option(
      names = {"--keep"},
      description = "Fail if the index exists, instead of deleting it")
  boolean keep;

  @Override
  public Integer call() throws Exception {
    int dimension = dataset.dimension();
    try (BenchClient client = target.client(1, true)) {
      ObjectNode bucket = BenchClient.MAPPER.createObjectNode();
      bucket.put("vectorBucketName", target.bucket);
      try {
        client.call("CreateVectorBucket", bucket);
        System.out.println("Created vector bucket " + target.bucket);
      } catch (BenchClient.BenchException e) {
        if (e.getStatus() != 409) {
          throw e;
        }
      }
      if (!keep) {
        try {
          client.call("DeleteIndex", target.request());
          System.out.println("Deleted existing index " + target.index);
        } catch (BenchClient.BenchException e) {
          if (e.getStatus() != 404) {
            throw e;
          }
        }
      }
      ObjectNode create = target.request();
      create.put("dataType", "float32");
      create.put("dimension", dimension);
      create.put("distanceMetric", metric);
      create.putObject("metadataConfiguration").putArray("nonFilterableMetadataKeys").add("text");
      client.call("CreateIndex", create);
      System.out.printf(
          "Created index %s/%s: dimension %d, %s%n",
          target.bucket, target.index, dimension, metric);
    }
    return 0;
  }
}
