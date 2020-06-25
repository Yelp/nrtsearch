/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.utils;

import com.yelp.nrtsearch.server.grpc.LuceneServerClient;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ParallelDocumentIndexer {
  private static final Logger logger = Logger.getLogger(ParallelDocumentIndexer.class.getName());
  private static final int DOCS_PER_INDEX_REQUEST = 1000;

  public static List<Future<Long>> buildAndIndexDocs(
      OneDocBuilder oneDocBuilder,
      Path path,
      ExecutorService executorService,
      LuceneServerClient luceneServerClient)
      throws IOException, InterruptedException {
    try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
      String line;
      List<String> rawLines = new ArrayList();
      List<Future<Long>> futures = new ArrayList<>();
      while ((line = br.readLine()) != null) {
        if (rawLines.size() < DOCS_PER_INDEX_REQUEST) {
          rawLines.add(line);
        } else {
          // launch indexing task
          logger.info(
              String.format(
                  "Launching DocumentGeneratorAndIndexer task for %s docs",
                  DOCS_PER_INDEX_REQUEST));
          List<String> copiedRawLines = new ArrayList<>(rawLines);
          Future<Long> genIdFuture =
              submitTask(oneDocBuilder, executorService, luceneServerClient, copiedRawLines);
          futures.add(genIdFuture);
          rawLines.clear();
        }
      }
      if (!rawLines.isEmpty()) {
        // convert left over docs
        logger.info(
            String.format(
                "Launching DocumentGeneratorAndIndexer task for %s docs", rawLines.size()));
        Future<Long> genIdFuture =
            submitTask(oneDocBuilder, executorService, luceneServerClient, rawLines);
        futures.add(genIdFuture);
      }
      return futures;
    }
  }

  private static Future<Long> submitTask(
      OneDocBuilder oneDocBuilder,
      ExecutorService executorService,
      LuceneServerClient luceneServerClient,
      List<String> rawLines)
      throws InterruptedException {
    Future<Long> genIdFuture;
    while (true) {
      try {
        genIdFuture =
            executorService.submit(
                new DocumentGeneratorAndIndexer(
                    oneDocBuilder, rawLines.stream(), luceneServerClient));
        return genIdFuture;
      } catch (RejectedExecutionException e) {
        logger.log(
            Level.WARNING,
            String.format("Waiting for 1s for LinkedBlockingQueue to have more capacity"),
            e);
        Thread.sleep(1000);
      }
    }
  }
}
