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
package com.yelp.nrtsearch.yelp_reviews.utils;

import com.yelp.nrtsearch.server.grpc.NrtsearchClient;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelDocumentIndexer {
  private static final Logger logger =
      LoggerFactory.getLogger(ParallelDocumentIndexer.class.getName());
  private static final int DOCS_PER_INDEX_REQUEST = 1000;

  public static List<Future<Long>> buildAndIndexDocs(
      OneDocBuilder oneDocBuilder,
      Path path,
      ExecutorService executorService,
      NrtsearchClient nrtsearchClient)
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
              "Launching DocumentGeneratorAndIndexer task for {} docs", DOCS_PER_INDEX_REQUEST);
          List<String> copiedRawLines = new ArrayList<>(rawLines);
          Future<Long> genIdFuture =
              submitTask(oneDocBuilder, executorService, nrtsearchClient, copiedRawLines);
          futures.add(genIdFuture);
          rawLines.clear();
        }
      }
      if (!rawLines.isEmpty()) {
        // convert left over docs
        logger.info("Launching DocumentGeneratorAndIndexer task for {} docs", rawLines.size());
        Future<Long> genIdFuture =
            submitTask(oneDocBuilder, executorService, nrtsearchClient, rawLines);
        futures.add(genIdFuture);
      }
      return futures;
    }
  }

  private static Future<Long> submitTask(
      OneDocBuilder oneDocBuilder,
      ExecutorService executorService,
      NrtsearchClient nrtsearchClient,
      List<String> rawLines)
      throws InterruptedException {
    Future<Long> genIdFuture;
    while (true) {
      try {
        genIdFuture =
            executorService.submit(
                new DocumentGeneratorAndIndexer(oneDocBuilder, rawLines.stream(), nrtsearchClient));
        return genIdFuture;
      } catch (RejectedExecutionException e) {
        logger.warn("Waiting for 1s for LinkedBlockingQueue to have more capacity", e);
        Thread.sleep(1000);
      }
    }
  }
}
