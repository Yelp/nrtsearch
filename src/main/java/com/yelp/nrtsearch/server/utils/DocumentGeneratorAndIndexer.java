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

import com.google.gson.Gson;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.LuceneServerClient;
import java.util.concurrent.Callable;
import java.util.logging.Logger;
import java.util.stream.Stream;

/** */
public class DocumentGeneratorAndIndexer implements Callable<Long> {
  private final Stream<String> lines;
  private final Gson gson = new Gson();
  final LuceneServerClient luceneServerClient;
  private static final Logger logger =
      Logger.getLogger(DocumentGeneratorAndIndexer.class.getName());
  private final OneDocBuilder oneDocBuilder;

  public DocumentGeneratorAndIndexer(
      OneDocBuilder oneDocBuilder, Stream<String> lines, LuceneServerClient luceneServerClient) {
    this.lines = lines;
    this.luceneServerClient = luceneServerClient;
    this.oneDocBuilder = oneDocBuilder;
  }

  private Stream<AddDocumentRequest> buildDocs() {
    Stream.Builder<AddDocumentRequest> builder = Stream.builder();
    lines.forEach(line -> builder.add(oneDocBuilder.buildOneDoc(line, gson)));
    return builder.build();
  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  @Override
  public Long call() throws Exception {
    long t1 = System.nanoTime();
    Stream<AddDocumentRequest> addDocumentRequestStream = buildDocs();
    long t2 = System.nanoTime();
    long timeMilliSecs = (t2 - t1) / (1000 * 100);
    String threadId = Thread.currentThread().getName() + Thread.currentThread().getId();
    logger.info(
        String.format("threadId: %s took %s milliSecs to buildDocs ", threadId, timeMilliSecs));

    t1 = System.nanoTime();
    Long genId = new IndexerTask().index(luceneServerClient, addDocumentRequestStream);
    t2 = System.nanoTime();
    timeMilliSecs = (t2 - t1) / (1000 * 100);
    logger.info(
        String.format("threadId: %s took %s milliSecs to indexDocs ", threadId, timeMilliSecs));
    return genId;
  }
}
