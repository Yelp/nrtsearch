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
package com.yelp.nrtsearch.server.search;

import org.apache.lucene.search.TopDocs;

/**
 * Result from executing a single retriever. Contains the retriever name, the {@link TopDocs} it
 * produced, and its execution time in milliseconds.
 */
public class RetrieverResult {

  private final String name;
  private final TopDocs topDocs;
  private final double timeTakenMs;

  public RetrieverResult(String name, TopDocs topDocs, double timeTakenMs) {
    this.name = name;
    this.topDocs = topDocs;
    this.timeTakenMs = timeTakenMs;
  }

  /** Get the retriever name. */
  public String getName() {
    return name;
  }

  /** Get the top documents produced by this retriever. */
  public TopDocs getTopDocs() {
    return topDocs;
  }

  /** Get the retriever execution time in milliseconds. */
  public double getTimeTakenMs() {
    return timeTakenMs;
  }
}
