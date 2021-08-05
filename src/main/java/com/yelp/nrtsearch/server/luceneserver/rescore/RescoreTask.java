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
package com.yelp.nrtsearch.server.luceneserver.rescore;

import java.io.IOException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.TopDocs;

/**
 * A wrapper component for {@link Rescorer} and <i>int windowSize</i>. It has a public
 * <i>rescore</i> method which is called by the {@link
 * com.yelp.nrtsearch.server.luceneserver.SearchHandler} to rescore the first-pass hits.
 */
public class RescoreTask {

  private final Rescorer rescorer;
  private final int windowSize;
  private final String name;

  private RescoreTask(Builder builder) {
    rescorer = builder.rescorer;
    windowSize = builder.windowSize;
    name = builder.name;
  }

  /**
   * This wrapper method calls {@link Rescorer} <i>rescore</i> method with <i>windowSize</i>
   * parameter passed from {@link RescoreTask} class field.
   *
   * @param searcher index searcher instance
   * @param hits results from the previous search pass
   * @return rescored documents
   * @throws IOException
   */
  public TopDocs rescore(IndexSearcher searcher, TopDocs hits) throws IOException {
    return rescorer.rescore(searcher, hits, windowSize);
  }

  public String getName() {
    return name;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private Rescorer rescorer;
    private int windowSize;
    private String name;

    private Builder() {}

    public Builder setRescorer(Rescorer rescorer) {
      this.rescorer = rescorer;
      return this;
    }

    public Builder setWindowSize(int windowSize) {
      this.windowSize = windowSize;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public RescoreTask build() {
      return new RescoreTask(this);
    }
  }
}
