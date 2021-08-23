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

import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import java.io.IOException;
import org.apache.lucene.search.TopDocs;

/**
 * A wrapper component for {@link RescoreOperation} and <i>int windowSize</i>. It has a public
 * <i>rescore</i> method which is called by the {@link
 * com.yelp.nrtsearch.server.luceneserver.SearchHandler} to rescore the first-pass hits.
 */
public class RescoreTask {

  private final RescoreOperation rescoreOperation;
  private final int windowSize;
  private final String name;

  private RescoreTask(Builder builder) {
    rescoreOperation = builder.rescoreOperation;
    windowSize = builder.windowSize;
    name = builder.name;
  }

  /**
   * This wrapper method calls {@link RescoreOperation} <i>rescore</i> method with a {@link
   * RescoreContext} to pass additional information.
   *
   * @param hits results from the previous search pass
   * @return rescored documents
   * @throws IOException on error loading index data
   */
  public TopDocs rescore(TopDocs hits, SearchContext searchContext) throws IOException {
    RescoreContext context = new RescoreContext(windowSize, searchContext);
    return rescoreOperation.rescore(hits, context);
  }

  public String getName() {
    return name;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private RescoreOperation rescoreOperation;
    private int windowSize;
    private String name;

    private Builder() {}

    public Builder setRescoreOperation(RescoreOperation rescoreOperation) {
      this.rescoreOperation = rescoreOperation;
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
