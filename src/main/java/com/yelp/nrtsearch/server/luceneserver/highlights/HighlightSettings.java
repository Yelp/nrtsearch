/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.highlights;

import org.apache.lucene.search.vectorhighlight.FieldQuery;

/** Holds the context from the search request required to build highlights. */
public class HighlightSettings {

  private final String[] preTags;
  private final String[] postTags;
  private final int fragmentSize;
  private final int maxNumFragments;
  private final FieldQuery fieldQuery;

  public HighlightSettings(
      String[] preTags,
      String[] postTags,
      int fragmentSize,
      int maxNumFragments,
      FieldQuery fieldQuery) {
    this.preTags = preTags;
    this.postTags = postTags;
    this.fragmentSize = fragmentSize;
    this.maxNumFragments = maxNumFragments;
    this.fieldQuery = fieldQuery;
  }

  public String[] getPreTags() {
    return preTags;
  }

  public String[] getPostTags() {
    return postTags;
  }

  public int getFragmentSize() {
    return fragmentSize;
  }

  public int getMaxNumFragments() {
    return maxNumFragments;
  }

  public FieldQuery getFieldQuery() {
    return fieldQuery;
  }

  public static final class Builder {

    private String[] preTags;
    private String[] postTags;
    private int fragmentSize;
    private int maxNumFragments;
    private FieldQuery fieldQuery;

    public Builder() {}

    public Builder withPreTags(String[] preTags) {
      this.preTags = preTags;
      return this;
    }

    public Builder withPostTags(String[] postTags) {
      this.postTags = postTags;
      return this;
    }

    public Builder withFragmentSize(int fragmentSize) {
      this.fragmentSize = fragmentSize;
      return this;
    }

    public Builder withMaxNumFragments(int maxNumFragments) {
      this.maxNumFragments = maxNumFragments;
      return this;
    }

    public Builder withFieldQuery(FieldQuery fieldQuery) {
      this.fieldQuery = fieldQuery;
      return this;
    }

    public HighlightSettings build() {
      return new HighlightSettings(preTags, postTags, fragmentSize, maxNumFragments, fieldQuery);
    }
  }
}
