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

import java.util.Arrays;
import org.apache.lucene.search.Query;

/** Holds the context from the search request required to build highlights. */
public class HighlightSettings {

  private final String highlighterType;
  private final String[] preTags;
  private final String[] postTags;
  private final int fragmentSize;
  private final int maxNumFragments;
  private final Query highlightQuery;
  private final boolean fieldMatch;
  private final boolean scoreOrdered;
  private final String fragmenter;

  public HighlightSettings(
      String highlighterType,
      String[] preTags,
      String[] postTags,
      int fragmentSize,
      int maxNumFragments,
      Query highlightQuery,
      boolean fieldMatch,
      boolean scoreOrdered,
      String fragmenter) {
    this.highlighterType = highlighterType;
    this.preTags = preTags;
    this.postTags = postTags;
    this.fragmentSize = fragmentSize;
    this.maxNumFragments = maxNumFragments;
    this.highlightQuery = highlightQuery;
    this.fieldMatch = fieldMatch;
    this.scoreOrdered = scoreOrdered;
    this.fragmenter = fragmenter;
  }

  public Builder toBuilder() {
    return new Builder()
        .withHighlighterType(this.highlighterType)
        .withPreTags(this.preTags)
        .withPostTags(this.postTags)
        .withFragmentSize(this.fragmentSize)
        .withMaxNumFragments(this.maxNumFragments)
        .withHighlightQuery(this.highlightQuery)
        .withFieldMatch(this.fieldMatch)
        .withScoreOrdered(this.scoreOrdered)
        .withFragmenter(this.fragmenter);
  }

  public String getHighlighterType() {
    return highlighterType;
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

  public Query getHighlightQuery() {
    return highlightQuery;
  }

  public boolean getFieldMatch() {
    return fieldMatch;
  }

  public boolean isScoreOrdered() {
    return scoreOrdered;
  }

  public String getFragmenter() {
    return fragmenter;
  }

  @Override
  public String toString() {
    return "HighlightSettings{"
        + "highlighterType='"
        + highlighterType
        + '\''
        + ", preTags="
        + Arrays.toString(preTags)
        + ", postTags="
        + Arrays.toString(postTags)
        + ", fragmentSize="
        + fragmentSize
        + ", maxNumFragments="
        + maxNumFragments
        + ", highlightQuery="
        + highlightQuery
        + ", fieldMatch="
        + fieldMatch
        + ", scoreOrdered="
        + scoreOrdered
        + ", fragmenter='"
        + fragmenter
        + '\''
        + '}';
  }

  public static final class Builder {

    private String highlighterType;
    private String[] preTags;
    private String[] postTags;
    private int fragmentSize;
    private int maxNumFragments;
    private Query highlightQuery;
    private boolean fieldMatch;
    private boolean scoreOrdered;
    private String fragmenter;

    public Builder() {}

    public Builder withHighlighterType(String highlighterType) {
      this.highlighterType = highlighterType;
      return this;
    }

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

    public Builder withHighlightQuery(Query highlightQuery) {
      this.highlightQuery = highlightQuery;
      return this;
    }

    public Builder withFieldMatch(boolean fieldMatch) {
      this.fieldMatch = fieldMatch;
      return this;
    }

    public Builder withScoreOrdered(boolean scoreOrdered) {
      this.scoreOrdered = scoreOrdered;
      return this;
    }

    public Builder withFragmenter(String fragmenter) {
      this.fragmenter = fragmenter;
      return this;
    }

    public HighlightSettings build() {
      return new HighlightSettings(
          highlighterType,
          preTags,
          postTags,
          fragmentSize,
          maxNumFragments,
          highlightQuery,
          fieldMatch,
          scoreOrdered,
          fragmenter);
    }
  }
}
