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
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.search.Query;

/** Holds the context from the search request required to build highlights. */
public class HighlightSettings {

  private final Highlighter highlighter;
  private final String[] preTags;
  private final String[] postTags;
  private final int fragmentSize;
  private final int maxNumFragments;
  private final Query highlightQuery;
  private final boolean fieldMatch;
  private final boolean scoreOrdered;
  private final String fragmenter;
  private final boolean discreteMultivalue;
  private final Map<String, Object> customHighlighterParams;
  private final String boundaryScanner;
  private final Character[] boundaryChars;
  private final int boundaryMaxScan;
  private final Locale boundaryScannerLocale;

  public HighlightSettings(
      Highlighter highlighter,
      String[] preTags,
      String[] postTags,
      int fragmentSize,
      int maxNumFragments,
      Query highlightQuery,
      boolean fieldMatch,
      boolean scoreOrdered,
      String fragmenter,
      boolean discreteMultivalue,
      String boundaryScanner,
      Character[] boundaryChars,
      int boundaryMaxScan,
      Locale boundaryScannerLocale,
      Map<String, Object> customHighlighterParams) {
    this.highlighter = highlighter;
    this.preTags = preTags;
    this.postTags = postTags;
    this.fragmentSize = fragmentSize;
    this.maxNumFragments = maxNumFragments;
    this.highlightQuery = highlightQuery;
    this.fieldMatch = fieldMatch;
    this.scoreOrdered = scoreOrdered;
    this.fragmenter = fragmenter;
    this.discreteMultivalue = discreteMultivalue;
    this.boundaryScanner = boundaryScanner;
    this.boundaryChars = boundaryChars;
    this.boundaryMaxScan = boundaryMaxScan;
    this.boundaryScannerLocale = boundaryScannerLocale;
    this.customHighlighterParams = customHighlighterParams;
  }

  public Builder toBuilder() {
    return new Builder()
        .withHighlighter(this.highlighter)
        .withPreTags(this.preTags)
        .withPostTags(this.postTags)
        .withFragmentSize(this.fragmentSize)
        .withMaxNumFragments(this.maxNumFragments)
        .withHighlightQuery(this.highlightQuery)
        .withFieldMatch(this.fieldMatch)
        .withScoreOrdered(this.scoreOrdered)
        .withFragmenter(this.fragmenter)
        .withDiscreteMultivalue(this.discreteMultivalue)
        .withBoundaryScanner(this.boundaryScanner)
        .withBoundaryChars(this.boundaryChars)
        .withBoundaryMaxScan(this.boundaryMaxScan)
        .withBoundaryScannerLocale(this.boundaryScannerLocale)
        .withCustomHighlighterParams(this.customHighlighterParams);
  }

  public Highlighter getHighlighter() {
    return highlighter;
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

  public boolean getDiscreteMultivalue() {
    return discreteMultivalue;
  }

  public String getBoundaryScanner() {
    return boundaryScanner;
  }

  public Character[] getBoundaryChars() {
    return boundaryChars;
  }

  public int getBoundaryMaxScan() {
    return boundaryMaxScan;
  }

  public Locale getBoundaryScannerLocale() {
    return boundaryScannerLocale;
  }

  public Map<String, Object> getCustomHighlighterParams() {
    return customHighlighterParams;
  }

  @Override
  public String toString() {
    return "HighlightSettings{"
        + "highlighter="
        + highlighter
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
        + ", discreteMultivalue="
        + discreteMultivalue
        + ", customHighlighterParams="
        + customHighlighterParams
        + ", boundaryScanner='"
        + boundaryScanner
        + '\''
        + ", boundaryChars="
        + Arrays.toString(boundaryChars)
        + ", boundaryCharsMaxScan="
        + boundaryMaxScan
        + ", boundaryScannerLocale="
        + boundaryScannerLocale.toLanguageTag()
        + '}';
  }

  public static final class Builder {

    private Highlighter highlighter;
    private String[] preTags;
    private String[] postTags;
    private int fragmentSize;
    private int maxNumFragments;
    private Query highlightQuery;
    private boolean fieldMatch;
    private boolean scoreOrdered;
    private String fragmenter;
    private boolean discreteMultivalue;
    private String boundaryScanner;
    private Character[] boundaryChars;
    private int boundaryMaxScan;
    private Locale boundaryScannerLocale;
    private Map<String, Object> customHighlighterParams;

    public Builder() {}

    public Builder withHighlighter(Highlighter highlighter) {
      this.highlighter = highlighter;
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

    public Builder withDiscreteMultivalue(boolean discreteMultivalue) {
      this.discreteMultivalue = discreteMultivalue;
      return this;
    }

    public Builder withBoundaryScanner(String boundaryScanner) {
      this.boundaryScanner = boundaryScanner;
      return this;
    }

    public Builder withBoundaryChars(Character[] boundaryChars) {
      this.boundaryChars = boundaryChars;
      return this;
    }

    public Builder withBoundaryMaxScan(int boundaryMaxScan) {
      this.boundaryMaxScan = boundaryMaxScan;
      return this;
    }

    public Builder withBoundaryScannerLocale(Locale boundaryScannerLocale) {
      this.boundaryScannerLocale = boundaryScannerLocale;
      return this;
    }

    public Builder withCustomHighlighterParams(Map<String, Object> customHighlighterParams) {
      this.customHighlighterParams = customHighlighterParams;
      return this;
    }

    public HighlightSettings build() {
      return new HighlightSettings(
          highlighter,
          preTags,
          postTags,
          fragmentSize,
          maxNumFragments,
          highlightQuery,
          fieldMatch,
          scoreOrdered,
          fragmenter,
          discreteMultivalue,
          boundaryScanner,
          boundaryChars,
          boundaryMaxScan,
          boundaryScannerLocale,
          customHighlighterParams);
    }
  }
}
