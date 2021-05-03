package com.chase.app.tokenFilters;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;

public final class EdgeNgramFilterHelper {

    public static TokenStream UsePrefixEdgeNgramFilter(TokenStream in)
    {
      return new EdgeNGramTokenFilter(in, 1, 20, false);
    }
  }