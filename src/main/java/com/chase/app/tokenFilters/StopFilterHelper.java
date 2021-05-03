package com.chase.app.tokenFilters;

import java.util.Arrays;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopFilter;

public final class StopFilterHelper {
    private final static CharArraySet _en_words = new CharArraySet(Arrays.asList("a", "an", "and", "are", "as", "at", "be", "but", "by",
        "for", "if", "in", "into", "is", "it",
        "no", "not", "of", "on", "or", "such",
        "that", "the", "their", "then", "there", "these",
        "they", "this", "to", "was", "will", "with"), false);
    
    
    public static TokenStream UseEnglishStopFilter(TokenStream in)
    {
      return new StopFilter(in, _en_words);
    }
  }