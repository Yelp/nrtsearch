package com.chase.app.tokenFilters;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter;

public final class WordDelimiterGraphFilterHelper {

    final private static int _configurationFlags = WordDelimiterGraphFilter.GENERATE_NUMBER_PARTS |
      WordDelimiterGraphFilter.GENERATE_WORD_PARTS |
      WordDelimiterGraphFilter.SPLIT_ON_CASE_CHANGE |
      WordDelimiterGraphFilter.SPLIT_ON_NUMERICS |
      WordDelimiterGraphFilter.STEM_ENGLISH_POSSESSIVE;
    public static TokenStream UseDefaultWordDelimiterGraphSettings(TokenStream in)
    {
      return new WordDelimiterGraphFilter(in, _configurationFlags, null);
    }
  }