package com.chase.app.tokenFilters;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;

public final class ShingleFilterHelper {
  public static TokenStream UseTrigramFilter(TokenStream in)
    {
      final ShingleFilter sf = new ShingleFilter(in, 3);
      sf.setTokenSeparator("");
      sf.setOutputUnigrams(true);
      return sf;
    }

    public static TokenStream UseTrigramLengthLimitFilter(TokenStream in)
    {
      return new LengthFilter(in, 1, 20);
    }
}