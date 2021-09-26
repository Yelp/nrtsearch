package com.chase.app.analyzers;

import com.chase.app.tokenFilters.EdgeNgramFilterHelper;
import com.chase.app.tokenFilters.ShingleFilterHelper;
import com.chase.app.tokenFilters.WordDelimiterGraphFilterHelper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.RemoveDuplicatesTokenFilter;

public class TextAggressiveDelimitionPrefixAnalyzer extends Analyzer { 
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      final Tokenizer src = new WhitespaceTokenizer();
      
      TokenStream res = WordDelimiterGraphFilterHelper.UseDefaultWordDelimiterGraphSettings(src);
      res = new LowerCaseFilter(res);
      res = new ASCIIFoldingFilter(res);
      res = ShingleFilterHelper.UseTrigramFilter(res);
      res = EdgeNgramFilterHelper.UsePrefixEdgeNgramFilter(res);
      res = new RemoveDuplicatesTokenFilter(res);
      
      return new TokenStreamComponents(src, res);
    }
  }