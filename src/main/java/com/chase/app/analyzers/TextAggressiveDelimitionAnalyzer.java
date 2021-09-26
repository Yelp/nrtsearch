package com.chase.app.analyzers;

import com.chase.app.tokenFilters.ShingleFilterHelper;
import com.chase.app.tokenFilters.WordDelimiterGraphFilterHelper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;

public class TextAggressiveDelimitionAnalyzer extends Analyzer { 
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      final Tokenizer src = new WhitespaceTokenizer();
      
      TokenStream res = WordDelimiterGraphFilterHelper.UseDefaultWordDelimiterGraphSettings(src);
      res = new LowerCaseFilter(res);
      res = new ASCIIFoldingFilter(res);
      res = ShingleFilterHelper.UseTrigramFilter(res);  
        // TODO why did we decide to use trigram in aggressive delimiter? isnt is enough to prefix on basic (whitespace + "-") analyzer
        // anyway this can create strange behavior (e.g. "bla.com/path+level+1" and someone looking for "compa"..)
      
      return new TokenStreamComponents(src, res);
    }
  }

  