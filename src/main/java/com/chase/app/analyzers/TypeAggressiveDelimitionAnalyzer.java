package com.chase.app.analyzers;

import com.chase.app.tokenFilters.WordDelimiterGraphFilterHelper;
import com.chase.app.tokenizers.PathsTokenizer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;

public class TypeAggressiveDelimitionAnalyzer extends Analyzer {

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      final Tokenizer src = new PathsTokenizer();
      TokenStream res = WordDelimiterGraphFilterHelper.UseDefaultWordDelimiterGraphSettings(src);
      res = new LowerCaseFilter(res);
      res = new ASCIIFoldingFilter(res);
      res = new PorterStemFilter(res);  // TODO is this the equivalent of ES'es StemmerFilter for light_english? check source code.
  
      return new TokenStreamComponents(src, res);
    }
  
  }