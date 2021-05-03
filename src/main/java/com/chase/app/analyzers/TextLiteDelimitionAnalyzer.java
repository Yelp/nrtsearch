package com.chase.app.analyzers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.shingle.ShingleFilterFactory;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class TextLiteDelimitionAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      final Tokenizer src = new StandardTokenizer();
      
      TokenStream res = new LowerCaseFilter(src);
      res = new ASCIIFoldingFilter(res);
      res = new PorterStemFilter(res); // TODO eqivalent of ES'es?
      
      return new TokenStreamComponents(src, res);
    }
  }